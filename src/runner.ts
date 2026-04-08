import { Redis } from 'ioredis';
import * as crypto from 'crypto';
import { v4 as uuidv4 } from 'uuid';
import { getRedis, createRedis } from './redis_client';
import { GatewayWorker } from './worker';
import { QueueNames, ConsumerGroups } from './constants';
import { AskAgentCommand, CancelTaskCommand, GatewayCommand, ResumeCommand, commandFromDict } from './protocol/commands';
import { WorkerRegistry } from './registry';
import { HistoryProvider } from './history';
import { AgentState } from './protocol/agent_state';

interface RunningExecution {
    executionId: string;
    messageId: string;
    parentMessageId: string;
    sessionId: string;
    workerId: string;
    abortController: AbortController;
    cancelReason: string;
    context: any;
}

export class WorkerRunner {
    private redis: Redis;
    private worker: GatewayWorker;
    private groupName: string;
    private consumerName: string;
    private running: boolean = false;
    private lockToken: string | null = null;
    private controlTask: Promise<void> | null = null;
    private controlLoopRunning: boolean = false;
    private maxConcurrency: number;
    private fetchCount: number;
    private activeExecutions = new Map<string, RunningExecution>();
    private messageToExecution = new Map<string, string>();
    private readonly terminalExecutionStates = new Set<string>([
        AgentState.COMPLETED,
        AgentState.FAILED,
        AgentState.CANCELLED,
    ]);

    constructor(
        workerOrOptions: GatewayWorker | { workerId: string; agentTypes: string[]; registry?: WorkerRegistry },
        options: {
            redisClient?: Redis;
            groupName?: string;
            maxConcurrency?: number;
            fetchCount?: number;
        } = {}
    ) {
        if ('workerId' in workerOrOptions && !('handleMessage' in workerOrOptions)) {
            // 提供一个极简的内部伪装对象，仅满足 runner 对 ID 和能力的查询需求
            const redis = options.redisClient || getRedis();
            this.worker = {
                workerId: workerOrOptions.workerId,
                getAgentTypes: () => workerOrOptions.agentTypes,
                registry: workerOrOptions.registry || new WorkerRegistry(redis),
                startHeartbeat: async () => { }, // 原子模式下由用户决定是否启动心跳
                stopHeartbeat: () => { },
                handleMessage: async () => { throw new Error("Manual poll mode: use processAndAck instead"); }
            } as any;
        } else {
            this.worker = workerOrOptions as GatewayWorker;
        }
        // IMPORTANT: Use a dedicated Redis connection for the runner (polling)
        // to avoid blocking the main connection used by the worker for emitting chunks.
        this.redis = options.redisClient || createRedis();
        this.groupName = options.groupName || this.autoGroupName();
        this.consumerName = this.worker.workerId;
        this.maxConcurrency = options.maxConcurrency ?? 50;
        this.fetchCount = options.fetchCount ?? 10;
    }

    private autoGroupName(): string {
        const caps = [...this.worker.getAgentTypes()].sort();
        const payload = caps.join(',');
        const digest = crypto.createHash('sha1').update(payload).digest('hex').substring(0, 10);
        return `${ConsumerGroups.AGENT_ENGINES}:${digest}`;
    }

    async setupStreams(): Promise<void> {
        for (const cap of this.worker.getAgentTypes()) {
            const streamName = QueueNames.ctrl_stream(cap);
            try {
                await this.redis.xgroup(
                    'CREATE',
                    streamName,
                    this.groupName,
                    '0',
                    'MKSTREAM'
                );
            } catch (err: any) {
                if (!err.message.includes('BUSYGROUP')) {
                    console.warn(`Warning setting up stream ${streamName}:`, err.message);
                }
            }
        }
    }

    async setupControlStreams(): Promise<void> {
        const streamName = QueueNames.worker_ctrl_stream(this.worker.workerId);
        try {
            await this.redis.xgroup(
                'CREATE',
                streamName,
                this.groupName,
                '0',
                'MKSTREAM'
            );
        } catch (err: any) {
            if (!err.message.includes('BUSYGROUP')) {
                console.warn(`Warning setting up stream ${streamName}:`, err.message);
            }
        }
    }

    /**
     * 初始化环境：抢占 worker_id 锁，设置 Stream，启动心跳。
     * 这是“解耦模式”下的首选初始化方式。
     */
    async initialize(): Promise<void> {
        console.log(`[${this.worker.workerId}] Initializing worker environment...`);

        // 1. 抢占独占锁
        this.lockToken = await this.worker.registry.claimWorkerId(this.worker.workerId);

        // 2. 设置流消费组
        await this.setupStreams();
        await this.setupControlStreams();

        // 3. 启动心跳
        await this.worker.startHeartbeat();
        this.startControlLoop();

        console.log(`[${this.worker.workerId}] Worker environment ready.`);
    }

    /**
     * 优雅释放资源：停止心跳并释放锁。
     */
    async release(): Promise<void> {
        this.controlLoopRunning = false;
        await this.worker.stopHeartbeat();
        await this.worker.registry.markWorkerInactive(this.worker.workerId);
        await this.worker.registry.unregisterWorkerMembership(this.worker.workerId);
        if ((this.worker as any).pluginRegistry?.onWorkerShutdown) {
            if (this.worker.pluginRegistry.logHookStatsOnShutdown) {
                this.worker.pluginRegistry.logHookStats();
            }
            await this.worker.pluginRegistry.onWorkerShutdown(this.worker);
        }
        if (this.lockToken) {
            await this.worker.registry.releaseWorkerId(this.worker.workerId, this.lockToken).catch(err => {
                console.warn(`[${this.worker.workerId}] Failed to release lock:`, err.message);
            });
            this.lockToken = null;
        }
        if (this.controlTask) {
            await this.controlTask.catch(() => undefined);
            this.controlTask = null;
        }
    }

    async poll(options: { count?: number; block?: number } = {}): Promise<{ streamName: string; msgId: string; data: GatewayCommand }[]> {
        // 在轮询前检查并刷新锁（类似于 Python SDK 的逻辑）
        try {
            const ok = await this.worker.registry.refreshWorkerIdLock(this.worker.workerId);
            if (!ok) {
                throw new Error(`Worker lock lost for ${this.worker.workerId}`);
            }
        } catch (err: any) {
            console.error(`[${this.worker.workerId}] Lock maintenance error:`, err.message);
            throw err;
        }

        const streams: string[] = [];
        const ids: string[] = [];
        for (const cap of this.worker.getAgentTypes()) {
            streams.push(QueueNames.ctrl_stream(cap));
            ids.push('>');
        }

        if (streams.length === 0) return [];

        const result = (await this.redis.xreadgroup(
            'GROUP',
            this.groupName,
            this.consumerName,
            'COUNT',
            options.count || this.fetchCount,
            'BLOCK',
            options.block || 2000,
            'STREAMS',
            ...streams,
            ...ids
        )) as [string, [string, string[]][]][] | null;

        const results: { streamName: string; msgId: string; data: GatewayCommand }[] = [];
        if (result) {
            for (const [streamName, messages] of result) {
                for (const [msgId, fieldValues] of messages) {
                    let dataStr = '';
                    for (let i = 0; i < fieldValues.length; i += 2) {
                        if (fieldValues[i] === 'data') {
                            dataStr = fieldValues[i + 1];
                            break;
                        }
                    }

                    if (dataStr) {
                        try {
                            const data = commandFromDict(JSON.parse(dataStr));
                            results.push({ streamName, msgId, data });
                        } catch (err) {
                            console.error(`Failed to parse message JSON: ${dataStr}`, err);
                        }
                    }
                }
            }
        }
        return results;
    }

    /**
     * 处理单条消息并确认。
     * 封装了 GatewayProcessor 的逻辑。
     */
    async processAndAck(
        streamName: string,
        msgId: string,
        data: GatewayCommand
    ): Promise<void> {
        try {
            if (data instanceof CancelTaskCommand) {
                await this._handleControlMessage(streamName, msgId, data);
                return;
            }

            // 注入会话历史消息（对标 Python SDK 的 HistoryProvider 注入逻辑）
            const history = await HistoryProvider.getSessionHistory(data.header.sessionId);
            if (data instanceof AskAgentCommand || data instanceof ResumeCommand) {
                const extraPayload = { ...data.extraPayload, history };
                data = data instanceof AskAgentCommand
                    ? new AskAgentCommand(data.header, data.content, data.waitForReply, extraPayload)
                    : new ResumeCommand(data.header, data.content, data.status, data.replyData, extraPayload);
            }

            const registry = this.worker.registry as any;
            const existingExecution = registry?.getExecutionByMessageId
                ? await registry.getExecutionByMessageId(data.header.messageId, data.header.sessionId)
                : null;

            if (existingExecution && this.terminalExecutionStates.has(String(existingExecution.status || ''))) {
                await this.redis.xack(streamName, this.groupName, msgId);
                return;
            }

            const executionId = String(existingExecution?.execution_id || `exec-${uuidv4().slice(0, 8)}`);
            const cancelReason = String(existingExecution?.cancel_reason || '');
            const parentMessageId = String(existingExecution?.parent_message_id || data.header.parentMessageId || '');
            const abortController = new AbortController();
            if (existingExecution?.cancel_requested) {
                abortController.abort(cancelReason || 'task cancelled');
            }

            // If this message was cancelled before any worker claimed it,
            // finalize immediately without entering business processing.
            if (existingExecution?.cancel_requested && String(existingExecution.status || '') !== 'RUNNING') {
                if (registry?.markExecutionFinished) {
                    await registry.markExecutionFinished(executionId, data.header.sessionId, AgentState.CANCELLED);
                }
                await this.redis.xack(streamName, this.groupName, msgId);
                return;
            }

            this.activeExecutions.set(executionId, {
                executionId,
                messageId: data.header.messageId,
                parentMessageId,
                sessionId: data.header.sessionId,
                workerId: this.worker.workerId,
                abortController,
                cancelReason,
                context: null,
            });
            this.messageToExecution.set(data.header.messageId, executionId);

            // Update or create RUNNING execution so worker_id is available for cancel routing.
            if (existingExecution && registry?.updateExecutionStatus) {
                await registry.updateExecutionStatus(executionId, data.header.sessionId, 'RUNNING', {
                    worker_id: this.worker.workerId,
                    stream_name: streamName,
                    redis_message_id: msgId,
                });
            } else if (registry?.saveExecution) {
                const nowMs = Date.now();
                await registry.saveExecution({
                    execution_id: executionId,
                    message_id: data.header.messageId,
                    parent_message_id: parentMessageId,
                    session_id: data.header.sessionId,
                    worker_id: this.worker.workerId,
                    target_agent_type: data.header.targetAgentType,
                    stream_name: streamName,
                    redis_message_id: msgId,
                    status: 'RUNNING',
                    cancel_requested: Boolean(existingExecution?.cancel_requested),
                    cancel_reason: cancelReason,
                    created_at: nowMs,
                    started_at: nowMs,
                    finished_at: 0,
                    updated_at: nowMs,
                });
            }

            const finalStatus = await this.worker.handleMessage(data, {
                cancelSignal: abortController.signal,
                cancelReason,
            });
            if (registry?.markExecutionFinished) {
                await registry.markExecutionFinished(executionId, data.header.sessionId, finalStatus);
            }
            await this.redis.xack(streamName, this.groupName, msgId);
        } catch (err) {
            console.error(`Failed to process/ack message ${msgId}:`, err);
            throw err;
        } finally {
            const executionId = this.messageToExecution.get(data.header.messageId);
            if (executionId) {
                this.messageToExecution.delete(data.header.messageId);
                this.activeExecutions.delete(executionId);
            }
        }
    }

    private startControlLoop(): void {
        if (this.controlTask) {
            return;
        }
        this.controlLoopRunning = true;
        this.controlTask = (async () => {
            while (this.controlLoopRunning) {
                try {
                    await this.runControlOnce();
                } catch (err) {
                    if (!this.controlLoopRunning) {
                        break;
                    }
                    console.error('Error in control loop:', err);
                    await new Promise((resolve) => setTimeout(resolve, 1000));
                }
            }
        })();
    }

    async runControlOnce(block: number = 2000): Promise<GatewayCommand[]> {
        const streamName = QueueNames.worker_ctrl_stream(this.worker.workerId);
        const result = (await this.redis.xreadgroup(
            'GROUP',
            this.groupName,
            this.consumerName,
            'COUNT',
            10,
            'BLOCK',
            block,
            'STREAMS',
            streamName,
            '>'
        )) as [string, [string, string[]][]][] | null;

        if (!result) {
            return [];
        }

        const commands: GatewayCommand[] = [];

        for (const [currentStreamName, messages] of result) {
            for (const [msgId, fieldValues] of messages) {
                let dataStr = '';
                for (let i = 0; i < fieldValues.length; i += 2) {
                    if (fieldValues[i] === 'data') {
                        dataStr = fieldValues[i + 1];
                        break;
                    }
                }
                if (!dataStr) {
                    continue;
                }
                try {
                    const command = commandFromDict(JSON.parse(dataStr));
                    await this._handleControlMessage(currentStreamName, msgId, command);
                    commands.push(command);
                } catch (err) {
                    console.error(`Failed to process control message ${msgId}:`, err);
                    await this.redis.xack(currentStreamName, this.groupName, msgId);
                }
            }
        }

        return commands;
    }

    private async _handleControlMessage(
        streamName: string,
        msgId: string,
        command: GatewayCommand
    ): Promise<void> {
        try {
            if (!(command instanceof CancelTaskCommand)) {
                return;
            }

            const executionId = command.targetExecutionId || this.messageToExecution.get(command.targetMessageId);
            const running = executionId ? this.activeExecutions.get(executionId) : undefined;
            const registry = this.worker.registry as any;

            if (executionId && registry?.markExecutionCancelling) {
                await registry.markExecutionCancelling(executionId, command.header.sessionId, command.reason);
            }

            if (running) {
                running.cancelReason = command.reason;
                running.abortController.abort(command.reason || 'task cancelled');
                // 触发 Worker 钩子
                Promise.resolve().then(() => this.worker.onCancelTask(command));
                // 触发插件钩子
                if (running.context && this.worker.pluginRegistry) {
                    Promise.resolve().then(() => this.worker.pluginRegistry.onTaskCancel(running.context, command));
                }
            }
        } finally {
            await this.redis.xack(streamName, this.groupName, msgId);
        }
    }

    async start(options: { handleSignals?: boolean } = {}): Promise<void> {
        const signalHandler = async () => {
            console.log('\nReceived shutdown signal, stopping runner...');
            this.stop();
            await this.release();
            process.exit(0);
        };

        if (options.handleSignals) {
            process.on('SIGINT', signalHandler);
            process.on('SIGTERM', signalHandler);
        }

        try {
            await this.initialize();
            console.log(`[${this.worker.workerId}] Runner auto-loop started, waiting for tasks...`);

            this.running = true;
            const inFlight = new Set<Promise<void>>();
            while (this.running) {
                try {
                    const messages = await this.poll();
                    for (const { streamName, msgId, data } of messages) {
                        while (inFlight.size >= this.maxConcurrency) {
                            await Promise.race(inFlight);
                        }

                        const task = this.processAndAck(streamName, msgId, data).catch((err) => {
                            console.error(`Error processing message ${msgId}:`, err);
                        }).finally(() => {
                            inFlight.delete(task);
                        });
                        inFlight.add(task);
                    }
                } catch (err) {
                    console.error('Error in runner loop:', err);
                    if (!this.running) break;
                    await new Promise((resolve) => setTimeout(resolve, 1000));
                }
            }
            if (inFlight.size > 0) {
                await Promise.allSettled(inFlight);
            }
        } finally {
            if (!options.handleSignals) {
                await this.release();
            }
            if (options.handleSignals) {
                process.off('SIGINT', signalHandler);
                process.off('SIGTERM', signalHandler);
            }
        }
    }

    async ack(streamName: string, msgId: string): Promise<void> {
        await this.redis.xack(streamName, this.groupName, msgId);
    }

    /**
     * 订阅模式：异步接收消息并通过回调处理。
     * 内部启动一个独立的循环，不会阻塞调用者。
     */
    subscribe(
        handler: (message: { streamName: string; msgId: string; data: GatewayCommand }) => Promise<void> | void,
        options: { pollInterval?: number } = {}
    ): { stop: () => void } {
        let subRunning = true;

        const loop = async () => {
            console.log(`[${this.worker.workerId}] Subscription loop started.`);
            while (subRunning && this.running === false) { // 如果全局没有显式 stop
                try {
                    const messages = await this.poll({ block: 500 }); // 缩短阻塞时间提高灵敏度
                    for (const msg of messages) {
                        // 关键：不等待 handler 返回，避免长耗时任务阻塞轮询循环
                        Promise.resolve().then(() => handler(msg)).catch(err => {
                            console.error(`Error in subscription handler:`, err);
                        });
                    }
                } catch (err) {
                    console.error('Error in subscription loop:', err);
                    if (!subRunning) break;
                    await new Promise(resolve => setTimeout(resolve, options.pollInterval || 1000));
                }
            }
            console.log(`[${this.worker.workerId}] Subscription loop stopped.`);
        };

        // 异步启动，不阻塞
        loop();

        return {
            stop: () => { subRunning = false; }
        };
    }

    /**
     * 订阅取消消息：异步接收来自控制流的 CancelTaskCommand 并通过回调处理。
     */
    subscribeCancel(
        handler: (command: CancelTaskCommand) => Promise<void> | void,
        options: { pollInterval?: number } = {}
    ): { stop: () => void } {
        let subRunning = true;

        const loop = async () => {
            console.log(`[${this.worker.workerId}] Cancel subscription loop started.`);
            while (subRunning && this.running === false) {
                try {
                    const commands = await this.runControlOnce(500); // 缩短阻塞时间
                    for (const cmd of commands) {
                        if (cmd instanceof CancelTaskCommand) {
                            // 同样不等待处理，立即继续下一轮监听
                            Promise.resolve().then(() => handler(cmd)).catch(handlerErr => {
                                console.error(`[${this.worker.workerId}] Error in cancel subscription handler:`, handlerErr);
                            });
                        }
                    }
                } catch (err) {
                    console.error(`[${this.worker.workerId}] Error in cancel subscription loop:`, err);
                    if (!subRunning) break;
                    await new Promise(resolve => setTimeout(resolve, options.pollInterval || 1000));
                }
            }
            console.log(`[${this.worker.workerId}] Cancel subscription loop stopped.`);
        };

        loop();

        return {
            stop: () => { subRunning = false; }
        };
    }

    stop(): void {
        this.running = false;
        this.controlLoopRunning = false;
    }
}

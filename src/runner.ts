import { Redis } from 'ioredis';
import * as crypto from 'crypto';
import { v4 as uuidv4 } from 'uuid';
import { getRedis, createRedis } from './redis_client';
import { GatewayWorker } from './worker';
import { QueueNames, ConsumerGroups, STREAM_READ_LAST_ID } from './constants';
import { AskAgentCommand, CancelTaskCommand, EvictWorkerCommand, GatewayCommand, ResumeCommand, ResumeWorkerCommand, SuspendWorkerCommand, commandFromDict } from './protocol/commands';
import { WorkerRegistry } from './registry';
import { HistoryProvider } from './history';
import { AgentState } from './protocol/agent_state';
import { SpanRecorder, TraceSpan } from './trace/span_recorder';

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
    private readonly ownsRedis: boolean;
    private readonly streamReadRedis: Redis;
    private readonly ownsStreamReadRedis: boolean;
    private readonly controlReadRedis: Redis;
    private readonly ownsControlReadRedis: boolean;
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
    private cancelHandlers = new Set<(command: CancelTaskCommand) => Promise<void> | void>();
    private readonly terminalExecutionStates = new Set<string>([
        AgentState.COMPLETED,
        AgentState.FAILED,
        AgentState.CANCELLED,
    ]);
    // Admin-controlled lifecycle: "active" | "suspended" | "evicted"
    private adminLifecycle: string = 'active';
    private evictForce: boolean = false;
    // In-memory cache of agent_types denied for this worker.
    private deniedAgentTypes: Set<string> = new Set();
    // Consumer loop liveness tracking for health_check.
    private lastConsumerTick: number = 0;
    private readonly consumerHealthTimeoutMs: number = 30_000;
    // Round-robin cursor for poll()'s phase-two blocking read, so no
    // agent_type is permanently starved of the blocking slot.
    private primaryCursor: number = 0;

    readonly spanRecorder: SpanRecorder;

    constructor(
        workerOrOptions: GatewayWorker | { workerId: string; agentTypes: string[]; registry?: WorkerRegistry },
        options: {
            redisClient?: Redis;
            groupName?: string;
            maxConcurrency?: number;
            fetchCount?: number;
            spanRecorder?: SpanRecorder;
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
        this.ownsRedis = !options.redisClient;
        this.redis = options.redisClient || createRedis();
        const streamReader = this.createBlockingRedisConnection();
        this.streamReadRedis = streamReader.client;
        this.ownsStreamReadRedis = streamReader.owned;
        const controlReader = this.createBlockingRedisConnection();
        this.controlReadRedis = controlReader.client;
        this.ownsControlReadRedis = controlReader.owned;
        this.groupName = options.groupName || this.autoGroupName();
        this.consumerName = this.worker.workerId;
        this.maxConcurrency = options.maxConcurrency ?? 50;
        this.fetchCount = options.fetchCount ?? 10;
        this.spanRecorder = options.spanRecorder || new SpanRecorder(this.redis);
    }

    /** Return the hex parent span ID propagated from the client dispatch. */
    static _clientDispatchParentSpanId(header: any): string {
        const fromField = String(header?.traceParentSpanId || '');
        if (fromField) return fromField;
        const meta = header?.metadata || {};
        const fromMeta = String(meta['trace_parent_span_id'] || '');
        if (fromMeta) return fromMeta;
        return `${header?.messageId || ''}:client.dispatch`;
    }

    /** Return the raw framework span ID used to build the Redis trace tree. */
    static _frameworkParentSpanId(header: any): string {
        const meta = header?.metadata || {};
        const fromMeta = String(meta['framework_parent_span_id'] || '');
        if (fromMeta) return fromMeta;
        const fromField = String(header?.traceParentSpanId || '');
        if (fromField) return fromField;
        const fromMetaTrace = String(meta['trace_parent_span_id'] || '');
        if (fromMetaTrace) return fromMetaTrace;
        return `${header?.messageId || ''}:client.dispatch`;
    }

    private createBlockingRedisConnection(): { client: Redis; owned: boolean } {
        const duplicate = (this.redis as unknown as { duplicate?: () => Redis }).duplicate;
        if (typeof duplicate === 'function') {
            return {
                client: duplicate.call(this.redis),
                owned: true,
            };
        }
        return {
            client: this.redis,
            owned: false,
        };
    }

    private async closeRedisConnection(client: Redis, owned: boolean): Promise<void> {
        if (!owned) {
            return;
        }

        const maybeClient = client as unknown as {
            quit?: () => Promise<unknown>;
            disconnect?: () => void;
        };

        try {
            if (typeof maybeClient.quit === 'function') {
                await maybeClient.quit();
                return;
            }
        } catch {
            // Fall back to a hard disconnect if graceful quit fails.
        }

        if (typeof maybeClient.disconnect === 'function') {
            maybeClient.disconnect();
        }
    }

    private notifyCancelHandlers(command: CancelTaskCommand): void {
        for (const handler of this.cancelHandlers) {
            Promise.resolve()
                .then(() => handler(command))
                .catch(handlerErr => {
                    console.error(`[${this.worker.workerId}] Error in cancel subscription handler:`, handlerErr);
                });
        }
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

        // 3. 启动心跳 (wire lifecycle, denylist, and consumer health callbacks)
        await this.worker.startHeartbeat(
            (lifecycle: string) => {
                this.adminLifecycle = lifecycle;
            },
            (denied: Set<string>) => {
                this.deniedAgentTypes = denied;
            },
            () => {
                // health_check: consumer loop is healthy if it ticked recently
                if (this.lastConsumerTick === 0) return true; // not started yet
                return (Date.now() - this.lastConsumerTick) < this.consumerHealthTimeoutMs;
            },
            () => {
                console.error(`[${this.worker.workerId}] Consumer loop unhealthy; stopping runner`);
                this.running = false;
            }
        );
        this.startControlLoop();

        console.log(`[${this.worker.workerId}] Worker environment ready.`);
    }

    /**
     * 优雅释放资源：停止心跳并释放锁。
     */
    async release(): Promise<void> {
        this.controlLoopRunning = false;
        await this.worker.stopHeartbeat();
        let releasedWorkerId = false;
        if (this.lockToken) {
            releasedWorkerId = await this.worker.registry.releaseWorkerId(this.worker.workerId, this.lockToken).catch(err => {
                console.warn(`[${this.worker.workerId}] Failed to release lock:`, err.message);
                return false;
            });
            this.lockToken = null;
        } else {
            releasedWorkerId = await this.worker.registry.markWorkerInactive(this.worker.workerId);
        }
        if (releasedWorkerId) {
            await this.worker.registry.unregisterWorkerMembership(this.worker.workerId);
        }
        if ((this.worker as any).pluginRegistry?.onWorkerShutdown) {
            if (this.worker.pluginRegistry.logHookStatsOnShutdown) {
                this.worker.pluginRegistry.logHookStats();
            }
            await this.worker.pluginRegistry.onWorkerShutdown(this.worker);
        }
        if (this.controlTask) {
            await this.controlTask.catch(() => undefined);
            this.controlTask = null;
        }
        await this.closeRedisConnection(this.controlReadRedis, this.ownsControlReadRedis);
        await this.closeRedisConnection(this.streamReadRedis, this.ownsStreamReadRedis);
        await this.closeRedisConnection(this.redis, this.ownsRedis);
    }

    /**
     * Two-phase read to stay Cluster-safe: different agent_type ctrl streams
     * are different entities/slots, so they can never be combined into one
     * XREADGROUP call.
     *
     * Phase one: concurrent non-blocking XREADGROUP (no BLOCK argument at
     * all — that means "return immediately with whatever's available", NOT
     * the same as BLOCK 0, which blocks forever), one call per active
     * stream. If any stream returned messages, return immediately.
     *
     * Phase two: only if every stream came back empty, one real blocking
     * XREADGROUP against a single "primary" stream, chosen by round-robin
     * across the declared agent_types so no agent_type is permanently
     * starved of the blocking slot. Both phases reuse the existing
     * streamReadRedis connection — no per-agent_type connection pool.
     */
    async poll(options: { count?: number; block?: number } = {}): Promise<{ streamName: string; msgId: string; data: GatewayCommand }[]> {
        const streamNames: string[] = [];
        for (const cap of this.worker.getAgentTypes()) {
            // Skip streams for denied agent types
            if (this.deniedAgentTypes.has(cap)) continue;
            streamNames.push(QueueNames.ctrl_stream(cap));
        }

        if (streamNames.length === 0) return [];

        const count = options.count || this.fetchCount;
        const block = options.block || 2000;

        const firstPass = await Promise.all(
            streamNames.map(streamName => this.readStream(streamName, count))
        );
        const firstResults = this.parseXreadgroupBatches(firstPass);
        if (firstResults.length > 0) return firstResults;

        const primary = streamNames[this.primaryCursor % streamNames.length];
        this.primaryCursor++;
        const blocked = await this.readStream(primary, count, block);
        return this.parseXreadgroupBatches([blocked]);
    }

    private async readStream(
        streamName: string,
        count: number,
        block?: number
    ): Promise<[string, [string, string[]][]][] | null> {
        const args: (string | number)[] = [
            'GROUP', this.groupName, this.consumerName,
            'COUNT', count,
        ];
        if (block !== undefined) {
            args.push('BLOCK', block);
        }
        args.push('STREAMS', streamName, STREAM_READ_LAST_ID);
        return (await (this.streamReadRedis.xreadgroup as (...a: any[]) => Promise<any>)(...args)) as
            [string, [string, string[]][]][] | null;
    }

    private parseXreadgroupBatches(
        batches: ([string, [string, string[]][]][] | null)[]
    ): { streamName: string; msgId: string; data: GatewayCommand }[] {
        const results: { streamName: string; msgId: string; data: GatewayCommand }[] = [];
        for (const batch of batches) {
            if (!batch) continue;
            for (const [streamName, messages] of batch) {
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

            const frameworkParentSpanId = WorkerRunner._frameworkParentSpanId(data.header);
            const executionStartedAt = Date.now();
            const executionRef: { context: any } = { context: null };

            const taskResult = await this.worker.handleMessage(data, {
                cancelSignal: abortController.signal,
                cancelReason,
                executionId,
                spanRecorder: this.spanRecorder,
                executionRef,
            });
            const executionFinishedAt = Date.now();
            const finalStatus = taskResult.status;

            // Extract telemetry from AgentContext filled by worker
            const execContext = executionRef.context;
            const chunkCount = Number(execContext?._chunkCount || 0);
            const tokenUsage: Record<string, any> = { ...(execContext?._tokenUsage || {}) };

            if (registry?.markExecutionFinished) {
                await registry.markExecutionFinished(executionId, data.header.sessionId, finalStatus);
            }

            await this._recordWorkerExecuteSpan({
                traceId: data.header.traceId,
                executionId,
                messageId: data.header.messageId,
                parentMessageId: String(existingExecution?.parent_message_id || data.header.parentMessageId || ''),
                sessionId: data.header.sessionId,
                workerId: this.worker.workerId,
                targetAgentType: data.header.targetAgentType,
                status: finalStatus,
                startTs: executionStartedAt,
                endTs: executionFinishedAt,
                parentSpanId: frameworkParentSpanId,
                chunkCount,
                tokens: tokenUsage,
            });

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

    private async _recordWorkerExecuteSpan(params: {
        traceId: string;
        executionId: string;
        messageId: string;
        parentMessageId: string;
        sessionId: string;
        workerId: string;
        targetAgentType: string;
        status: string;
        startTs: number;
        endTs: number;
        parentSpanId?: string;
        chunkCount?: number;
        tokens?: Record<string, any>;
        errorType?: string;
        errorMessage?: string;
    }): Promise<void> {
        try {
            await this.spanRecorder.recordSpan({
                traceId: params.traceId,
                spanId: `${params.executionId}:worker.execute`,
                parentSpanId: params.parentSpanId || `${params.messageId}:client.dispatch`,
                operation: 'worker.execute',
                component: 'worker',
                startTs: params.startTs,
                endTs: params.endTs,
                status: params.status,
                sessionId: params.sessionId,
                executionId: params.executionId,
                messageId: params.messageId,
                parentMessageId: params.parentMessageId,
                workerId: params.workerId,
                targetAgentType: params.targetAgentType,
                chunkCount: params.chunkCount || 0,
                tokens: params.tokens && Object.keys(params.tokens).length > 0 ? params.tokens : undefined,
                errorType: params.errorType || '',
                errorMessage: params.errorMessage || '',
            } as TraceSpan);
        } catch (err) {
            // best effort
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
        const result = (await this.controlReadRedis.xreadgroup(
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
            if (command instanceof SuspendWorkerCommand) {
                this.adminLifecycle = 'suspended';
                console.log(`[${this.worker.workerId}] Worker suspended by admin: ${command.reason}`);
                return;
            }

            if (command instanceof ResumeWorkerCommand) {
                this.adminLifecycle = 'active';
                console.log(`[${this.worker.workerId}] Worker resumed by admin`);
                return;
            }

            if (command instanceof EvictWorkerCommand) {
                this.adminLifecycle = 'evicted';
                this.evictForce = command.force;
                console.log(`[${this.worker.workerId}] Worker eviction requested by admin (force=${command.force}): ${command.reason}`);
                return;
            }

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
            this.notifyCancelHandlers(command);
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
                    // Update liveness tick so heartbeat health_check knows the loop is alive
                    this.lastConsumerTick = Date.now();

                    // If suspended, pause without consuming
                    if (this.adminLifecycle === 'suspended') {
                        await new Promise((resolve) => setTimeout(resolve, 1000));
                        continue;
                    }
                    // If evicted, exit the loop
                    if (this.adminLifecycle === 'evicted') {
                        console.log(`[${this.worker.workerId}] Eviction requested; stopping runner loop`);
                        break;
                    }

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
        this.cancelHandlers.add(handler);
        let subRunning = true;
        let pollingLoopRunning = false;

        const loop = async () => {
            console.log(`[${this.worker.workerId}] Cancel subscription loop started.`);
            pollingLoopRunning = true;
            while (subRunning && this.running === false) {
                try {
                    await this.runControlOnce(500); // 缩短阻塞时间
                } catch (err) {
                    console.error(`[${this.worker.workerId}] Error in cancel subscription loop:`, err);
                    if (!subRunning) break;
                    await new Promise(resolve => setTimeout(resolve, options.pollInterval || 1000));
                }
            }
            console.log(`[${this.worker.workerId}] Cancel subscription loop stopped.`);
            pollingLoopRunning = false;
        };

        if (!this.controlLoopRunning) {
            loop();
        }

        return {
            stop: () => {
                subRunning = false;
                this.cancelHandlers.delete(handler);
                if (!pollingLoopRunning) {
                    return;
                }
            }
        };
    }

    stop(): void {
        this.running = false;
        this.controlLoopRunning = false;
    }
}

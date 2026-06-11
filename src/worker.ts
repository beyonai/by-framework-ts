import * as fs from 'fs/promises';
import * as path from 'path';
import { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { getRedis } from './redis_client';
import { GatewayCommand, ResumeCommand, AskAgentCommand } from './protocol/commands';
import { AgentState, isTerminalState } from './protocol/agent_state';
import { EventType } from './protocol/event_type';
import { AgentContext, TaskCancelledError } from './context';
import { QueueNames, RegistryKeys, TASK_GROUP_TTL_SECONDS, TASK_GROUP_FIELD_TOTAL, TASK_GROUP_FIELD_COMPLETED } from './constants';
import { WorkerRegistry } from './registry';
import { WorkerHeartbeat } from './heartbeat';
import { MessageHeader } from './protocol/message_header';
import { JsonValue, ProcessCommandResult, WireContent, normalizeProcessResult, AgentTaskResult } from './protocol/results';
import { PluginRegistry } from './extensions/registry';
import { HistoryProvider } from './history';
import { WorkspaceManager } from './workspace';
import { HookSandbox, getActiveWorkspace, setActiveWorkspace } from './sandbox';
import { FileStorage } from './runtime/filestore/base';
import { SpanRecorder } from './trace/span_recorder';

// === Types ===
interface HandleMessageOptions {
    readonly cancelSignal?: AbortSignal | CancelSignalLegacy;
    readonly cancelReason?: string;
    readonly execution?: {
        readonly parentMessageId?: string;
        readonly isResumed?: boolean;
    };
    /** Execution ID pre-computed by runner; propagated into AgentContext for trace linkage. */
    readonly executionId?: string;
    /** SpanRecorder from runner; reused in AgentContext so spans share the same exporter. */
    readonly spanRecorder?: SpanRecorder;
    /** Mutable ref filled with the AgentContext once it is created; lets runner read telemetry. */
    readonly executionRef?: { context: AgentContext | null };
}

interface CancelSignalLegacy {
    readonly aborted?: boolean;
    readonly is_set?: boolean;
}

export abstract class GatewayWorker {
    public readonly workerId: string;
    protected readonly redis: Redis;
    public readonly registry: WorkerRegistry;
    public readonly pluginRegistry: PluginRegistry;
    protected readonly workspaceManager?: WorkspaceManager;
    protected readonly sandbox?: HookSandbox;
    public readonly storage?: FileStorage;
    private _heartbeat: WorkerHeartbeat | null = null;

    public constructor(
        workerId: string,
        registry?: WorkerRegistry,
        redisClient?: Redis,
        pluginRegistry?: PluginRegistry,
        workspaceManager?: WorkspaceManager,
        sandbox?: HookSandbox,
        storage?: FileStorage
    ) {
        this.workerId = workerId;
        this.redis = redisClient ?? getRedis();
        this.registry = registry ?? new WorkerRegistry(this.redis);
        this.pluginRegistry = pluginRegistry ?? new PluginRegistry();
        this.workspaceManager = workspaceManager;
        this.sandbox = sandbox;
        this.storage = storage;
    }

    /** Return the heartbeat interval in seconds. */
    get heartbeatInterval(): number {
        return RegistryKeys.WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS;
    }

    /** Return the worker online lease TTL in seconds. */
    get heartbeatLeaseTtlSeconds(): number {
        return RegistryKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS;
    }

    abstract getAgentTypes(): ReadonlyArray<string>;

    abstract processCommand(command: GatewayCommand, context: AgentContext): Promise<ProcessCommandResult>;

    async onCancelTask(_command: unknown): Promise<void> {
        console.log(`[${this.workerId}] Received cancel request`);
    }

    async startHeartbeat(): Promise<void> {
        await this.pluginRegistry.onWorkerStartup(this);
        this._heartbeat = new WorkerHeartbeat(
            this.workerId,
            [...this.getAgentTypes()],
            this.redis,
            this.registry,
            this.heartbeatInterval * 1000,
            this.heartbeatLeaseTtlSeconds
        );
        await this._heartbeat.start();
    }

    async stopHeartbeat(): Promise<void> {
        if (this._heartbeat) {
            await this._heartbeat.stop();
            this._heartbeat = null;
        }
    }

    async handleMessage(
        command: GatewayCommand,
        options: HandleMessageOptions = {}
    ): Promise<AgentTaskResult> {
        const traceId = command.header.traceId || uuidv4().replace(/-/g, '');
        const context = new AgentContext(
            command.header.sessionId,
            traceId,
            this.redis,
            command.header.targetAgentType,
            command.header.messageId,
            command,
            options.cancelSignal,
            options.cancelReason || '',
            this.pluginRegistry,
            options.executionId || '',
            options.spanRecorder
        );
        context.setAgentConfigs(this.pluginRegistry.agentConfigs);
        if (options.executionRef) {
            options.executionRef.context = context;
        }

        const isResume = command instanceof ResumeCommand;
        const sourceAgentType = command.header.sourceAgentType;
        const hasSourceAgent = !!sourceAgentType && !isResume;

        // Determine parent message id - restore from execution if resumed
        let parentMessageId = command.header.parentMessageId || '';
        if (options.execution?.isResumed && options.execution?.parentMessageId) {
            parentMessageId = options.execution.parentMessageId;
            console.log(`[${this.workerId}] Task Resumed: Restored parent_message_id=${parentMessageId}`);
        } else {
            console.log(`[${this.workerId}] New Task: message_id=${command.header.messageId}, parent_message_id=${parentMessageId}`);
        }

        // Permission transfer tracking
        let permissionTransferred = false;

        console.log(`[${this.workerId}] Processing message: ${command.header.messageId}`);
        let workspacePaths: { private: string; public?: string } | null = null;
        const prevWorkspace = getActiveWorkspace();

        try {
            await this.pluginRegistry.onTaskStart(context);
            if (!isResume && command instanceof AskAgentCommand) {
                await HistoryProvider.saveMessage(command.header.sessionId, 'user', command.content as any, {
                    message_id: command.header.messageId,
                    trace_id: command.header.traceId,
                });
            }

            if (this.workspaceManager) {
                workspacePaths = await this.workspaceManager.setupWorkspace(
                    command.header.sessionId,
                    command.header.messageId
                ) as { private: string; public?: string } | null;
                if (this.sandbox) {
                    this.sandbox.install();
                }
                if (workspacePaths?.private) {
                    setActiveWorkspace(workspacePaths.private);
                }
            }

            // Pre-processing cancellation check: if cancelled before processing, bail out immediately
            const cancelSignal = options.cancelSignal as CancelSignalLegacy | AbortSignal | undefined;
            if (cancelSignal && ((cancelSignal as CancelSignalLegacy).aborted || (cancelSignal as CancelSignalLegacy).is_set)) {
                throw new TaskCancelledError(options.cancelReason || 'task cancelled before processing');
            }

            if (isResume) {
                // Persist agent return state
                await this.persistAgentReturnState(workspacePaths, command);

                // Check for scatter-gather join
                if (command.header.taskGroupId) {
                    const groupKey = QueueNames.task_group(command.header.taskGroupId);
                    const resultsKey = QueueNames.task_group_results(command.header.taskGroupId);
                    const totalStr = await this.redis.hget(groupKey, TASK_GROUP_FIELD_TOTAL);
                    if (totalStr !== null) {
                        // Store result in Redis Hash for distributed access
                        const resultData = {
                            status: (command as ResumeCommand).status,
                            reply_data: (command as ResumeCommand).replyData,
                            content: (command as ResumeCommand).content,
                            metadata: command.header.metadata,
                            extra_payload: (command as ResumeCommand).extraPayload,
                        };
                        await this.redis.hset(resultsKey, command.header.messageId, JSON.stringify(resultData));
                        await this.redis.expire(resultsKey, TASK_GROUP_TTL_SECONDS);

                        const completed = await this.redis.hincrby(groupKey, TASK_GROUP_FIELD_COMPLETED, 1);
                        if (completed < parseInt(totalStr, 10)) {
                            console.log(`[${this.workerId}] TaskGroup ${command.header.taskGroupId} completed ${completed}/${totalStr}, waiting...`);
                            return new AgentTaskResult({ status: `${AgentState.QUEUED}: waiting_for_group` });
                        }
                        console.log(`[${this.workerId}] TaskGroup ${command.header.taskGroupId} ALL COMPLETED (${totalStr})!`);
                    }
                }
                await context.emitState({ state: AgentState.RESUMED });
            }

            const result = await this.processCommand(command, context);
            const taskResult = normalizeProcessResult(result);

            // Determine final status from result
            const finalStatus = taskResult.status;

            if (hasSourceAgent) {
                permissionTransferred = true;
                const returnOptions = {
                    content: taskResult.content,
                    metadata: taskResult.metadata,
                    extraPayload: taskResult.extraPayload,
                };
                const returnInfo = { status: taskResult.status, replyData: taskResult.replyData, ...returnOptions };
                await this.pluginRegistry.onAgentReturnStart(context, command, returnInfo);
                try {
                    await this.enqueueAgentReturn(command, taskResult.status, taskResult.replyData, returnOptions);
                    await this.pluginRegistry.onAgentReturnComplete(context, command, returnInfo);
                } catch (returnErr: any) {
                    await this.pluginRegistry.onAgentReturnError(context, command, returnInfo, returnErr instanceof Error ? returnErr : new Error(String(returnErr)));
                    throw returnErr;
                }
            }
            await this.pluginRegistry.onTaskComplete(context, result);

            // Extract final message and emit FINAL_ANSWER
            let finalMessage: string | null = null;
            if (typeof taskResult.content === 'string' && taskResult.content) {
                finalMessage = taskResult.content;
            } else if (typeof taskResult.replyData === 'string' && taskResult.replyData) {
                finalMessage = taskResult.replyData;
            } else if (taskResult.replyData !== null && taskResult.replyData !== undefined) {
                finalMessage = JSON.stringify(taskResult.replyData);
            }
            taskResult.finalAnswer = finalMessage || "";

            if (finalMessage !== null) {
                await context.emitChunk(finalMessage, EventType.FINAL_ANSWER);
            }

            // Emit APP_STREAM_RESPONSE if conditions are met
            const shouldEmitStreamEnd = !hasSourceAgent && isTerminalState(finalStatus) && !permissionTransferred && !context.isSuspended();
            if (shouldEmitStreamEnd) {
                if (!context.isStreamFinished()) {
                    await context.emitChunk('', EventType.APP_STREAM_RESPONSE);
                }
            } else {
                await context.flushToHistory();
            }

            return taskResult;
        } catch (error: unknown) {
            if (error instanceof TaskCancelledError || (error instanceof Error && error.name === 'TaskCancelledError')) {
                // Check if parent execution also has cancel_requested — if so, skip callback
                let shouldCallback = hasSourceAgent;
                if (shouldCallback && parentMessageId) {
                    const parentExec = await this.registry.getExecutionByMessageId(parentMessageId, command.header.sessionId);
                    if (parentExec?.cancel_requested) {
                        shouldCallback = false;
                    }
                }
                if (shouldCallback) {
                    const cancelReplyData = { reason: String(error instanceof Error ? error.message : error) };
                    const cancelReturnInfo = { status: AgentState.CANCELLED, replyData: cancelReplyData };
                    await this.pluginRegistry.onAgentReturnStart(context, command, cancelReturnInfo);
                    try {
                        await this.enqueueAgentReturn(command, AgentState.CANCELLED, cancelReplyData);
                        await this.pluginRegistry.onAgentReturnComplete(context, command, cancelReturnInfo);
                    } catch (returnErr: any) {
                        await this.pluginRegistry.onAgentReturnError(context, command, cancelReturnInfo, returnErr instanceof Error ? returnErr : new Error(String(returnErr)));
                    }
                }

                const shouldEmitStreamEnd = !hasSourceAgent && !permissionTransferred;
                if (shouldEmitStreamEnd) {
                    await context.emitChunk('', EventType.APP_STREAM_RESPONSE);
                } else {
                    await context.flushToHistory();
                }
                return new AgentTaskResult({ status: AgentState.CANCELLED });
            }
            const err = error instanceof Error ? error : new Error(String(error));
            console.error(`[${this.workerId}] Task failed:`, err);
            if (hasSourceAgent) {
                const failedReplyData = { error: String(error) };
                const failedReturnInfo = { status: AgentState.FAILED, replyData: failedReplyData };
                await this.pluginRegistry.onAgentReturnStart(context, command, failedReturnInfo);
                try {
                    await this.enqueueAgentReturn(command, AgentState.FAILED, failedReplyData);
                    await this.pluginRegistry.onAgentReturnComplete(context, command, failedReturnInfo);
                } catch (returnErr: any) {
                    await this.pluginRegistry.onAgentReturnError(context, command, failedReturnInfo, returnErr instanceof Error ? returnErr : new Error(String(returnErr)));
                }
            }
            await this.pluginRegistry.onTaskError(context, err);

            const shouldEmitStreamEnd = !hasSourceAgent && !permissionTransferred;
            if (shouldEmitStreamEnd) {
                await context.emitChunk('', EventType.APP_STREAM_RESPONSE);
            } else {
                await context.flushToHistory();
            }

            return new AgentTaskResult({ status: AgentState.FAILED });
        } finally {
            setActiveWorkspace(prevWorkspace);
            if (this.sandbox) {
                this.sandbox.uninstall();
            }
            if (this.workspaceManager && workspacePaths) {
                await this.workspaceManager.cleanupTask(command.header.sessionId, command.header.messageId);
            }
        }
    }

    private async enqueueAgentReturn(
        command: GatewayCommand,
        status: string,
        replyData: JsonValue,
        options: {
            readonly content?: WireContent;
            readonly metadata?: Readonly<Record<string, JsonValue>>;
            readonly extraPayload?: Readonly<Record<string, JsonValue>>;
        } = {}
    ): Promise<void> {
        const header = command.header;
        if (!header.sourceAgentType) return;
        const mergedMetadata = {
            ...header.metadata,
            ...(options.metadata ?? {}),
        };

        const callbackMsg = new ResumeCommand(
            new MessageHeader(`msg-${uuidv4().slice(0, 8)}`, header.sessionId, header.traceId, {
                sourceAgentType: header.targetAgentType || this.workerId,
                targetAgentType: header.sourceAgentType,
                parentMessageId: header.messageId,
                taskGroupId: header.taskGroupId,
                userCode: header.userCode,
                userName: header.userName,
                metadata: mergedMetadata,
            }),
            options.content ?? '',
            status,
            replyData,
            options.extraPayload ?? {}
        );

        await this.redis.xadd(
            QueueNames.ctrl_stream(callbackMsg.header.targetAgentType),
            '*',
            'data',
            JSON.stringify(callbackMsg.toDict())
        );
    }

    /** Persist agent return state to filesystem (aligned with Python _persist_agent_return_state). */
    protected async persistAgentReturnState(paths: { private?: string; public?: string } | null, command: GatewayCommand): Promise<void> {
        if (!paths || !paths.public) return;

        const header = command.header;
        const stateDir = path.join(paths.public, 'session', 'agent_returns');

        let stateFile: string;
        if (header.taskGroupId) {
            const groupDir = path.join(stateDir, header.taskGroupId);
            await fs.mkdir(groupDir, { recursive: true });
            stateFile = path.join(groupDir, `${header.messageId}.json`);
        } else {
            await fs.mkdir(stateDir, { recursive: true });
            const fileKey = header.parentMessageId || header.messageId;
            stateFile = path.join(stateDir, `${fileKey}.json`);
        }

        const stateData = {
            message_id: header.messageId,
            parent_message_id: header.parentMessageId,
            source_agent_type: header.sourceAgentType,
            target_agent_type: header.targetAgentType,
            user_code: header.userCode,
            user_name: header.userName,
            action_type: (command as any).constructor?.name || 'Unknown',
            status: (command as ResumeCommand).status || '',
            content: (command as ResumeCommand).content || null,
            reply_data: (command as ResumeCommand).replyData || null,
            trace_id: header.traceId,
            session_id: header.sessionId,
            metadata: header.metadata || {},
        };

        await fs.writeFile(stateFile, JSON.stringify(stateData, null, 2), 'utf-8');
    }
}

/**
 * Anonymous Worker class that allows passing callback functions to process tasks without inheritance.
 * Suitable for decoupled mode or quick integration.
 */
export class AnonymousWorker extends GatewayWorker {
    private readonly agentTypes: ReadonlyArray<string>;
    private readonly onTask: (command: GatewayCommand, context: AgentContext) => Promise<ProcessCommandResult>;

    constructor(options: {
        readonly workerId: string;
        readonly agentTypes: ReadonlyArray<string>;
        readonly onTask: (command: GatewayCommand, context: AgentContext) => Promise<ProcessCommandResult>;
        readonly registry?: WorkerRegistry;
        readonly redisClient?: Redis;
        readonly pluginRegistry?: PluginRegistry;
        readonly storage?: FileStorage;
    }) {
        const redis = options.redisClient ?? new Redis();
        const registry = options.registry ?? new WorkerRegistry(redis);
        const pluginRegistry = options.pluginRegistry ?? new PluginRegistry();
        super(options.workerId, registry, redis, pluginRegistry, undefined, undefined, options.storage);
        this.agentTypes = options.agentTypes;
        this.onTask = options.onTask;
    }

    getAgentTypes(): ReadonlyArray<string> {
        return this.agentTypes;
    }

    async processCommand(command: GatewayCommand, context: AgentContext): Promise<ProcessCommandResult> {
        return this.onTask(command, context);
    }
}

import * as fs from 'fs/promises';
import * as path from 'path';
import { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { getRedis } from './redis_client';
import { GatewayCommand, ResumeCommand, AskAgentCommand } from './protocol/commands';
import { AgentState, isTerminalState } from './protocol/agent_state';
import { EventType } from './protocol/event_type';
import { AgentContext, TaskCancelledError } from './context';
import { QueueNames, TASK_GROUP_TTL_SECONDS, TASK_GROUP_FIELD_TOTAL, TASK_GROUP_FIELD_COMPLETED } from './constants';
import { WorkerRegistry } from './registry';
import { MessageHeader } from './protocol/message_header';
import { PluginRegistry } from './extensions/registry';
import { HistoryProvider } from './history';
import { WorkspaceManager } from './workspace';
import { HookSandbox, getActiveWorkspace, setActiveWorkspace } from './sandbox';

// === Types ===
interface HandleMessageOptions {
    readonly cancelSignal?: AbortSignal | CancelSignalLegacy;
    readonly cancelReason?: string;
    readonly execution?: {
        readonly parentMessageId?: string;
        readonly isResumed?: boolean;
    };
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

    public constructor(
        workerId: string,
        registry?: WorkerRegistry,
        redisClient?: Redis,
        pluginRegistry?: PluginRegistry,
        workspaceManager?: WorkspaceManager,
        sandbox?: HookSandbox
    ) {
        this.workerId = workerId;
        this.redis = redisClient ?? getRedis();
        this.registry = registry ?? new WorkerRegistry(this.redis);
        this.pluginRegistry = pluginRegistry ?? new PluginRegistry();
        this.workspaceManager = workspaceManager;
        this.sandbox = sandbox;
    }

    abstract getCapabilities(): ReadonlyArray<string>;

    abstract processCommand(command: GatewayCommand, context: AgentContext): Promise<unknown>;

    async onCancelTask(_command: unknown): Promise<void> {
        console.log(`[${this.workerId}] Received cancel request`);
    }

    private heartbeatTimer: NodeJS.Timeout | null = null;

    async startHeartbeat(): Promise<void> {
        await this.pluginRegistry.onWorkerStartup(this);
        // Initial registration
        await this.registry.registerWorker(this.workerId, [...this.getCapabilities()]);

        // Periodic refresh every 30s
        this.heartbeatTimer = setInterval(async () => {
            try {
                await this.registry.registerWorker(this.workerId, [...this.getCapabilities()]);
            } catch (err) {
                console.error(`[${this.workerId}] Heartbeat failed:`, err);
            }
        }, 30000);
    }

    stopHeartbeat(): void {
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }
    }

    async handleMessage(
        command: GatewayCommand,
        options: HandleMessageOptions = {}
    ): Promise<string> {
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
            this.pluginRegistry
        );
        context.setAgentConfigs(this.pluginRegistry.agentConfigs);

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
                await HistoryProvider.saveMessage(command.header.sessionId, 'user', command.content, {
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
                        };
                        await this.redis.hset(resultsKey, command.header.messageId, JSON.stringify(resultData));
                        await this.redis.expire(resultsKey, TASK_GROUP_TTL_SECONDS);

                        const completed = await this.redis.hincrby(groupKey, TASK_GROUP_FIELD_COMPLETED, 1);
                        if (completed < parseInt(totalStr, 10)) {
                            console.log(`[${this.workerId}] TaskGroup ${command.header.taskGroupId} completed ${completed}/${totalStr}, waiting...`);
                            return `${AgentState.QUEUED}: waiting_for_group`;
                        }
                        console.log(`[${this.workerId}] TaskGroup ${command.header.taskGroupId} ALL COMPLETED (${totalStr})!`);
                    }
                }
                await context.emitState({ state: AgentState.RESUMED });
            }

            const result = await this.processCommand(command, context);

            // Determine final status from result
            let finalStatus = AgentState.COMPLETED;
            if (typeof result === 'string' && Object.values(AgentState).includes(result as AgentState)) {
                finalStatus = result as AgentState;
            } else if (typeof result === 'object' && result !== null && 'status' in result) {
                finalStatus = String((result as any).status) as AgentState;
            }

            if (hasSourceAgent) {
                permissionTransferred = true;
                await this.enqueueAgentReturn(command, AgentState.COMPLETED, result);
                await context.emitState({ state: `${AgentState.QUEUED}: ${sourceAgentType}` });
            } else {
                await context.emitState({ state: AgentState.COMPLETED });
            }
            await this.pluginRegistry.onTaskComplete(context, result);

            // Emit APP_STREAM_RESPONSE if conditions are met
            const shouldEmitStreamEnd = !hasSourceAgent && isTerminalState(finalStatus) && !permissionTransferred;
            if (shouldEmitStreamEnd) {
                await context.emitChunk('', EventType.APP_STREAM_RESPONSE);
            } else {
                await context.flushToHistory();
            }

            return finalStatus;
        } catch (error: unknown) {
            if (error instanceof TaskCancelledError || (error instanceof Error && error.name === 'TaskCancelledError')) {
                if (hasSourceAgent) {
                    await this.enqueueAgentReturn(command, AgentState.CANCELLED, { reason: String(error instanceof Error ? error.message : error) });
                }
                await context.emitState({ state: AgentState.CANCELLING });
                await context.emitState({ state: AgentState.CANCELLED });

                const shouldEmitStreamEnd = !hasSourceAgent && !permissionTransferred;
                if (shouldEmitStreamEnd) {
                    await context.emitChunk('', EventType.APP_STREAM_RESPONSE);
                } else {
                    await context.flushToHistory();
                }
                return AgentState.CANCELLED;
            }
            const err = error instanceof Error ? error : new Error(String(error));
            console.error(`[${this.workerId}] Task failed:`, err);
            if (hasSourceAgent) {
                await this.enqueueAgentReturn(command, AgentState.FAILED, { error: String(error) });
            }
            await context.emitState({ state: `${AgentState.FAILED}: ${error}` });
            await this.pluginRegistry.onTaskError(context, err);

            const shouldEmitStreamEnd = !hasSourceAgent && !permissionTransferred;
            if (shouldEmitStreamEnd) {
                await context.emitChunk('', EventType.APP_STREAM_RESPONSE);
            } else {
                await context.flushToHistory();
            }

            return AgentState.FAILED;
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

    private async enqueueAgentReturn(command: GatewayCommand, status: string, replyData: any): Promise<void> {
        const header = command.header;
        if (!header.sourceAgentType) return;

        const callbackMsg = new ResumeCommand(
            new MessageHeader(`msg-${uuidv4().slice(0, 8)}`, header.sessionId, header.traceId, {
                sourceAgentType: header.targetAgentType || this.workerId,
                targetAgentType: header.sourceAgentType,
                parentMessageId: header.messageId,
                taskGroupId: header.taskGroupId,
                tenantId: header.tenantId,
            }),
            '',
            status,
            replyData
        );

        await this.redis.xadd(
            QueueNames.ctrl_stream(callbackMsg.header.targetAgentType),
            '*',
            'data',
            JSON.stringify(callbackMsg.toDict())
        );
    }

    private async persistAgentReturnState(paths: { private?: string; public?: string } | null, command: GatewayCommand): Promise<void> {
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
    private readonly capabilities: ReadonlyArray<string>;
    private readonly onTask: (command: GatewayCommand, context: AgentContext) => Promise<unknown>;

    constructor(options: {
        readonly workerId: string;
        readonly capabilities: ReadonlyArray<string>;
        readonly onTask: (command: GatewayCommand, context: AgentContext) => Promise<unknown>;
        readonly registry?: WorkerRegistry;
        readonly redisClient?: Redis;
        readonly pluginRegistry?: PluginRegistry;
    }) {
        const redis = options.redisClient ?? new Redis();
        const registry = options.registry ?? new WorkerRegistry(redis);
        const pluginRegistry = options.pluginRegistry ?? new PluginRegistry();
        super(options.workerId, registry, redis, pluginRegistry);
        this.capabilities = options.capabilities;
        this.onTask = options.onTask;
    }

    getCapabilities(): ReadonlyArray<string> {
        return this.capabilities;
    }

    async processCommand(command: GatewayCommand, context: AgentContext): Promise<unknown> {
        return this.onTask(command, context);
    }
}

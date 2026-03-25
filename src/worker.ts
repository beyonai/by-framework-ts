import { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { getRedis } from './redis_client';
import { GatewayCommand, ResumeCommand, AskAgentCommand } from './protocol/commands';
import { AgentState } from './protocol/agent_state';
import { AgentContext, TaskCancelledError } from './context';
import { QueueNames, RegistryKeys } from './constants';
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
        const sourceAgentId = command.header.sourceAgentId;
        const hasSourceAgent = !!sourceAgentId && !isResume;

        console.log(`[${this.workerId}] Processing message: ${command.header.messageId}`);
        let workspacePaths: { private: string } | null = null;
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
                );
                if (this.sandbox) {
                    this.sandbox.install();
                }
                setActiveWorkspace(workspacePaths.private);
            }

            if (isResume) {
                // Check for scatter-gather join
                if (command.header.taskGroupId) {
                    const groupKey = RegistryKeys.task_group(command.header.taskGroupId);
                    const totalStr = await this.redis.hget(groupKey, "total");
                    if (totalStr !== null) {
                        const completed = await this.redis.hincrby(groupKey, "completed", 1);
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

            if (hasSourceAgent) {
                await this.enqueueAgentReturn(command, 'SUCCESS', result);
                await context.emitState({ state: `${AgentState.QUEUED}: ${sourceAgentId}` });
            } else {
                await context.emitState({ state: AgentState.COMPLETED });
            }
            await this.pluginRegistry.onTaskComplete(context, result);
            await context.flushToHistory();
            return AgentState.COMPLETED;
        } catch (error: unknown) {
            if (error instanceof TaskCancelledError || (error instanceof Error && error.name === 'TaskCancelledError')) {
                if (hasSourceAgent) {
                    await this.enqueueAgentReturn(command, AgentState.CANCELLED, { reason: String(error instanceof Error ? error.message : error) });
                }
                await context.emitState({ state: AgentState.CANCELLING });
                await context.emitState({ state: AgentState.CANCELLED });
                return AgentState.CANCELLED;
            }
            const err = error instanceof Error ? error : new Error(String(error));
            console.error(`[${this.workerId}] Task failed:`, err);
            if (hasSourceAgent) {
                await this.enqueueAgentReturn(command, 'FAILED', { error: String(error) });
            }
            await context.emitState({ state: `${AgentState.FAILED}: ${error}` });
            await this.pluginRegistry.onTaskError(context, err);
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
        if (!header.sourceAgentId) return;

        const callbackMsg = new ResumeCommand(
            new MessageHeader(`msg-${uuidv4().slice(0, 8)}`, header.sessionId, header.traceId, {
                sourceAgentId: header.targetAgentType || this.workerId,
                targetAgentType: header.sourceAgentId,
                parentMessageId: header.messageId,
                taskGroupId: header.taskGroupId, // 关键：透传任务组 ID
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
}

/**
 * 匿名 Worker 类，允许通过传入回调函数来处理任务，无需继承。
 * 适用于解耦模式或快速集成。
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

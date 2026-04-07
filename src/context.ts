import { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { QueueNames, TASK_GROUP_FIELD_TOTAL, TASK_GROUP_FIELD_COMPLETED, TASK_GROUP_TTL_SECONDS } from './constants';
import { EventType } from './protocol/event_type';
import { AgentState } from './protocol/agent_state';
import { AskAgentCommand } from './protocol/commands';
import { MessageHeader } from './protocol/message_header';
import {
    StateChangeEvent,
    StreamChunkEvent,
    ArtifactEvent,
    AskUserEvent,
} from './protocol/events';

import { GatewayDataEmitter } from './emitter';
import { AgentConfig } from './extensions/agent_config';
import type { PluginRegistry } from './extensions/registry';
import { HistoryProvider } from './history';

export class TaskCancelledError extends Error {
    constructor(message: string = 'task cancelled') {
        super(message);
        this.name = 'TaskCancelledError';
    }
}

interface CancelSignalLegacy {
    readonly aborted?: boolean;
    readonly is_set?: boolean;
    readonly reason?: string;
}

interface CallAgentResult {
    status: string;
    messageId: string;
    parentMessageId?: string;
    targetAgentType: string;
    error?: string;
    error_code?: string;
}

interface DispatchedTask {
    message_id: string;
    target_agent_type: string;
}

interface DispatchGroupResult {
    status: string;
    taskGroupId: string;
    dispatchedTasks: DispatchedTask[];
}

interface GroupResult {
    message_id: string;
    status: string;
    reply_data: unknown;
    content?: string;
}

export class AgentContext {
    private emitter: GatewayDataEmitter;
    private agentConfigs: ReadonlyArray<AgentConfig> = [];
    private prevAgentConfigs: ReadonlyArray<AgentConfig> = [];
    private responseBuffer: ReadonlyArray<string> = [];
    private historySaved = false;
    private _isSuspended = false;
    private _permissionTransferred = false;

    constructor(
        public readonly sessionId: string,
        public readonly traceId: string,
        private readonly redis: Redis,
        private readonly currentAgentType: string = '',
        private readonly currentMessageId: string = '',
        public readonly currentCommand?: unknown,
        private readonly cancelSignal?: AbortSignal | CancelSignalLegacy,
        private readonly cancelReason: string = '',
        public readonly pluginRegistry?: PluginRegistry
    ) {
        this.emitter = new GatewayDataEmitter(this.redis);
    }

    setAgentConfigs(newConfigs: ReadonlyArray<AgentConfig>): void {
        this.agentConfigs = [...newConfigs];
    }

    listAgentConfigs(): ReadonlyArray<AgentConfig> {
        return Object.freeze([...this.agentConfigs]);
    }

    getAgentConfig(agentId: string): AgentConfig | undefined {
        return this.agentConfigs.find((c) => c.agent_id === agentId);
    }

    freezePrevAgentConfigs(): void {
        this.prevAgentConfigs = [...this.agentConfigs];
    }

    getPrevAgentConfigs(): ReadonlyArray<AgentConfig> {
        return this.prevAgentConfigs;
    }

    isSuspended(): boolean {
        return this._isSuspended;
    }

    isPermissionTransferred(): boolean {
        return this._permissionTransferred;
    }

    async callTool(name: string, kwargs: Readonly<Record<string, unknown>> = {}): Promise<unknown> {
        for (const config of this.agentConfigs) {
            const tool = config.tools?.[name];
            if (typeof tool === 'function') {
                return await tool(kwargs);
            }
        }
        throw new Error(`Tool '${name}' not found in any agent config`);
    }

    isCancelRequested(): boolean {
        return Boolean(this.cancelSignal && (
            (this.cancelSignal as CancelSignalLegacy).aborted ||
            (this.cancelSignal as CancelSignalLegacy).is_set
        ));
    }

    async checkCancelled(): Promise<void> {
        if (this.isCancelRequested()) {
            const signalReason = this.cancelSignal && 'reason' in this.cancelSignal
                ? String((this.cancelSignal as { reason?: string }).reason || '')
                : '';
            throw new TaskCancelledError(this.cancelReason || signalReason || 'task cancelled');
        }
    }

    async getActiveWorkers(): Promise<Record<string, unknown>> {
        const { WorkerRegistry } = await import('./registry');
        const registry = new WorkerRegistry(this.redis);
        return registry.getAllWorkers() as unknown as Record<string, unknown>;
    }

    private generateMessageId(): string {
        return `msg-${uuidv4().slice(0, 8)}`;
    }

    private async emitEvent(params: {
        readonly eventType: string;
        readonly data?: Readonly<Record<string, unknown>>;
        readonly stateMsg?: string;
        readonly artifactUrl?: string;
        readonly metadata?: Readonly<Record<string, unknown>>;
    }): Promise<void> {
        await this.emitter.emitEvent({
            sessionId: this.sessionId,
            traceId: this.traceId,
            sourceAgentType: this.currentAgentType,
            messageId: this.currentMessageId,
            ...params
        });
    }

    async emitChunk(event: StreamChunkEvent | string, eventType?: string): Promise<void> {
        const content = typeof event === 'string' ? event : (event.content || '');
        if (content) {
            this.responseBuffer = [...this.responseBuffer, content];
        }
        await this.emitter.emitChunk(this.sessionId, this.traceId, event, {
            sourceAgentType: this.currentAgentType,
            messageId: this.currentMessageId,
            eventType: eventType as EventType
        });
        if (eventType === EventType.APP_STREAM_RESPONSE) {
            await this.flushToHistory();
        }
    }

    async emitState(event: StateChangeEvent | string, eventType?: string): Promise<void> {
        await this.emitter.emitState(this.sessionId, this.traceId, event, {
            sourceAgentType: this.currentAgentType,
            messageId: this.currentMessageId,
            eventType: eventType as EventType
        });
    }

    async emitArtifact(event: ArtifactEvent | string, eventType?: string): Promise<void> {
        await this.emitter.emitArtifact(this.sessionId, this.traceId, event, {
            sourceAgentType: this.currentAgentType,
            messageId: this.currentMessageId,
            eventType: eventType as EventType
        });
    }

    async askUser(event: AskUserEvent | string): Promise<{ readonly status: string }> {
        await this.emitter.askUser(this.sessionId, this.traceId, event, {
            sourceAgentType: this.currentAgentType,
            messageId: this.currentMessageId,
        });
        this._isSuspended = true;
        return { status: AgentState.WAITING_USER };
    }

    async flushToHistory(): Promise<void> {
        if (this.historySaved || this.responseBuffer.length === 0) {
            return;
        }
        const fullContent = this.responseBuffer.join('');
        await HistoryProvider.saveMessage(this.sessionId, 'assistant', fullContent, {
            trace_id: this.traceId,
            agent_id: this.currentAgentType,
            message_id: this.currentMessageId,
        });
        this.historySaved = true;
    }

    async callAgent(params: {
        readonly targetAgentType: string;
        readonly content: string | Array<Record<string, unknown>>;
        readonly payload?: Readonly<Record<string, unknown>>;
        readonly waitForReply?: boolean;
        readonly metadata?: Readonly<Record<string, unknown>>;
        readonly messageId?: string;
        readonly parentMessageId?: string;
        readonly probeAgentType?: boolean;
    }): Promise<CallAgentResult> {
        const probeAgentType = params.probeAgentType ?? true;

        // Probe agent type if enabled
        if (probeAgentType) {
            const { WorkerRegistry } = await import('./registry');
            const registry = new WorkerRegistry(this.redis);
            const [hasCap] = await registry.hasAgentType(params.targetAgentType, true);
            if (!hasCap) {
                return {
                    status: AgentState.FAILED,
                    messageId: '',
                    parentMessageId: params.parentMessageId || this.currentMessageId,
                    targetAgentType: params.targetAgentType,
                    error: `No alive worker found with agent type '${params.targetAgentType}'`,
                    error_code: 'AGENT_TYPE_NOT_FOUND',
                };
            }
        }

        const messageId = params.messageId || this.generateMessageId();
        const parentMessageId = params.parentMessageId || this.currentMessageId;
        const waitForReply = params.waitForReply ?? true;

        const mergedPayload: Record<string, unknown> = { ...(params.payload || {}) };
        if (waitForReply) {
            mergedPayload.wait_for_reply = true;
            this._isSuspended = true;
        } else {
            this._permissionTransferred = true;
        }

        const command = new AskAgentCommand(
            new MessageHeader(messageId, this.sessionId, this.traceId, {
                sourceAgentType: waitForReply ? this.currentAgentType : '',
                targetAgentType: params.targetAgentType,
                parentMessageId: parentMessageId,
                metadata: params.metadata,
            }),
            params.content as string | unknown[],
            waitForReply,
            Object.fromEntries(Object.entries(mergedPayload).filter(([key]) => key !== 'wait_for_reply'))
        );

        await this.redis.xadd(
            QueueNames.ctrl_stream(params.targetAgentType),
            '*',
            'data',
            JSON.stringify(command.toDict())
        );

        return {
            status: AgentState.QUEUED,
            messageId,
            parentMessageId,
            targetAgentType: params.targetAgentType,
        };
    }

    /**
     * Dispatch multiple tasks concurrently as a group (Scatter-Gather).
     *
     * @param tasks - Array of task objects with targetAgentType, content, payload
     * @param waitForReply - If true, sets up Redis counters to wait for all
     * @param messageId - Optional custom message ID
     * @param parentMessageId - Optional custom parent message ID
     * @returns DispatchGroupResult with status, taskGroupId, and dispatched tasks
     */
    async dispatchGroup(params: {
        readonly tasks: ReadonlyArray<{
            readonly targetAgentType: string;
            readonly content: string;
            readonly payload?: Readonly<Record<string, unknown>>;
            readonly metadata?: Readonly<Record<string, unknown>>;
        }>;
        readonly waitForReply?: boolean;
        readonly messageId?: string;
        readonly parentMessageId?: string;
    }): Promise<DispatchGroupResult> {
        const { tasks, waitForReply = true, messageId, parentMessageId } = params;

        if (!tasks || tasks.length === 0) {
            return { status: 'EMPTY', taskGroupId: '', dispatchedTasks: [] };
        }

        const taskGroupId = `tg-${uuidv4().slice(0, 8)}`;
        const wait = waitForReply ?? true;

        // Setup Redis counters if waiting for replies
        if (wait) {
            const groupKey = QueueNames.task_group(taskGroupId);
            await this.redis.hset(groupKey, {
                [TASK_GROUP_FIELD_TOTAL]: tasks.length.toString(),
                [TASK_GROUP_FIELD_COMPLETED]: '0',
                source_agent_type: this.currentAgentType,
            });
            await this.redis.expire(groupKey, TASK_GROUP_TTL_SECONDS);
            this._isSuspended = true;
        } else {
            this._permissionTransferred = true;
        }

        const dispatchedTasks: DispatchedTask[] = [];

        for (const task of tasks) {
            const currentMessageId = this.generateMessageId();
            const currentParentMessageId = parentMessageId || this.currentMessageId;

            const mergedPayload: Record<string, unknown> = { ...(task.payload || {}) };
            if (wait) {
                mergedPayload.wait_for_reply = true;
            }

            const command = new AskAgentCommand(
                new MessageHeader(currentMessageId, this.sessionId, this.traceId, {
                    sourceAgentType: wait ? this.currentAgentType : '',
                    targetAgentType: task.targetAgentType,
                    parentMessageId: currentParentMessageId,
                    taskGroupId: taskGroupId,
                    metadata: task.metadata,
                }),
                task.content,
                wait,
                Object.fromEntries(Object.entries(mergedPayload).filter(([key]) => key !== 'wait_for_reply'))
            );

            await this.redis.xadd(
                QueueNames.ctrl_stream(task.targetAgentType),
                '*',
                'data',
                JSON.stringify(command.toDict())
            );

            dispatchedTasks.push({
                message_id: currentMessageId,
                target_agent_type: task.targetAgentType,
            });
        }

        return {
            status: 'GROUP_QUEUED',
            taskGroupId,
            dispatchedTasks,
        };
    }

    /**
     * Collect results from all tasks in a group.
     *
     * @param taskGroupId - The task group ID returned by dispatchGroup
     * @param timeout - Maximum time to wait in seconds (default 30)
     * @returns Array of results from all completed tasks
     */
    async collectGroupResults(taskGroupId: string, timeout: number = 30): Promise<GroupResult[]> {
        if (!taskGroupId) {
            return [];
        }

        const resultsKey = QueueNames.task_group_results(taskGroupId);
        const groupKey = QueueNames.task_group(taskGroupId);

        const totalStr = await this.redis.hget(groupKey, TASK_GROUP_FIELD_TOTAL);
        const total = totalStr ? parseInt(totalStr, 10) : Infinity;

        const results: GroupResult[] = [];
        const startTime = Date.now();

        while (results.length < total) {
            const elapsed = (Date.now() - startTime) / 1000;
            if (elapsed >= timeout) {
                break;
            }

            const rawResults = await this.redis.hgetall(resultsKey);
            if (rawResults) {
                for (const [msgId, data] of Object.entries(rawResults)) {
                    try {
                        const parsed = JSON.parse(data as string);
                        results.push({
                            message_id: msgId,
                            status: parsed.status || '',
                            reply_data: parsed.reply_data,
                            content: parsed.content,
                        });
                    } catch {
                        // Skip invalid JSON
                    }
                }
                if (results.length >= total) {
                    break;
                }
            }

            // Wait 100ms before polling again
            await new Promise(resolve => setTimeout(resolve, 100));
        }

        return results;
    }
}

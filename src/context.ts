import { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { QueueNames, TASK_GROUP_FIELD_TOTAL, TASK_GROUP_FIELD_COMPLETED, TASK_GROUP_FIELD_SOURCE_AGENT, TASK_GROUP_FIELD_ABORTED, TASK_GROUP_FIELD_PROTOCOL_VERSION, TASK_GROUP_FIELD_TASK_ORDER, TASK_GROUP_PROTOCOL_V2, TASK_GROUP_ID_PREFIX, TASK_GROUP_TTL_SECONDS } from './constants';
import { createRedisCallAgentDeps, callAgent as publishCallAgent } from './dispatch/dispatch_ask_agent';
import { RoutePolicy, type RoutePolicy as RoutePolicyType } from './availability';
import type { CallAgentPublishInput, CallAgentPublishResult } from './dispatch/types';
import { EventType } from './protocol/event_type';
import { AgentState } from './protocol/agent_state';
import { AskAgentCommand, ResumeCommand } from './protocol/commands';
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
import { SpanRecorder, spanIdHex, TraceSpan } from './trace/span_recorder';
import type { JsonValue } from './protocol/results';

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

export interface CallAgentResult {
    status: string;
    messageId: string;
    parentMessageId?: string;
    targetAgentType: string;
    error?: string;
    error_code?: string;
}

export interface CallAgentParams {
    readonly targetAgentType: string;
    readonly content: unknown;
    readonly extraPayload?: Readonly<Record<string, unknown>>;
    /** @deprecated Use extraPayload. */
    readonly payload?: Readonly<Record<string, unknown>>;
    readonly waitForReply?: boolean;
    readonly metadata?: Readonly<Record<string, unknown>>;
    readonly messageId?: string;
    readonly parentMessageId?: string;
    readonly routePolicy?: RoutePolicyType;
    readonly availabilityTimeoutMs?: number;
    readonly region?: string;
    readonly priority?: number;
}

/**
 * One task in a callAgents batch. Mirrors CallAgentParams minus waitForReply
 * (batch-level), so a group member can be routed exactly like a single call.
 */
export interface CallAgentsTask {
    readonly targetAgentType: string;
    readonly content: unknown;
    readonly extraPayload?: Readonly<Record<string, unknown>>;
    /** @deprecated Use extraPayload. */
    readonly payload?: Readonly<Record<string, unknown>>;
    readonly metadata?: Readonly<Record<string, unknown>>;
    readonly messageId?: string;
    readonly routePolicy?: RoutePolicyType;
    readonly availabilityTimeoutMs?: number;
    readonly region?: string;
    readonly priority?: number;
}

interface DispatchedTask {
    message_id: string;
    target_agent_type: string;
    status?: string;
    reply_data?: unknown;
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
    /**
     * Task Group sub-tasks that never reached a worker (their target agent type
     * was unavailable at dispatch time). Each is a fully-formed ResumeCommand
     * addressed back at this caller, so Group Join counts and aggregates it
     * exactly like a real sub-agent's failure reply. GatewayWorker flushes these
     * AFTER processCommand returns — see flushPendingGroupReplies for why not
     * inline.
     */
    private readonly _pendingGroupReplies: ResumeCommand[] = [];
    private _permissionTransferred = false;
    private _isStreamFinished = false;

    public readonly executionId: string;
    public readonly spanRecorder: SpanRecorder;
    public _chunkCount: number = 0;
    private _tokenUsage: Record<string, any> = {};
    private _traceParentObservationId: string = '';

    constructor(
        public readonly sessionId: string,
        public readonly traceId: string,
        private readonly redis: Redis,
        private readonly currentAgentType: string = '',
        private readonly currentMessageId: string = '',
        public readonly currentCommand?: unknown,
        private readonly cancelSignal?: AbortSignal | CancelSignalLegacy,
        private readonly cancelReason: string = '',
        public readonly pluginRegistry?: PluginRegistry,
        executionId?: string,
        spanRecorder?: SpanRecorder,
    ) {
        this.emitter = new GatewayDataEmitter(this.redis);
        this.executionId = executionId || '';
        this.spanRecorder = spanRecorder || new SpanRecorder(redis);
    }

    get traceParentObservationId(): string {
        return this._traceParentObservationId;
    }

    set traceParentObservationId(id: string) {
        this._traceParentObservationId = id || '';
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

    isStreamFinished(): boolean {
        return this._isStreamFinished;
    }

    setStreamFinished(finished: boolean): void {
        this._isStreamFinished = finished;
    }

    recordTokenUsage(params: { promptTokens?: number; completionTokens?: number; model?: string }): void {
        const { promptTokens = 0, completionTokens = 0, model } = params;
        this._tokenUsage['prompt_tokens'] = (this._tokenUsage['prompt_tokens'] || 0) + Math.max(0, promptTokens);
        this._tokenUsage['completion_tokens'] = (this._tokenUsage['completion_tokens'] || 0) + Math.max(0, completionTokens);
        this._tokenUsage['total_tokens'] = this._tokenUsage['prompt_tokens'] + this._tokenUsage['completion_tokens'];
        if (model) {
            this._tokenUsage['model'] = model;
        }
    }

    getTokenUsage(): Record<string, any> {
        return { ...this._tokenUsage };
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

    async updateExecutionState(status: string): Promise<void> {
        const { WorkerRegistry } = await import('./registry');
        const registry = new WorkerRegistry(this.redis);
        await registry.updateExecutionStatusByMessage(this.currentMessageId, this.sessionId, status);
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
        this._chunkCount += 1;
        await this.emitter.emitChunk(this.sessionId, this.traceId, event, {
            sourceAgentType: this.currentAgentType,
            messageId: this.currentMessageId,
            eventType: eventType as EventType
        });
        if (eventType === EventType.APP_STREAM_RESPONSE) {
            this._isStreamFinished = true;
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

    async callAgent(params: CallAgentParams): Promise<CallAgentResult> {
        const { WorkerRegistry } = await import('./registry');
        const registry = new WorkerRegistry(this.redis);
        const deps = createRedisCallAgentDeps({ redis: this.redis, registry, queueNames: QueueNames });

        // Pre-generate messageId so we can compute trace span IDs before dispatch
        const messageId = params.messageId || this.generateMessageId();
        const callParentSpanId = `${messageId}:client.dispatch`;
        const traceParentSpanId = this._resolveCallTraceParentSpanId(callParentSpanId);

        const mergedMetadata: Record<string, unknown> = {
            ...(params.metadata || {}),
            trace_parent_span_id: traceParentSpanId,
            framework_parent_span_id: callParentSpanId,
        };

        const currentHeader = this.currentCommand instanceof AskAgentCommand
            ? this.currentCommand.header
            : undefined;
        const input: CallAgentPublishInput = {
            sessionId: this.sessionId,
            traceId: this.traceId,
            sourceAgentType: this.currentAgentType,
            defaultParentMessageId: this.currentMessageId,
            targetAgentType: params.targetAgentType,
            content: params.content,
            extraPayload: params.extraPayload ?? params.payload,
            waitForReply: params.waitForReply,
            userCode: currentHeader?.userCode,
            userName: currentHeader?.userName,
            taskGroupId: currentHeader?.taskGroupId,
            metadata: mergedMetadata,
            messageId,
            parentMessageId: params.parentMessageId,
            routePolicy: params.routePolicy,
            availabilityTimeoutMs: params.availabilityTimeoutMs,
            region: params.region,
            priority: params.priority,
            langfuseParentObservationId: this.traceParentObservationId || '',
        };

        if (this.pluginRegistry) {
            await this.pluginRegistry.onCallAgentStart(this, params);
        }

        const dispatchStartTs = Date.now();
        let raw: any;
        try {
            raw = await publishCallAgent(deps, input);
        } catch (error: any) {
            if (this.pluginRegistry) {
                await this.pluginRegistry.onCallAgentError(this, params, error instanceof Error ? error : new Error(String(error)));
            }
            throw error;
        }

        await this._recordAgentDispatchSpan({
            messageId: raw.messageId || messageId,
            parentMessageId: params.parentMessageId || this.currentMessageId,
            sourceAgentType: params.waitForReply !== false ? this.currentAgentType : '',
            targetAgentType: raw.targetAgentType || params.targetAgentType,
            routePolicy: params.routePolicy ?? RoutePolicy.FAIL_FAST,
            routeStatus: raw.routeStatus || raw.status,
            startTs: dispatchStartTs,
            endTs: Date.now(),
        });

        if (raw.status === AgentState.FAILED) {
            if (this.pluginRegistry) {
                await this.pluginRegistry.onCallAgentError(this, params, new Error(raw.error || 'Agent type unavailable'));
            }
            return {
                status: raw.status, messageId: raw.messageId, parentMessageId: raw.parentMessageId,
                targetAgentType: raw.targetAgentType, error: raw.error, error_code: raw.error_code,
            };
        }

        if (raw.runtimeHint === 'suspend' || params.waitForReply !== false) {
            this._isSuspended = true;
        } else if (raw.runtimeHint === 'transfer') {
            this._permissionTransferred = true;
        }

        const result: CallAgentResult = {
            status: raw.status,
            messageId: raw.messageId,
            parentMessageId: raw.parentMessageId,
            targetAgentType: raw.targetAgentType,
            error: raw.error,
            error_code: raw.error_code,
        };

        if (this.pluginRegistry) {
            await this.pluginRegistry.onCallAgentComplete(this, params, result);
        }

        return result;
    }

    private async _recordAgentDispatchSpan(params: {
        messageId: string;
        parentMessageId: string;
        sourceAgentType: string;
        targetAgentType: string;
        routePolicy?: string;
        routeStatus?: string;
        workerId?: string;
        startTs: number;
        endTs: number;
    }): Promise<void> {
        const parentSpanId = this.executionId
            ? `${this.executionId}:worker.execute`
            : `${this.currentMessageId}:worker.execute`;
        try {
            await this.spanRecorder.recordSpan({
                traceId: this.traceId,
                spanId: `${params.messageId}:client.dispatch`,
                parentSpanId,
                operation: 'client.dispatch',
                component: 'agent_context',
                startTs: params.startTs,
                endTs: params.endTs,
                status: 'COMPLETED',
                sessionId: this.sessionId,
                messageId: params.messageId,
                parentMessageId: params.parentMessageId,
                workerId: params.workerId || '',
                sourceAgentType: params.sourceAgentType,
                targetAgentType: params.targetAgentType,
                routePolicy: params.routePolicy || '',
                routeStatus: params.routeStatus || '',
            } as TraceSpan);
        } catch (err) {
            // best effort
        }
    }

    private _resolveCallTraceParentSpanId(callParentSpanId: string): string {
        return spanIdHex(callParentSpanId);
    }

    /**
     * Dispatch multiple tasks concurrently as a group (Scatter-Gather).
     */
    /**
     * Dispatch multiple tasks concurrently as a Task Group — callAgent's plural.
     *
     * Every per-call option callAgent takes is accepted per task, with the same
     * defaults, so a task that names no routing options behaves exactly like the
     * equivalent single callAgent call. The only increment is that the caller is
     * resumed once, with every task's result aggregated in dispatch order, after
     * all of them complete. On that resume `replyData` is the aggregate and
     * `content` is `''`.
     *
     * Contract: by-framework-python/docs/adr/0001-unify-call-agent-and-call-agents-behavior.md
     */
    async callAgents(params: {
        readonly tasks: ReadonlyArray<CallAgentsTask>;
        readonly waitForReply?: boolean;
        readonly messageId?: string;
        readonly parentMessageId?: string;
    }): Promise<DispatchGroupResult> {
        const { tasks, waitForReply = true, messageId, parentMessageId } = params;

        if (!tasks || tasks.length === 0) {
            throw new Error('callAgents/dispatchGroup requires at least one task');
        }
        if (messageId && tasks.length > 1) {
            throw new Error(
                `callAgents/dispatchGroup cannot share one messageId across ${tasks.length} tasks: `
                + "Task Group results are keyed by each sub-task's own messageId, so the siblings "
                + 'would overwrite each other. Pass a per-task "messageId" instead.'
            );
        }

        const taskGroupId = `${TASK_GROUP_ID_PREFIX}${uuidv4().slice(0, 8)}`;
        const wait = waitForReply ?? true;
        const groupDispatchStartTs = Date.now();
        const groupKey = QueueNames.task_group(taskGroupId);

        // Setup Redis counters if waiting for replies
        if (wait) {
            await this.redis.hset(groupKey, {
                [TASK_GROUP_FIELD_TOTAL]: tasks.length.toString(),
                [TASK_GROUP_FIELD_COMPLETED]: '0',
                [TASK_GROUP_FIELD_SOURCE_AGENT]: this.currentAgentType,
                [TASK_GROUP_FIELD_PROTOCOL_VERSION]: TASK_GROUP_PROTOCOL_V2,
            });
            await this.redis.expire(groupKey, TASK_GROUP_TTL_SECONDS);
            this._isSuspended = true;
        } else {
            this._permissionTransferred = true;
        }

        const dispatchedTasks: DispatchedTask[] = [];
        const pendingFailures: ResumeCommand[] = [];
        const { WorkerRegistry } = await import('./registry');
        const registry = new WorkerRegistry(this.redis);
        const deps = createRedisCallAgentDeps({ redis: this.redis, registry, queueNames: QueueNames });
        const currentHeader = this.currentCommand instanceof AskAgentCommand
            ? this.currentCommand.header
            : undefined;

        for (const task of tasks) {
            const currentMessageId = task.messageId || messageId || this.generateMessageId();

            // Compute trace span IDs for this sub-task
            const callParentSpanId = `${currentMessageId}:client.dispatch`;
            const traceParentSpanId = this._resolveCallTraceParentSpanId(callParentSpanId);

            const taskMetadata: Record<string, unknown> = {
                ...(task.metadata || {}),
                trace_parent_span_id: traceParentSpanId,
                framework_parent_span_id: callParentSpanId,
            };

            const input: CallAgentPublishInput = {
                sessionId: this.sessionId,
                traceId: this.traceId,
                sourceAgentType: this.currentAgentType,
                defaultParentMessageId: this.currentMessageId,
                targetAgentType: task.targetAgentType,
                content: task.content,
                extraPayload: task.extraPayload ?? task.payload,
                waitForReply: wait,
                userCode: currentHeader?.userCode,
                userName: currentHeader?.userName,
                taskGroupId,
                metadata: taskMetadata,
                messageId: currentMessageId,
                parentMessageId,
                routePolicy: task.routePolicy,
                availabilityTimeoutMs: task.availabilityTimeoutMs,
                region: task.region,
                priority: task.priority,
                langfuseParentObservationId: this.traceParentObservationId || '',
            };

            if (this.pluginRegistry) {
                await this.pluginRegistry.onCallAgentStart(this, task);
            }

            const dispatchStartTs = Date.now();
            let taskResult: CallAgentPublishResult;
            try {
                taskResult = await publishCallAgent(deps, input);
            } catch (error: any) {
                // A genuine dispatch-time failure (not an availability rejection,
                // which publishCallAgent turns into a FAILED result instead of
                // throwing). Stop fanning out and mark the group aborted so
                // already-sent siblings' replies cannot later resume this
                // now-failed caller execution. Synthetic replies queued so far are
                // dropped with the throw: the worker only flushes them once
                // processCommand returns normally.
                if (this.pluginRegistry) {
                    await this.pluginRegistry.onCallAgentError(this, task, error instanceof Error ? error : new Error(String(error)));
                }
                if (wait) {
                    await this.redis.hset(groupKey, { [TASK_GROUP_FIELD_ABORTED]: '1' });
                }
                throw error;
            }

            await this._recordAgentDispatchSpan({
                messageId: currentMessageId,
                parentMessageId: taskResult.parentMessageId || this.currentMessageId,
                sourceAgentType: wait ? this.currentAgentType : '',
                targetAgentType: taskResult.targetAgentType,
                routePolicy: task.routePolicy || RoutePolicy.FAIL_FAST,
                routeStatus: taskResult.routeStatus || '',
                startTs: dispatchStartTs,
                endTs: Date.now(),
            });

            if (this.pluginRegistry) {
                if (taskResult.status === AgentState.FAILED) {
                    await this.pluginRegistry.onCallAgentError(this, task, new Error(taskResult.error || 'agent type unavailable'));
                } else {
                    await this.pluginRegistry.onCallAgentComplete(this, task, taskResult);
                }
            }

            if (taskResult.status === AgentState.FAILED && wait) {
                // The target agent type was unavailable, so no worker will ever
                // reply for this sub-task. Rather than book-keeping the group here
                // — a second implementation of the accounting GatewayWorker's Group
                // Join owns, and the one that could push `completed` to `total`
                // with nobody left to resume the caller — synthesize the reply a
                // sub-agent WOULD have sent had it started and failed.
                pendingFailures.push(this._buildGroupFailureReply({
                    taskGroupId,
                    callerMessageId: taskResult.parentMessageId || this.currentMessageId,
                    taskMessageId: currentMessageId,
                    targetAgentType: taskResult.targetAgentType,
                    error: taskResult.error,
                    errorCode: taskResult.error_code,
                    metadata: taskMetadata,
                }));
            }

            const dispatched: DispatchedTask = {
                message_id: currentMessageId,
                // taskResult.targetAgentType reflects any fallback reroute the
                // publish pipeline performed, so this stays consistent with what
                // Group Join later reports.
                target_agent_type: taskResult.targetAgentType,
                status: taskResult.status,
            };
            if (taskResult.status === AgentState.FAILED) {
                // Same shape a real sub-agent failure arrives in, so callers read
                // dispatch-time and run-time failures the same way.
                dispatched.reply_data = { error: taskResult.error, error_code: taskResult.error_code };
            }
            dispatchedTasks.push(dispatched);
        }

        if (wait) {
            // Written after the loop so it records exactly what was dispatched.
            // Group Join aggregates in this order and uses it to name results that
            // never arrived.
            await this.redis.hset(groupKey, {
                [TASK_GROUP_FIELD_TASK_ORDER]: JSON.stringify(dispatchedTasks.map((t) => t.message_id)),
            });
            this._pendingGroupReplies.push(...pendingFailures);
        }

        // Record aggregate span for the entire group dispatch
        const groupParentSpanId = this.executionId
            ? `${this.executionId}:worker.execute`
            : `${this.currentMessageId}:worker.execute`;
        try {
            await this.spanRecorder.recordSpan({
                traceId: this.traceId,
                spanId: `${taskGroupId}:agent.dispatch_group`,
                parentSpanId: groupParentSpanId,
                operation: 'agent.dispatch_group',
                component: 'agent_context',
                startTs: groupDispatchStartTs,
                endTs: Date.now(),
                status: 'COMPLETED',
                sessionId: this.sessionId,
                executionId: this.executionId,
                messageId: this.currentMessageId,
                targetAgentType: this.currentAgentType,
                metadata: {
                    task_group_id: taskGroupId,
                    task_count: tasks.length,
                    wait_for_reply: wait,
                },
            } as TraceSpan);
        } catch (err) {
            // best effort
        }

        return {
            status: AgentState.QUEUED,
            taskGroupId,
            dispatchedTasks,
        };
    }

    /** Alias for callAgents, kept permanently for source compatibility. Not deprecated. */
    async dispatchGroup(params: {
        readonly tasks: ReadonlyArray<CallAgentsTask>;
        readonly waitForReply?: boolean;
        readonly messageId?: string;
        readonly parentMessageId?: string;
    }): Promise<DispatchGroupResult> {
        return this.callAgents(params);
    }

    /**
     * Build the reply a sub-agent WOULD have sent had it started and failed.
     *
     * The header derivation mirrors GatewayWorker.enqueueAgentReturn exactly,
     * because that shape is load-bearing in two places:
     *
     * - header.messageId must be the CALLER's own message id: WorkerRunner
     *   reattaches a ResumeCommand to the suspended execution via
     *   getExecutionByMessageId(header.messageId), so any other value would
     *   orphan the caller's execution instead of resuming it.
     * - header.parentMessageId must be this sub-task's dispatch message id: it
     *   is what Group Join keys the result hash by, and the only per-sibling-
     *   unique id available on a reply.
     *
     * replyData carries the failure detail because that is how a real failure
     * arrives (GatewayWorker returns status=FAILED, replyData={error}); putting
     * it anywhere else would make dispatch-time and run-time failures read
     * differently.
     */
    private _buildGroupFailureReply(params: {
        readonly taskGroupId: string;
        readonly callerMessageId: string;
        readonly taskMessageId: string;
        readonly targetAgentType: string;
        readonly error?: string;
        readonly errorCode?: string;
        readonly metadata?: Readonly<Record<string, unknown>>;
    }): ResumeCommand {
        const currentHeader = this.currentCommand instanceof AskAgentCommand
            ? this.currentCommand.header
            : undefined;
        return new ResumeCommand(
            new MessageHeader(params.callerMessageId, this.sessionId, this.traceId, {
                sourceAgentType: params.targetAgentType,
                targetAgentType: this.currentAgentType,
                parentMessageId: params.taskMessageId,
                taskGroupId: params.taskGroupId,
                userCode: currentHeader?.userCode,
                userName: currentHeader?.userName,
                metadata: { ...(params.metadata || {}) } as Record<string, JsonValue>,
            }),
            '',
            AgentState.FAILED,
            { error: params.error ?? null, error_code: params.errorCode ?? 'AGENT_TYPE_UNAVAILABLE' }
        );
    }

    /**
     * Collect results from all tasks in a group.
     */
    async collectGroupResults(taskGroupId: string, timeout: number = 30): Promise<GroupResult[]> {
        if (!taskGroupId) {
            return [];
        }

        const resultsKey = QueueNames.task_group_results(taskGroupId);
        const groupKey = QueueNames.task_group(taskGroupId);

        const totalStr = await this.redis.hget(groupKey, TASK_GROUP_FIELD_TOTAL);
        const total = totalStr ? parseInt(totalStr, 10) : Infinity;

        // Ordering mirrors what Group Join hands the caller on resume: dispatch
        // order, not the Redis hash's unspecified iteration order. A group from a
        // pre-v2 dispatcher has no task_order and keeps hash order.
        const rawOrder = await this.redis.hget(groupKey, TASK_GROUP_FIELD_TASK_ORDER);
        let order: string[] = [];
        try {
            order = rawOrder ? JSON.parse(rawOrder) : [];
        } catch {
            order = [];
        }

        let results: GroupResult[] = [];
        const startTime = Date.now();

        while (results.length < total) {
            const elapsed = (Date.now() - startTime) / 1000;
            if (elapsed >= timeout) {
                break;
            }

            const rawResults = await this.redis.hgetall(resultsKey);
            if (rawResults) {
                // Rebuilt, not appended to: this loop polls the same hash
                // repeatedly, so pushing onto the previous pass's array would
                // duplicate every result already seen.
                const orderedIds = [
                    ...order.filter((m) => Object.prototype.hasOwnProperty.call(rawResults, m)),
                    ...Object.keys(rawResults).filter((m) => !order.includes(m)),
                ];
                results = [];
                for (const msgId of orderedIds) {
                    try {
                        const parsed = JSON.parse(rawResults[msgId] as string);
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

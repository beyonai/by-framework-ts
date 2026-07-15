import { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { QueueNames, TASK_GROUP_FIELD_TOTAL, TASK_GROUP_FIELD_COMPLETED, TASK_GROUP_TTL_SECONDS } from './constants';
import { createRedisCallAgentDeps, callAgent as publishCallAgent } from './dispatch/dispatch_ask_agent';
import { RoutePolicy, type RoutePolicy as RoutePolicyType } from './availability';
import type { CallAgentPublishInput } from './dispatch/types';
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
import { SpanRecorder, spanIdHex, TraceSpan } from './trace/span_recorder';

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

    async callAgent(params: {
        readonly targetAgentType: string;
        readonly content: string | Array<Record<string, unknown>>;
        readonly payload?: Readonly<Record<string, unknown>>;
        readonly waitForReply?: boolean;
        readonly userCode?: string;
        readonly userName?: string;
        readonly taskGroupId?: string;
        readonly metadata?: Readonly<Record<string, unknown>>;
        readonly messageId?: string;
        readonly parentMessageId?: string;
        readonly probeAgentType?: boolean;
        readonly routePolicy?: RoutePolicyType;
        readonly availabilityTimeoutMs?: number;
        readonly region?: string;
        readonly priority?: number;
        /** Explicitly set the Langfuse parent observation ID for this sub-call.
         *  Overrides context.traceParentObservationId when provided. */
        readonly langfuseParentObservationId?: string;
    }): Promise<CallAgentResult> {
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

        const input: CallAgentPublishInput = {
            sessionId: this.sessionId,
            traceId: this.traceId,
            sourceAgentType: this.currentAgentType,
            defaultParentMessageId: this.currentMessageId,
            targetAgentType: params.targetAgentType,
            content: params.content,
            payload: params.payload,
            waitForReply: params.waitForReply,
            userCode: params.userCode,
            userName: params.userName,
            taskGroupId: params.taskGroupId,
            metadata: mergedMetadata,
            messageId,
            parentMessageId: params.parentMessageId,
            probeAgentType: params.probeAgentType,
            routePolicy: params.routePolicy ?? (params.probeAgentType === undefined ? undefined : (params.probeAgentType ? RoutePolicy.FAIL_FAST : RoutePolicy.SEND_ANYWAY)),
            availabilityTimeoutMs: params.availabilityTimeoutMs,
            region: params.region,
            priority: params.priority,
            langfuseParentObservationId: params.langfuseParentObservationId ?? this.traceParentObservationId ?? '',
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
            routePolicy: params.routePolicy ?? (params.probeAgentType === false ? RoutePolicy.SEND_ANYWAY : RoutePolicy.FAIL_FAST),
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
        const groupDispatchStartTs = Date.now();

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
        const { WorkerRegistry } = await import('./registry');
        const dispatchRegistry = new WorkerRegistry(this.redis);

        for (const task of tasks) {
            const currentMessageId = this.generateMessageId();
            const currentParentMessageId = parentMessageId || this.currentMessageId;

            // Compute trace span IDs for this sub-task
            const callParentSpanId = `${currentMessageId}:client.dispatch`;
            const traceParentSpanId = this._resolveCallTraceParentSpanId(callParentSpanId);

            const taskMetadata: Record<string, unknown> = {
                ...(task.metadata || {}),
                trace_parent_span_id: traceParentSpanId,
                framework_parent_span_id: callParentSpanId,
            };

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
                    metadata: taskMetadata,
                    traceParentSpanId,
                    langfuseParentObservationId: this.traceParentObservationId || '',
                }),
                task.content,
                wait,
                Object.fromEntries(Object.entries(mergedPayload).filter(([key]) => key !== 'wait_for_reply'))
            );

            if (this.pluginRegistry) {
                await this.pluginRegistry.onCallAgentStart(this, command);
            }

            // Initialize execution record for each dispatched task
            await dispatchRegistry.initializeExecution({
                execution_id: `exec-${uuidv4().slice(0, 8)}`,
                message_id: currentMessageId,
                parent_message_id: currentParentMessageId,
                session_id: this.sessionId,
                trace_id: this.traceId,
                source_agent_type: wait ? this.currentAgentType : '',
                stream_name: QueueNames.ctrl_stream(task.targetAgentType),
                worker_id: '',
                target_agent_type: task.targetAgentType,
                status: 'QUEUED',
                cancel_requested: false,
                cancel_reason: '',
            });

            const dispatchStartTs = Date.now();
            try {
                await this.redis.xadd(
                    QueueNames.ctrl_stream(task.targetAgentType),
                    '*',
                    'data',
                    JSON.stringify(command.toDict())
                );
            } catch (error: any) {
                if (this.pluginRegistry) {
                    await this.pluginRegistry.onCallAgentError(this, command, error instanceof Error ? error : new Error(String(error)));
                }
                throw error;
            }

            await this._recordAgentDispatchSpan({
                messageId: currentMessageId,
                parentMessageId: currentParentMessageId,
                sourceAgentType: wait ? this.currentAgentType : '',
                targetAgentType: task.targetAgentType,
                routePolicy: 'SEND_ANYWAY',
                routeStatus: 'GROUP_DISPATCH',
                startTs: dispatchStartTs,
                endTs: Date.now(),
            });

            const taskResult = {
                status: AgentState.QUEUED,
                messageId: currentMessageId,
                parentMessageId: currentParentMessageId,
                targetAgentType: task.targetAgentType,
            };
            if (this.pluginRegistry) {
                await this.pluginRegistry.onCallAgentComplete(this, command, taskResult);
            }

            dispatchedTasks.push({
                message_id: currentMessageId,
                target_agent_type: task.targetAgentType,
            });
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
            status: 'GROUP_QUEUED',
            taskGroupId,
            dispatchedTasks,
        };
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

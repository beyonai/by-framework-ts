import { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { getRedis } from './redis_client';
import { WorkerRegistry } from './registry';
import { CancelSessionResponse, CancelTaskResponse, ExecutionStatus, SendMessageResponse } from './protocol/responses';
import { ActionType } from './protocol/action_type';
import { AskAgentCommand, BaseCommand, CancelTaskCommand, ResumeCommand } from './protocol/commands';
import { MessageHeader } from './protocol/message_header';
import { QueueNames } from './constants';
import { initializeQueuedExecution } from './dispatch/execution_init';
import { AvailabilityRouter, AvailabilityStatus, RoutePolicy, type RoutePolicy as RoutePolicyType } from './availability';
import { publishAskAgentCommand } from './dispatch/publish_ask_agent';
import { BaiYingMessage } from './protocol/message';
import { GatewayInterceptor } from './interceptors';
import { SpanRecorder, spanIdHex } from './trace/span_recorder';
import type { TraceSpan } from './trace/span_recorder';

// === Types ===
type LangfuseDispatchObservation = {
    readonly id?: string;
    end?: (options?: { output?: unknown }) => void;
    update?: (options: { level?: string; statusMessage?: string; output?: unknown }) => void;
};

type LangfuseDispatchFn = (params: {
    readonly traceId: string;
    readonly messageId: string;
    readonly targetAgentType: string;
    readonly sessionId: string;
    readonly userCode: string;
    readonly userName: string;
    readonly content: unknown;
    readonly metadata: Record<string, unknown>;
}) => LangfuseDispatchObservation | null;

interface RouteResolution {
    streamName: string;
    targetWorkerId: string;
}

export interface SendMessageParams {
    readonly targetAgentType: string;
    readonly sessionId: string;
    readonly content: string | BaiYingMessage | ReadonlyArray<BaiYingMessage>;
    readonly sourceAgentType?: string;
    readonly traceId?: string;
    readonly userCode?: string;
    readonly userName?: string;
    readonly actionType?: ActionType;
    readonly extraPayload?: Readonly<Record<string, unknown>>;
    readonly parentMessageId?: string;
    readonly messageId?: string;
    readonly metadata?: Readonly<Record<string, unknown>>;
    readonly targetWorkerId?: string;
    readonly requireOnlineWorker?: boolean;
    readonly routePolicy?: RoutePolicyType;
    readonly availabilityTimeoutMs?: number;
    readonly region?: string;
    readonly priority?: number;
}

export class GatewayClient {
    private readonly redis: Redis;
    private readonly registry: WorkerRegistry;
    private readonly interceptors: GatewayInterceptor[];
    readonly spanRecorder: SpanRecorder;
    private readonly langfuseDispatchFn: LangfuseDispatchFn | null;

    public constructor(
        registry?: WorkerRegistry,
        redisClient?: Redis,
        interceptors?: GatewayInterceptor[],
        spanRecorder?: SpanRecorder,
        langfuseDispatchFn?: LangfuseDispatchFn | null,
    ) {
        this.redis = redisClient ?? getRedis();
        this.registry = registry ?? new WorkerRegistry(this.redis);
        this.interceptors = interceptors ?? [];
        this.spanRecorder = spanRecorder ?? new SpanRecorder(this.redis);
        this.langfuseDispatchFn = langfuseDispatchFn ?? this.resolveLangfuseDispatchFn();
    }

    addInterceptor(interceptor: GatewayInterceptor): void {
        this.interceptors.push(interceptor);
    }

    private runInterceptors(params: Record<string, any>): Record<string, any> {
        let result = params;
        for (const interceptor of this.interceptors) {
            if (interceptor.beforeSend) {
                result = interceptor.beforeSend(result);
            }
        }
        return result;
    }

    private resolveLangfuseDispatchFn(): LangfuseDispatchFn | null {
        const publicKey = process.env.LANGFUSE_PUBLIC_KEY || '';
        const secretKey = process.env.LANGFUSE_SECRET_KEY || '';
        if (!publicKey || !secretKey) {
            return null;
        }

        try {
            // Optional dependency; GatewayClient must keep working without it.
            // eslint-disable-next-line @typescript-eslint/no-require-imports
            const mod = require('langfuse') as Record<string, unknown>;
            const Langfuse = (mod.default ?? mod.Langfuse) as
                | (new (options: Record<string, unknown>) => {
                    trace?: (options: Record<string, unknown>) => unknown;
                    span?: (options: Record<string, unknown>) => LangfuseDispatchObservation;
                })
                | undefined;
            if (!Langfuse) {
                return null;
            }
            const client = new Langfuse({
                publicKey,
                secretKey,
                baseUrl: process.env.LANGFUSE_HOST || process.env.LANGFUSE_BASE_URL || 'https://cloud.langfuse.com',
                enabled: true,
            });
            return params => {
                const safeInput = this.safeSerialize(params.content);
                const metadata = {
                    ...params.metadata,
                    targetAgentType: params.targetAgentType,
                    sessionId: params.sessionId,
                    messageId: params.messageId,
                };
                client.trace?.({
                    id: params.traceId,
                    name: 'client.dispatch',
                    sessionId: params.sessionId,
                    userId: params.userCode || params.userName || undefined,
                    input: safeInput,
                    metadata,
                    tags: ['by-framework'],
                });
                return client.span?.({
                    id: spanIdHex(`${params.messageId}:client.dispatch`),
                    traceId: params.traceId,
                    name: 'client.dispatch',
                    startTime: new Date(),
                    input: safeInput,
                    metadata,
                }) ?? null;
            };
        } catch {
            return null;
        }
    }

    private safeSerialize(value: unknown): unknown {
        if (value === null || value === undefined) return null;
        if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return value;
        try {
            return JSON.parse(JSON.stringify(value));
        } catch {
            return String(value);
        }
    }

    private startLangfuseClientDispatchObservation(params: {
        readonly traceId: string;
        readonly messageId: string;
        readonly targetAgentType: string;
        readonly sessionId: string;
        readonly userCode: string;
        readonly userName: string;
        readonly content: unknown;
        readonly metadata: Record<string, unknown>;
    }): LangfuseDispatchObservation | null {
        if (!this.langfuseDispatchFn) {
            return null;
        }
        try {
            return this.langfuseDispatchFn(params);
        } catch (err) {
            console.warn('[GatewayClient] Langfuse client.dispatch observation skipped:', err);
            return null;
        }
    }

    private endLangfuseClientDispatchObservation(
        observation: LangfuseDispatchObservation | null,
        output: unknown,
        error = ''
    ): void {
        if (!observation) {
            return;
        }
        try {
            if (error && observation.update) {
                observation.update({ level: 'ERROR', statusMessage: error });
            }
            if (observation.end) {
                observation.end({ output });
                return;
            }
        } catch {
            // Try the older update(output)+end() shape below.
        }
        try {
            observation.update?.({ output });
            observation.end?.();
        } catch {
            // best effort
        }
    }

    /**
     * Resolve agent-type-mode routing.
     * Agent type sends always publish to the agent-type stream. When requireOnlineWorker is true,
     * we verify that at least one online worker exists.
     */
    private async resolveAgentTypeRoute(targetAgentType: string, requireOnlineWorker: boolean): Promise<RouteResolution> {
        if (requireOnlineWorker) {
            const [hasOnline] = await this.registry.hasOnlineAgentType(targetAgentType);
            if (hasOnline) {
                return { streamName: QueueNames.ctrl_stream(targetAgentType), targetWorkerId: '' };
            }
            throw new Error(`No online worker found for agent_type '${targetAgentType}'`);
        }
        return { streamName: QueueNames.ctrl_stream(targetAgentType), targetWorkerId: '' };
    }

    /**
     * Resolve direct-worker routing for debug or worker-specific control.
     */
    private async resolveDirectWorkerRoute(targetWorkerId: string, requireOnlineWorker: boolean): Promise<RouteResolution> {
        if (requireOnlineWorker) {
            const isOnline = await this.registry.isWorkerOnline(targetWorkerId);
            if (!isOnline) {
                throw new Error(`Target worker '${targetWorkerId}' is not online or not registered`);
            }
        }
        return {
            streamName: QueueNames.worker_ctrl_stream(targetWorkerId),
            targetWorkerId: targetWorkerId,
        };
    }

    /**
     * Build a gateway command from parameters.
     */
    private buildGatewayCommand(
        actionType: ActionType | string,
        header: MessageHeader,
        content: string | unknown[],
        extraPayload: Record<string, unknown>
    ): AskAgentCommand | ResumeCommand {
        if (actionType === ActionType.RESUME) {
            const resumePayload = { ...extraPayload };
            const status = String(resumePayload.status || '');
            const replyData = resumePayload.reply_data;
            delete resumePayload.status;
            delete resumePayload.reply_data;
            return new ResumeCommand(
                header,
                content as string | unknown[],
                status,
                replyData,
                resumePayload
            );
        }
        return new AskAgentCommand(
            header,
            content as string | unknown[],
            Boolean((extraPayload as { wait_for_reply?: boolean }).wait_for_reply),
            Object.fromEntries(Object.entries(extraPayload).filter(([key]) => key !== 'wait_for_reply'))
        );
    }

    async sendCommand(command: BaseCommand, streamName?: string): Promise<SendMessageResponse> {
        const resolvedStreamName = streamName || QueueNames.ctrl_stream(command.header.targetAgentType);

        if (!streamName && command.header.targetAgentType) {
            if (command instanceof AskAgentCommand) {
                await publishAskAgentCommand({
                    redis: this.redis,
                    registry: this.registry,
                    command,
                    streamName: resolvedStreamName,
                    executionSourceAgentFallback: 'client',
                });
            } else {
                await initializeQueuedExecution(this.registry, {
                    execution_id: `exec-${uuidv4().slice(0, 8)}`,
                    message_id: command.header.messageId,
                    session_id: command.header.sessionId,
                    trace_id: command.header.traceId,
                    parent_message_id: command.header.parentMessageId || '',
                    source_agent_type: command.header.sourceAgentType || 'client',
                    target_agent_type: command.header.targetAgentType,
                    stream_name: resolvedStreamName,
                    worker_id: '',
                    status: 'QUEUED',
                    cancel_requested: false,
                    cancel_reason: '',
                }).catch(() => undefined);
                await this.redis.xadd(resolvedStreamName, '*', 'data', JSON.stringify(command.toDict()));
            }
        } else {
            await this.redis.xadd(resolvedStreamName, '*', 'data', JSON.stringify(command.toDict()));
        }

        return {
            success: true,
            message_id: command.header.messageId,
            trace_id: command.header.traceId,
            target_worker_id: '',
            timestamp: Date.now(),
            status: ExecutionStatus.QUEUED,
        };
    }

    async cancelTask(params: {
        messageId: string;
        sessionId: string;
        reason?: string;
        targetAgentType?: string;
        requestedBy?: string;
        cancelMode?: 'graceful' | 'force';
    }): Promise<CancelTaskResponse> {
        const TERMINAL_STATES = new Set(['COMPLETED', 'FAILED', 'CANCELLED']);

        // Fetch all session executions for BFS-based cascading cancellation
        const allExecutions = await this.registry.getAllSessionExecutions(params.sessionId);

        // Find the target execution by message_id
        const targetExecution = allExecutions.find(e => e.message_id === params.messageId);
        if (!targetExecution) {
            return {
                success: false,
                message_id: params.messageId,
                execution_id: '',
                worker_id: '',
                status: ExecutionStatus.NOT_FOUND,
                timestamp: Date.now(),
                error: `execution not found for message_id=${params.messageId}`,
            };
        }

        if (targetExecution.session_id !== params.sessionId) {
            return {
                success: false,
                message_id: params.messageId,
                execution_id: targetExecution.execution_id || '',
                worker_id: targetExecution.worker_id || '',
                status: ExecutionStatus.NOT_FOUND,
                timestamp: Date.now(),
                error: `session mismatch for message_id=${params.messageId}`,
            };
        }

        // Build parent_message_id -> children map for BFS
        const childrenMap = new Map<string, Record<string, any>[]>();
        for (const exec of allExecutions) {
            const parentMsgId = exec.parent_message_id;
            if (parentMsgId) {
                if (!childrenMap.has(parentMsgId)) {
                    childrenMap.set(parentMsgId, []);
                }
                childrenMap.get(parentMsgId)!.push(exec);
            }
        }

        // BFS from target to collect all descendant executions
        const toCancel: Record<string, any>[] = [];
        const terminalAncestors: Record<string, any>[] = [];
        const queue = [targetExecution];

        while (queue.length > 0) {
            const current = queue.shift()!;
            const status = String(current.status || '');

            if (TERMINAL_STATES.has(status)) {
                terminalAncestors.push(current);
            } else {
                toCancel.push(current);
            }

            // Enqueue children by message_id
            const children = childrenMap.get(current.message_id) || [];
            for (const child of children) {
                queue.push(child);
            }
        }

        // Mark terminal ancestors with cancel_requested (don't change status)
        const reason = params.reason || '';
        for (const exec of terminalAncestors) {
            await this.registry.markCancelRequested(
                String(exec.execution_id),
                params.sessionId,
                reason
            );
        }

        if (toCancel.length === 0) {
            return {
                success: false,
                message_id: params.messageId,
                execution_id: targetExecution.execution_id || '',
                worker_id: targetExecution.worker_id || '',
                status: ExecutionStatus.ALREADY_FINISHED,
                timestamp: Date.now(),
                error: `all executions already in terminal state`,
            };
        }

        // Cancel all non-terminal descendants
        let cancelledCount = 0;
        for (const exec of toCancel) {
            const executionId = String(exec.execution_id);
            const workerId = String(exec.worker_id || '');

            await this.registry.markExecutionCancelling(executionId, params.sessionId, reason);

            const cancelCommand = new CancelTaskCommand(
                new MessageHeader(`msg-cancel-${uuidv4().slice(0, 8)}`, params.sessionId, uuidv4().replace(/-/g, ''), {
                    targetAgentType: params.targetAgentType || exec.target_agent_type || '',
                    parentMessageId: exec.message_id,
                }),
                exec.message_id,
                executionId,
                workerId,
                reason,
                params.requestedBy || 'client',
                params.cancelMode || 'graceful'
            );

            if (workerId) {
                await this.sendCommand(cancelCommand, QueueNames.worker_ctrl_stream(workerId));
            }
            cancelledCount++;
        }

        return {
            success: true,
            message_id: params.messageId,
            execution_id: String(targetExecution.execution_id),
            worker_id: String(targetExecution.worker_id || ''),
            status: ExecutionStatus.CANCEL_REQUESTED,
            timestamp: Date.now(),
            cancelled_count: cancelledCount,
        };
    }

    async cancelSession(params: {
        sessionId: string;
        reason?: string;
        targetAgentType?: string;
        requestedBy?: string;
        cancelMode?: 'graceful' | 'force';
    }): Promise<CancelSessionResponse> {
        const TERMINAL_STATES = new Set(['COMPLETED', 'FAILED', 'CANCELLED']);
        const reason = params.reason || '';

        const allExecutions = await this.registry.getAllSessionExecutions(params.sessionId);

        if (allExecutions.length === 0) {
            return {
                success: false,
                session_id: params.sessionId,
                status: ExecutionStatus.NOT_FOUND,
                timestamp: Date.now(),
                cancelled_count: 0,
                already_finished_count: 0,
            };
        }

        const toCancel = allExecutions.filter(exec => !TERMINAL_STATES.has(String(exec.status || '')));
        const terminalExecutions = allExecutions.filter(exec => TERMINAL_STATES.has(String(exec.status || '')));

        for (const exec of terminalExecutions) {
            await this.registry.markCancelRequested(String(exec.execution_id), params.sessionId, reason);
        }

        if (toCancel.length === 0) {
            return {
                success: false,
                session_id: params.sessionId,
                status: ExecutionStatus.ALREADY_FINISHED,
                timestamp: Date.now(),
                cancelled_count: 0,
                already_finished_count: terminalExecutions.length,
            };
        }

        for (const exec of toCancel) {
            const executionId = String(exec.execution_id);
            const workerId = String(exec.worker_id || '');

            await this.registry.markExecutionCancelling(executionId, params.sessionId, reason);

            if (workerId) {
                const cancelCommand = new CancelTaskCommand(
                    new MessageHeader(`msg-cancel-${uuidv4().slice(0, 8)}`, params.sessionId, uuidv4().replace(/-/g, ''), {
                        targetAgentType: params.targetAgentType || exec.target_agent_type || '',
                        parentMessageId: exec.message_id,
                    }),
                    exec.message_id,
                    executionId,
                    workerId,
                    reason,
                    params.requestedBy || 'client',
                    params.cancelMode || 'graceful'
                );
                await this.sendCommand(cancelCommand, QueueNames.worker_ctrl_stream(workerId));
            }
        }

        return {
            success: true,
            session_id: params.sessionId,
            status: ExecutionStatus.CANCEL_REQUESTED,
            timestamp: Date.now(),
            cancelled_count: toCancel.length,
            already_finished_count: terminalExecutions.length,
        };
    }

    async sendMessage(params: SendMessageParams): Promise<SendMessageResponse> {
        const requireOnline = params.requireOnlineWorker ?? true;

        // Run interceptors before sending
        let requestParams: Record<string, any> = {
            targetAgentType: params.targetAgentType,
            sessionId: params.sessionId,
            userCode: params.userCode || '',
            userName: params.userName || '',
            content: params.content,
            actionType: params.actionType || ActionType.ASK_AGENT,
            parentMessageId: params.parentMessageId || '',
            extraPayload: params.extraPayload || {},
            metadata: params.metadata || {},
        };
        requestParams = this.runInterceptors(requestParams);

        const messageId = params.messageId || `msg-${uuidv4().slice(0, 8)}`;
        const traceId = params.traceId || uuidv4().replace(/-/g, '');
        const metadata: Record<string, unknown> = { ...(requestParams.metadata || {}) };
        let traceParentSpanId = String(
            metadata.trace_parent_span_id || metadata.traceParentSpanId || ''
        );
        let langfuseParentObservationId = String(
            metadata.langfuse_parent_observation_id || metadata.langfuseParentObservationId || ''
        );
        delete metadata.trace_parent_span_id;
        delete metadata.traceParentSpanId;
        delete metadata.langfuse_parent_observation_id;
        delete metadata.langfuseParentObservationId;

        if (!traceParentSpanId) {
            traceParentSpanId = spanIdHex(`${messageId}:client.dispatch`);
        }

        let langfuseClientDispatch: LangfuseDispatchObservation | null = null;
        if (!langfuseParentObservationId) {
            langfuseClientDispatch = this.startLangfuseClientDispatchObservation({
                traceId,
                messageId,
                targetAgentType: requestParams.targetAgentType,
                sessionId: requestParams.sessionId,
                userCode: requestParams.userCode,
                userName: requestParams.userName,
                content: requestParams.content,
                metadata,
            });
            langfuseParentObservationId = langfuseClientDispatch?.id || traceParentSpanId;
        }

        // Resolve route and optionally probe agent type/liveness
        let route: RouteResolution;
        try {
            if (params.targetWorkerId) {
                route = await this.resolveDirectWorkerRoute(params.targetWorkerId, requireOnline);
            } else {
                route = { streamName: QueueNames.ctrl_stream(params.targetAgentType), targetWorkerId: '' };
            }
        } catch (err) {
            const error = err instanceof Error ? err.message : String(err);
            this.endLangfuseClientDispatchObservation(
                langfuseClientDispatch,
                { success: false, error },
                error
            );
            if (error.includes('not online')) {
                return {
                    success: false,
                    status: ExecutionStatus.FAILED,
                    message_id: '',
                    trace_id: '',
                    target_worker_id: params.targetWorkerId || '',
                    timestamp: Date.now(),
                    error: error,
                    error_code: ExecutionStatus.ERR_WORKER_NOT_ONLINE,
                };
            }
            return {
                success: false,
                status: ExecutionStatus.FAILED,
                message_id: '',
                trace_id: '',
                target_worker_id: '',
                timestamp: Date.now(),
                error: error,
                error_code: ExecutionStatus.ERR_AGENT_TYPE_NOT_FOUND,
            };
        }

        // 序列化 content
        let formattedContent: string | unknown[];
        if (typeof requestParams.content === 'string') {
            formattedContent = requestParams.content;
        } else {
            const msgs = Array.isArray(requestParams.content) ? requestParams.content : [requestParams.content];
            formattedContent = msgs.map((m): unknown => {
                if (!m || typeof m !== 'object') {
                    return m;
                }
                const role = (m as BaiYingMessage).role;
                const innerContent = (m as BaiYingMessage).content;
                if (innerContent === undefined) {
                    return m;
                }
                const msgObj = {
                    role: role,
                    content: typeof innerContent === 'string' ? innerContent : {
                        text: (innerContent as { text?: string }).text || '',
                        files: (innerContent as { files?: unknown[] }).files || [],
                        resources: (innerContent as { resources?: unknown[] }).resources || []
                    }
                };
                return msgObj;
            });
        }

        const header = new MessageHeader(messageId, requestParams.sessionId, traceId, {
            sourceAgentType: params.sourceAgentType || '',
            targetAgentType: requestParams.targetAgentType,
            parentMessageId: requestParams.parentMessageId || '',
            userCode: requestParams.userCode || '',
            userName: requestParams.userName || '',
            metadata,
            traceParentSpanId,
            langfuseParentObservationId,
        });

        const payload = requestParams.extraPayload ?? {};
        const mergedPayload: Record<string, unknown> = {
            ...payload,
            attachments: (payload as { attachments?: unknown[] })?.attachments || []
        };

        let command = this.buildGatewayCommand(
            requestParams.actionType || ActionType.ASK_AGENT,
            header,
            formattedContent,
            mergedPayload
        );

        const routePolicy = params.routePolicy ?? (requireOnline ? RoutePolicy.FAIL_FAST : RoutePolicy.SEND_ANYWAY);
        const executionId = `exec-${uuidv4().slice(0, 8)}`;
        let routeStatus = params.targetWorkerId ? 'DIRECT_WORKER' : AvailabilityStatus.DELIVER_NOW;
        if (!params.targetWorkerId) {
            const availability = await new AvailabilityRouter(this.redis, this.registry).prepareDelivery({
                executionId, messageId, sessionId: requestParams.sessionId, traceId,
                source: params.sourceAgentType || 'client', targetAgentType: requestParams.targetAgentType,
                userCode: requestParams.userCode, region: params.region, priority: params.priority,
                policy: routePolicy, timeoutMs: params.availabilityTimeoutMs,
                commandPayload: command.toDict() as Record<string, unknown>, metadata,
            });
            routeStatus = availability.status;
            if (availability.status === AvailabilityStatus.REJECT) {
                const error = availability.error || 'Agent type unavailable';
                await initializeQueuedExecution(this.registry, {
                    execution_id: executionId, message_id: messageId, session_id: requestParams.sessionId,
                    trace_id: traceId, parent_message_id: requestParams.parentMessageId || '',
                    source_agent_type: params.sourceAgentType || 'client', target_agent_type: requestParams.targetAgentType,
                    stream_name: '', worker_id: '', status: 'FAILED', route_policy: routePolicy,
                    route_status: availability.status, availability_error: error,
                    availability_error_code: availability.errorCode || ExecutionStatus.ERR_AGENT_TYPE_UNAVAILABLE,
                }).catch(() => undefined);
                this.endLangfuseClientDispatchObservation(langfuseClientDispatch, { success: false, error }, error);
                return {
                    success: false, status: ExecutionStatus.FAILED, message_id: '', trace_id: '',
                    target_worker_id: '', timestamp: Date.now(), error,
                    error_code: availability.errorCode || ExecutionStatus.ERR_AGENT_TYPE_UNAVAILABLE,
                };
            }
            route.streamName = availability.streamName || route.streamName;
            if (availability.selectedAgentType && command instanceof AskAgentCommand) {
                const old = command.header;
                command = new AskAgentCommand(new MessageHeader(old.messageId, old.sessionId, old.traceId, {
                    sourceAgentType: old.sourceAgentType, targetAgentType: availability.selectedAgentType,
                    parentMessageId: old.parentMessageId, taskGroupId: old.taskGroupId,
                    userCode: old.userCode, userName: old.userName, metadata: old.metadata,
                    traceParentSpanId: old.traceParentSpanId,
                    langfuseParentObservationId: old.langfuseParentObservationId,
                }), command.content, command.waitForReply, command.extraPayload);
                requestParams.targetAgentType = availability.selectedAgentType;
            }
        }

        const dispatchStartedAt = Date.now();
        if (routeStatus === AvailabilityStatus.QUEUE_PENDING) {
            await initializeQueuedExecution(this.registry, {
                execution_id: executionId,
                message_id: messageId,
                session_id: requestParams.sessionId,
                trace_id: traceId,
                parent_message_id: requestParams.parentMessageId || '',
                source_agent_type: params.sourceAgentType || 'client',
                target_agent_type: requestParams.targetAgentType,
                stream_name: route.streamName,
                worker_id: '',
                status: 'QUEUED',
                cancel_requested: false,
                cancel_reason: '',
                route_policy: routePolicy,
                route_status: routeStatus,
            }).catch(() => undefined);
        } else if (command instanceof AskAgentCommand) {
            await initializeQueuedExecution(this.registry, {
                execution_id: executionId, message_id: messageId, session_id: requestParams.sessionId,
                trace_id: traceId, parent_message_id: requestParams.parentMessageId || '',
                source_agent_type: params.sourceAgentType || 'client', target_agent_type: requestParams.targetAgentType,
                stream_name: route.streamName, worker_id: '', status: 'QUEUED', cancel_requested: false,
                cancel_reason: '', route_policy: routePolicy, route_status: routeStatus,
            }).catch(() => undefined);
            await this.redis.xadd(route.streamName, '*', 'data', JSON.stringify(command.toDict()));
        } else {
            await initializeQueuedExecution(this.registry, {
                execution_id: executionId, message_id: messageId, session_id: requestParams.sessionId,
                trace_id: traceId, parent_message_id: requestParams.parentMessageId || '',
                source_agent_type: params.sourceAgentType || 'client', target_agent_type: requestParams.targetAgentType,
                stream_name: route.streamName, worker_id: '', status: 'QUEUED', cancel_requested: false,
                cancel_reason: '', route_policy: routePolicy, route_status: routeStatus,
            }).catch(() => undefined);
            await this.redis.xadd(route.streamName, '*', 'data', JSON.stringify(command.toDict()));
        }

        // Record the root client.dispatch span — this is the trace root.
        await this._recordClientDispatchSpan({
            traceId,
            messageId,
            sessionId: requestParams.sessionId,
            parentMessageId: requestParams.parentMessageId || '',
            targetAgentType: requestParams.targetAgentType,
            targetWorkerId: route.targetWorkerId,
            routePolicy,
            routeStatus,
            startTs: dispatchStartedAt,
            endTs: Date.now(),
        });

        const response = {
            success: true,
            message_id: messageId,
            trace_id: traceId,
            target_worker_id: route.targetWorkerId,
            timestamp: Date.now(),
            status: ExecutionStatus.QUEUED,
        };
        this.endLangfuseClientDispatchObservation(langfuseClientDispatch, {
            success: true,
            message_id: messageId,
            trace_id: traceId,
            target_worker_id: route.targetWorkerId,
            status: response.status,
        });
        return response;
    }

    private async _recordClientDispatchSpan(params: {
        traceId: string;
        messageId: string;
        sessionId: string;
        parentMessageId: string;
        targetAgentType: string;
        targetWorkerId: string;
        routePolicy: string;
        routeStatus: string;
        startTs: number;
        endTs: number;
    }): Promise<void> {
        try {
            const span: TraceSpan = {
                traceId:        params.traceId,
                spanId:         `${params.messageId}:client.dispatch`,
                parentSpanId:   '',
                operation:      'client.dispatch',
                component:      'client',
                startTs:        params.startTs,
                endTs:          params.endTs,
                status:         'COMPLETED',
                sessionId:      params.sessionId,
                messageId:      params.messageId,
                parentMessageId: params.parentMessageId,
                workerId:       params.targetWorkerId,
                sourceAgentType: 'client',
                targetAgentType: params.targetAgentType,
                routePolicy:    params.routePolicy,
                routeStatus:    params.routeStatus,
            };
            await this.spanRecorder.recordSpan(span);
        } catch (err) {
            console.debug('[GatewayClient] Failed to record client dispatch span:', err);
        }
    }
}

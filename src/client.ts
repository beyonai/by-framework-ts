import { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { getRedis } from './redis_client';
import { WorkerRegistry } from './registry';
import { CancelTaskResponse, ExecutionStatus, SendMessageResponse } from './protocol/responses';
import { ActionType } from './protocol/action_type';
import { AskAgentCommand, BaseCommand, CancelTaskCommand, ResumeCommand } from './protocol/commands';
import { MessageHeader } from './protocol/message_header';
import { QueueNames } from './constants';
import { BaiYingMessage } from './protocol/message';
import { GatewayInterceptor } from './interceptors';

// === Types ===
interface RouteResolution {
    streamName: string;
    targetWorkerId: string;
}

interface SendMessageParams {
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
}

export class GatewayClient {
    private readonly redis: Redis;
    private readonly registry: WorkerRegistry;
    private readonly interceptors: GatewayInterceptor[];

    public constructor(registry?: WorkerRegistry, redisClient?: Redis, interceptors?: GatewayInterceptor[]) {
        this.redis = redisClient ?? getRedis();
        this.registry = registry ?? new WorkerRegistry(this.redis);
        this.interceptors = interceptors ?? [];
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

    private async initializeQueuedExecution(execution: Record<string, unknown>): Promise<void> {
        const registry = this.registry as WorkerRegistry & {
            initializeExecution?: (execution: Record<string, unknown>) => Promise<void>;
            saveExecution?: (execution: Record<string, unknown>) => Promise<void>;
        };

        if (typeof registry.initializeExecution === 'function') {
            await registry.initializeExecution(execution);
            return;
        }
        if (typeof registry.saveExecution === 'function') {
            await registry.saveExecution(execution);
        }
    }

    async sendCommand(command: BaseCommand, streamName?: string): Promise<SendMessageResponse> {
        const resolvedStreamName = streamName || QueueNames.ctrl_stream(command.header.targetAgentType);

        if (!streamName && command.header.targetAgentType) {
            await this.initializeQueuedExecution({
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
        }

        await this.redis.xadd(resolvedStreamName, '*', 'data', JSON.stringify(command.toDict()));

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

        // Resolve route and optionally probe agent type/liveness
        let route: RouteResolution;
        try {
            if (params.targetWorkerId) {
                route = await this.resolveDirectWorkerRoute(params.targetWorkerId, requireOnline);
            } else {
                route = await this.resolveAgentTypeRoute(params.targetAgentType, requireOnline);
            }
        } catch (err) {
            const error = err instanceof Error ? err.message : String(err);
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

        const messageId = params.messageId || `msg-${uuidv4().slice(0, 8)}`;
        const traceId = params.traceId || uuidv4().replace(/-/g, '');

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
            metadata: requestParams.metadata,
        });

        const payload = requestParams.extraPayload ?? {};
        const mergedPayload: Record<string, unknown> = {
            ...payload,
            attachments: (payload as { attachments?: unknown[] })?.attachments || []
        };

        const command = this.buildGatewayCommand(
            requestParams.actionType || ActionType.ASK_AGENT,
            header,
            formattedContent,
            mergedPayload
        );

        await this.initializeQueuedExecution({
            execution_id: `exec-${uuidv4().slice(0, 8)}`,
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
        }).catch(() => undefined);

        await this.redis.xadd(route.streamName, '*', 'data', JSON.stringify(command.toDict()));

        return {
            success: true,
            message_id: messageId,
            trace_id: traceId,
            target_worker_id: route.targetWorkerId,
            timestamp: Date.now(),
            status: ExecutionStatus.QUEUED,
        };
    }
}

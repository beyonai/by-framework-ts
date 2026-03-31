import { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { getRedis } from './redis_client';
import { WorkerRegistry } from './registry';
import { CancelTaskResponse, SendMessageResponse } from './protocol/responses';
import { ActionType } from './protocol/action_type';
import { AskAgentCommand, BaseCommand, CancelTaskCommand, ResumeCommand } from './protocol/commands';
import { MessageHeader } from './protocol/message_header';
import { QueueNames } from './constants';
import { BaiYingMessage } from './protocol/message';

// === Types ===
interface SendMessageParams {
    readonly targetAgentType: string;
    readonly sessionId: string;
    readonly content: string | BaiYingMessage | ReadonlyArray<BaiYingMessage>;
    readonly sourceAgentType?: string;
    readonly traceId?: string;
    readonly tenantId?: string;
    readonly actionType?: ActionType;
    readonly payload?: Readonly<Record<string, unknown>>;
    readonly parentMessageId?: string;
    readonly messageId?: string;
}

export class GatewayClient {
    private readonly redis: Redis;
    private readonly registry: WorkerRegistry;

    public constructor(registry?: WorkerRegistry, redisClient?: Redis) {
        this.redis = redisClient ?? getRedis();
        this.registry = registry ?? new WorkerRegistry(this.redis);
    }

    async sendCommand(command: BaseCommand, streamName?: string): Promise<SendMessageResponse> {
        const resolvedStreamName = streamName || QueueNames.ctrl_stream(command.header.targetAgentType);
        const targetWorkerId = command.header.targetAgentType
            ? await this.registry.getTargetWorker(command.header.targetAgentType)
            : '';

        if (!streamName && command.header.targetAgentType && !targetWorkerId) {
            return {
                success: false,
                status: 'FAILED',
                message_id: '',
                trace_id: '',
                target_worker_id: '',
                timestamp: Date.now(),
            };
        }

        await this.redis.xadd(resolvedStreamName, '*', 'data', JSON.stringify(command.toDict()));

        if (!streamName && command.header.targetAgentType) {
            await this.registry.saveExecution({
                execution_id: `exec-${uuidv4().slice(0, 8)}`,
                message_id: command.header.messageId,
                session_id: command.header.sessionId,
                worker_id: '',
                target_agent_type: command.header.targetAgentType,
                stream_name: resolvedStreamName,
                redis_message_id: '',
                status: 'QUEUED',
                cancel_requested: false,
                cancel_reason: '',
                created_at: Date.now(),
                started_at: 0,
                finished_at: 0,
                updated_at: Date.now(),
            });
        }

        return {
            success: true,
            message_id: command.header.messageId,
            trace_id: command.header.traceId,
            target_worker_id: targetWorkerId || '',
            timestamp: Date.now(),
            status: 'QUEUED',
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
        const execution = await this.registry.getExecutionByMessageId(params.messageId, params.sessionId);
        if (!execution) {
            return {
                success: false,
                message_id: params.messageId,
                execution_id: '',
                worker_id: '',
                status: 'NOT_FOUND',
                timestamp: Date.now(),
                error: `execution not found for message_id=${params.messageId}`,
            };
        }

        if (execution.session_id !== params.sessionId) {
            return {
                success: false,
                message_id: params.messageId,
                execution_id: execution.execution_id || '',
                worker_id: execution.worker_id || '',
                status: 'NOT_FOUND',
                timestamp: Date.now(),
                error: `session mismatch for message_id=${params.messageId}`,
            };
        }

        const executionStatus = execution.status || '';
        if (['COMPLETED', 'FAILED', 'CANCELLED'].includes(executionStatus)) {
            return {
                success: false,
                message_id: params.messageId,
                execution_id: execution.execution_id || '',
                worker_id: execution.worker_id || '',
                status: 'ALREADY_FINISHED',
                timestamp: Date.now(),
                error: `execution already in terminal state: ${executionStatus}`,
            };
        }

        const executionId = String(execution.execution_id);
        const workerId = String(execution.worker_id);
        const reason = params.reason || '';
        await this.registry.markExecutionCancelling(executionId, params.sessionId, reason);

        const cancelCommand = new CancelTaskCommand(
            new MessageHeader(`msg-cancel-${uuidv4().slice(0, 8)}`, params.sessionId, uuidv4().replace(/-/g, ''), {
                targetAgentType: params.targetAgentType || execution.target_agent_type || '',
                parentMessageId: params.messageId,
            }),
            params.messageId,
            executionId,
            workerId,
            reason,
            params.requestedBy || 'client',
            params.cancelMode || 'graceful'
        );

        if (workerId) {
            await this.sendCommand(cancelCommand, QueueNames.worker_ctrl_stream(workerId));
        }

        return {
            success: true,
            message_id: params.messageId,
            execution_id: executionId,
            worker_id: workerId,
            status: 'CANCEL_REQUESTED',
            timestamp: Date.now(),
        };
    }

    async sendMessage(params: SendMessageParams): Promise<SendMessageResponse> {
        const workerId = await this.registry.getTargetWorker(params.targetAgentType);
        if (!workerId) {
            return {
                success: false,
                status: 'FAILED',
                message_id: '',
                trace_id: '',
                target_worker_id: '',
                timestamp: Date.now(),
            };
        }

        const messageId = params.messageId || `msg-${uuidv4().slice(0, 8)}`;
        const traceId = params.traceId || uuidv4().replace(/-/g, '');

        // 序列化 content
        let formattedContent: string | unknown[];
        if (typeof params.content === 'string') {
            formattedContent = params.content;
        } else {
            const msgs = Array.isArray(params.content) ? params.content : [params.content];
            formattedContent = msgs.map((m): unknown => {
                // 如果传入的已经是序列化后的字典格式，不强求实例化
                if (!m || typeof m !== 'object') {
                    return m;
                }

                // 确保对 role 和 content 属性进行安全提取
                const role = (m as BaiYingMessage).role;
                const innerContent = (m as BaiYingMessage).content;

                // 如果没有 content 属性，可能这是一个非标准的自由扩展对象，直接透传
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

        const header = new MessageHeader(messageId, params.sessionId, traceId, {
            sourceAgentType: params.sourceAgentType || '',
            targetAgentType: params.targetAgentType,
            parentMessageId: params.parentMessageId || '',
            tenantId: params.tenantId || '',
        });

        const payload = params.payload ?? {};
        const mergedPayload: Record<string, unknown> = {
            ...payload,
            attachments: (payload as { attachments?: unknown[] })?.attachments || []
        };

        const command = (params.actionType || ActionType.ASK_AGENT) === ActionType.RESUME
            ? new ResumeCommand(
                header,
                formattedContent as string | unknown[],
                String(mergedPayload.status || ''),
                mergedPayload.reply_data,
                Object.fromEntries(Object.entries(mergedPayload).filter(([key]) => !['status', 'reply_data'].includes(key)))
            )
            : new AskAgentCommand(
                header,
                formattedContent as string | unknown[],
                Boolean((mergedPayload as { wait_for_reply?: boolean }).wait_for_reply),
                Object.fromEntries(Object.entries(mergedPayload).filter(([key]) => key !== 'wait_for_reply'))
            );

        return this.sendCommand(command);
    }
}

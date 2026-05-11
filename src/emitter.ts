import { Redis } from 'ioredis';
import { QueueNames, RegistryKeys } from './constants';
import { DataMessage } from './protocol/data_message';
import { EventType } from './protocol/event_type';
import { getRedis } from './redis_client';
import {
    StateChangeEvent,
    StreamChunkEvent,
    ArtifactEvent,
    AskUserEvent,
} from './protocol/events';
import { SseMessageType, SseReasonMessageType } from './protocol/content_type';
import { v4 as uuidv4 } from 'uuid';

export interface EmitOptions {
    sourceAgentType?: string;
    messageId?: string;
    parentMessageId?: string;
    metadata?: Record<string, any>;
    eventType?: EventType | string;
    contentType?: string;
    objectType?: string;
    status?: string;
}

interface SseLayoutParams {
    content?: string | null;
    role?: string | null;
    contentType: string;
    sourceAgentType: string;
    functionCall?: Record<string, any> | null;
    toolCalls?: Array<Record<string, any>> | null;
    orderId?: string;
    parentOrderId?: string;
    objectType?: string;
    status?: string;
}

/**
 * 原子化数据上报器。
 * 允许在任何地方独立于业务上下文发送数据到数据流。
 */
export class GatewayDataEmitter {
    private redis: Redis;
    private fixedDataStreamName?: string;
    private sourceAgentType?: string;

    constructor(redisClient?: Redis, params?: {
        sourceAgentType?: string,
        dataStreamName?: string,
    }) {
        this.redis = redisClient || getRedis();
        // 如果提供了固定的 dataStreamName，则始终使用它（保持兼容性）
        this.fixedDataStreamName = params?.dataStreamName;
        this.sourceAgentType = params?.sourceAgentType;
    }

    private _buildSseLayout(params: SseLayoutParams): Record<string, any> {
        const {
            content,
            role,
            contentType,
            sourceAgentType,
            functionCall,
            toolCalls,
            orderId,
            parentOrderId,
            objectType,
            status,
        } = params;
        return {
            id: uuidv4().replace(/-/g, '').toUpperCase(),
            created: Math.floor(Date.now() / 1000),
            model: "",
            object: "",
            objectType,
            contentType,
            status,
            agentId: sourceAgentType || null,
            orderId: orderId || null,
            parentOrderId: parentOrderId || null,
            choices: [
                {
                    index: 0,
                    finish_reason: "",
                    delta: {
                        role,
                        content: content ?? null,
                        function_call: functionCall ?? null,
                        tool_calls: toolCalls ?? null
                    }
                }
            ]
        };
    }

    /**
     * 底层事件上报方法
     */
    async emitEvent(params: {
        sessionId: string;
        traceId: string;
        eventType: string;
        sourceAgentType?: string;
        messageId?: string;
        parentMessageId?: string;
        data?: Record<string, any>;
        stateMsg?: string;
        artifactUrl?: string;
        metadata?: Record<string, any>;
    }): Promise<void> {
        const msg: DataMessage = {
            trace_id: params.traceId,
            session_id: params.sessionId,
            event_type: params.eventType,
            source_agent_type: params.sourceAgentType || '',
            message_id: params.messageId || '',
            parent_message_id: params.parentMessageId || '',
            timestamp: Date.now(),
            data: params.data || {},
            state_msg: params.stateMsg || '',
            artifact_url: params.artifactUrl || '',
            metadata: params.metadata || {},
        };

        // 确定流名称：优先使用 Session 隔离流，除非初始化时指定了固定流名
        const streamName = this.fixedDataStreamName || QueueNames.session_data_stream(params.sessionId);
        await this.redis.pipeline()
            .xadd(streamName, '*', 'data', JSON.stringify(msg))
            .expire(streamName, RegistryKeys.DEFAULT_SESSION_TTL)
            .exec();
    }

    async emitChunk(
        sessionId: string,
        traceId: string,
        event: StreamChunkEvent | string,
        options: EmitOptions = {}
    ): Promise<void> {
        const chunkEvent = typeof event === 'string' ? { content: event } : event;
        await this.emitEvent({
            sessionId,
            traceId,
            eventType: options.eventType || EventType.ANSWER_DELTA,
            sourceAgentType: options.sourceAgentType || this.sourceAgentType,
            messageId: options.messageId,
            parentMessageId: options.parentMessageId,
            data: this._buildSseLayout(
                {
                    content: chunkEvent.content,
                    role: chunkEvent.role ?? 'assistant',
                    contentType: options.contentType || SseMessageType.text,
                    sourceAgentType: options.sourceAgentType || '',
                    functionCall: chunkEvent.function_call,
                    toolCalls: chunkEvent.tool_calls,
                    orderId: options.messageId,
                    parentOrderId: options.parentMessageId,
                    objectType: options.objectType,
                    status: options.status,
                }
            ),
            metadata: (typeof event === 'string' ? {} : event.metadata) || options.metadata,
        });
    }

    async emitState(
        sessionId: string,
        traceId: string,
        event: StateChangeEvent | string,
        options: EmitOptions = {}
    ): Promise<void> {
        const stateMsg = typeof event === 'string' ? event : event.state;
        await this.emitEvent({
            sessionId,
            traceId,
            eventType: options.eventType || EventType.REASONING_LOG_DELTA,
            sourceAgentType: options.sourceAgentType || this.sourceAgentType,
            messageId: options.messageId,
            parentMessageId: options.parentMessageId,
            data: this._buildSseLayout(
                {
                    content: stateMsg,
                    role: null,
                    contentType: options.contentType || SseReasonMessageType.think_title,
                    sourceAgentType: options.sourceAgentType || '',
                    functionCall: null,
                    toolCalls: null,
                    orderId: options.messageId,
                    parentOrderId: options.parentMessageId,
                }
            ),
            metadata: (typeof event === 'string' ? {} : event.metadata) || options.metadata,
        });
    }

    async emitArtifact(
        sessionId: string,
        traceId: string,
        event: ArtifactEvent | string,
        options: EmitOptions = {}
    ): Promise<void> {
        const artifactUrl = typeof event === 'string' ? event : event.url;
        const filesPayload = [{ fileUrl: artifactUrl }];
        await this.emitEvent({
            sessionId,
            traceId,
            eventType: options.eventType || EventType.REASONING_LOG_DELTA,
            sourceAgentType: options.sourceAgentType,
            messageId: options.messageId,
            parentMessageId: options.parentMessageId,
            data: this._buildSseLayout(
                {
                    content: JSON.stringify(filesPayload),
                    role: null,
                    contentType: SseReasonMessageType.task_create_file,
                    sourceAgentType: options.sourceAgentType || '',
                    functionCall: null,
                    toolCalls: null,
                    orderId: options.messageId,
                    parentOrderId: options.parentMessageId,
                }
            ),
            metadata: (typeof event === 'string' ? {} : event.metadata) || options.metadata,
        });
    }

    async askUser(
        sessionId: string,
        traceId: string,
        event: AskUserEvent | string,
        options: EmitOptions = {}
    ): Promise<void> {
        const prompt = typeof event === 'string' ? event : event.prompt;
        const inputForm = {
            formStatus: 0,
            pluginMachineFields: [
                {
                    formType: 'textarea',
                    fieldName: '用户输入',
                    fieldCode: 'user_input',
                    description: prompt,
                    required: true,
                }
            ]
        };
        await this.emitEvent({
            sessionId,
            traceId,
            eventType: EventType.REASONING_LOG_DELTA,
            sourceAgentType: options.sourceAgentType,
            messageId: options.messageId,
            parentMessageId: options.parentMessageId,
            data: this._buildSseLayout(
                {
                    content: JSON.stringify(inputForm),
                    role: 'assistant',
                    contentType: SseReasonMessageType.task_user_input,
                    sourceAgentType: options.sourceAgentType || '',
                    functionCall: null,
                    toolCalls: null,
                    orderId: options.messageId,
                    parentOrderId: options.parentMessageId,
                }
            ),
            metadata: (typeof event === 'string' ? {} : event.metadata) || options.metadata,
        });
    }
}

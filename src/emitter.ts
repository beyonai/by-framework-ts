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
    sourceAgentId?: string;
    messageId?: string;
    metadata?: Record<string, any>;
    eventType?: string;
}

/**
 * 原子化数据上报器。
 * 允许在任何地方独立于业务上下文发送数据到数据流。
 */
export class GatewayDataEmitter {
    private redis: Redis;

    constructor(redisClient?: Redis) {
        this.redis = redisClient || getRedis();
    }

    private _buildSseLayout(
        content: string | null | undefined,
        role: string | null,
        contentType: string,
        sourceAgentId: string,
        functionCall?: Record<string, any> | null,
        toolCalls?: Array<Record<string, any>> | null
    ): Record<string, any> {
        return {
            id: uuidv4().replace(/-/g, '').toUpperCase(),
            created: Math.floor(Date.now() / 1000),
            model: "",
            object: "",
            contentType,
            agentId: sourceAgentId || null,
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
        sourceAgentId?: string;
        messageId?: string;
        data?: Record<string, any>;
        stateMsg?: string;
        artifactUrl?: string;
        metadata?: Record<string, any>;
    }): Promise<void> {
        const msg: DataMessage = {
            trace_id: params.traceId,
            session_id: params.sessionId,
            event_type: params.eventType,
            source_agent_id: params.sourceAgentId || '',
            message_id: params.messageId || '',
            timestamp: Date.now(),
            data: params.data || {},
            state_msg: params.stateMsg || '',
            artifact_url: params.artifactUrl || '',
            metadata: params.metadata || {},
        };

        const streamName = QueueNames.session_data_stream(params.sessionId);
        await this.redis.pipeline()
            .xadd(streamName, '*', 'data', JSON.stringify(msg))
            .expire(streamName, RegistryKeys.DEFAULT_SESSION_TTL)
            .exec();
    }

    async emitChunk(sessionId: string, traceId: string, event: StreamChunkEvent | string, options: EmitOptions = {}): Promise<void> {
        const chunkEvent = typeof event === 'string' ? { content: event } : event;
        await this.emitEvent({
            sessionId,
            traceId,
            eventType: options.eventType || EventType.ANSWER_DELTA,
            sourceAgentId: options.sourceAgentId,
            messageId: options.messageId,
            data: this._buildSseLayout(
                chunkEvent.content,
                chunkEvent.role ?? 'assistant',
                SseMessageType.text,
                options.sourceAgentId || '',
                chunkEvent.function_call,
                chunkEvent.tool_calls
            ),
            metadata: (typeof event === 'string' ? {} : event.metadata) || options.metadata,
        });
    }

    async emitState(sessionId: string, traceId: string, event: StateChangeEvent | string, options: EmitOptions = {}): Promise<void> {
        const stateMsg = typeof event === 'string' ? event : event.state;
        await this.emitEvent({
            sessionId,
            traceId,
            eventType: options.eventType || EventType.REASONING_LOG_DELTA,
            sourceAgentId: options.sourceAgentId,
            messageId: options.messageId,
            data: this._buildSseLayout(stateMsg, null, SseReasonMessageType.think_title, options.sourceAgentId || ''),
            metadata: (typeof event === 'string' ? {} : event.metadata) || options.metadata,
        });
    }

    async emitArtifact(sessionId: string, traceId: string, event: ArtifactEvent | string, options: EmitOptions = {}): Promise<void> {
        const artifactUrl = typeof event === 'string' ? event : event.url;
        const filesPayload = [{ fileUrl: artifactUrl }];
        await this.emitEvent({
            sessionId,
            traceId,
            eventType: options.eventType || EventType.REASONING_LOG_DELTA,
            sourceAgentId: options.sourceAgentId,
            messageId: options.messageId,
            data: this._buildSseLayout(JSON.stringify(filesPayload), null, SseReasonMessageType.task_create_file, options.sourceAgentId || ''),
            metadata: (typeof event === 'string' ? {} : event.metadata) || options.metadata,
        });
    }

    async askUser(sessionId: string, traceId: string, event: AskUserEvent | string, options: EmitOptions = {}): Promise<void> {
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
            sourceAgentId: options.sourceAgentId,
            messageId: options.messageId,
            data: this._buildSseLayout(JSON.stringify(inputForm), 'assistant', SseReasonMessageType.task_user_input, options.sourceAgentId || ''),
            metadata: (typeof event === 'string' ? {} : event.metadata) || options.metadata,
        });
    }
}

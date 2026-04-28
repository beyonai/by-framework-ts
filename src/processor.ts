import { v4 as uuidv4 } from 'uuid';
import { Redis } from 'ioredis';
import { GatewayCommand, ResumeCommand } from './protocol/commands';
import { AgentState } from './protocol/agent_state';
import { EventType } from './protocol/event_type';
import { AgentContext } from './context';
import { QueueNames } from './constants';
import { getRedis } from './redis_client';
import { MessageHeader } from './protocol/message_header';
import { JsonValue, ProcessCommandResult, WireContent, normalizeProcessResult } from './protocol/results';

export type ContextHandler = (command: GatewayCommand, context: AgentContext) => Promise<ProcessCommandResult>;

export class GatewayProcessor {
    private workerId: string;
    private redis: Redis;

    constructor(workerId: string, redisClient?: Redis) {
        this.workerId = workerId;
        this.redis = redisClient || getRedis();
    }

    async process(command: GatewayCommand, handler: ContextHandler): Promise<any> {
        const traceId = command.header.traceId || uuidv4().replace(/-/g, '');
        const isAgentReturn = command instanceof ResumeCommand;
        const sourceAgentType = command.header.sourceAgentType;
        const hasSourceAgent = !!sourceAgentType && !isAgentReturn;

        const context = new AgentContext(
            command.header.sessionId,
            traceId,
            this.redis,
            command.header.targetAgentType || '',
            command.header.messageId
        );

        console.log(`[${this.workerId}] Processing message: ${command.header.messageId}`);

        try {
            if (isAgentReturn) {
                await context.emitState({ state: AgentState.RESUMED });
            }

            const result = await handler(command, context);
            const taskResult = normalizeProcessResult(result);

            if (hasSourceAgent) {
                await this.enqueueCallback(command, taskResult.status, taskResult.replyData, {
                    content: taskResult.content,
                    metadata: taskResult.metadata,
                    extraPayload: taskResult.extraPayload,
                });
                await context.emitState({ state: `${AgentState.QUEUED}: ${sourceAgentType}` });
            } else {
                await context.emitState({ state: AgentState.COMPLETED });
            }

            // Extract final message and emit FINAL_ANSWER
            let finalMessage: string | null = null;
            if (typeof taskResult.content === 'string' && taskResult.content) {
                finalMessage = taskResult.content;
            } else if (typeof taskResult.replyData === 'string' && taskResult.replyData) {
                finalMessage = taskResult.replyData;
            } else if (taskResult.replyData !== null && taskResult.replyData !== undefined) {
                finalMessage = JSON.stringify(taskResult.replyData);
            }

            if (finalMessage !== null) {
                await context.emitChunk(finalMessage, EventType.FINAL_ANSWER);
            }

            // Emit APP_STREAM_RESPONSE if conditions are met
            const shouldEmitStreamEnd = !hasSourceAgent && !context.isSuspended();
            if (shouldEmitStreamEnd) {
                if (!context.isStreamFinished()) {
                    await context.emitChunk('', EventType.APP_STREAM_RESPONSE);
                }
            }

            return result;
        } catch (error) {
            console.error(`[${this.workerId}] Processing failed:`, error);
            if (hasSourceAgent) {
                await this.enqueueCallback(command, 'FAILED', { error: String(error) });
            }
            await context.emitState({ state: `${AgentState.FAILED}: ${error}` });
            throw error;
        }
    }

    private async enqueueCallback(
        originalCommand: GatewayCommand,
        status: string,
        replyData: JsonValue,
        options: {
            readonly content?: WireContent;
            readonly metadata?: Readonly<Record<string, JsonValue>>;
            readonly extraPayload?: Readonly<Record<string, JsonValue>>;
        } = {}
    ): Promise<void> {
        const header = originalCommand.header;
        const mergedMetadata = {
            ...header.metadata,
            ...(options.metadata ?? {}),
        };
        const callbackMsg = new ResumeCommand(
            new MessageHeader(`msg-${uuidv4().slice(0, 8)}`, header.sessionId, header.traceId || uuidv4().replace(/-/g, ''), {
                sourceAgentType: header.targetAgentType || this.workerId,
                targetAgentType: header.sourceAgentType || '',
                parentMessageId: header.messageId,
                metadata: mergedMetadata,
            }),
            options.content ?? '',
            status,
            replyData,
            options.extraPayload ?? {}
        );

        await this.redis.xadd(
            QueueNames.ctrl_stream(callbackMsg.header.targetAgentType),
            '*',
            'data',
            JSON.stringify(callbackMsg.toDict())
        );
    }
}

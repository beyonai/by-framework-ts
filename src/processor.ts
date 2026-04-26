import { v4 as uuidv4 } from 'uuid';
import { Redis } from 'ioredis';
import { GatewayCommand, ResumeCommand } from './protocol/commands';
import { AgentState } from './protocol/agent_state';
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

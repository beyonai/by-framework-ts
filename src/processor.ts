import { v4 as uuidv4 } from 'uuid';
import { Redis } from 'ioredis';
import { GatewayCommand, ResumeCommand } from './protocol/commands';
import { AgentState, isTerminalState } from './protocol/agent_state';
import { EventType } from './protocol/event_type';
import { AgentContext } from './context';
import { CLIENT_SOURCE_AGENT_TYPE, QueueNames } from './constants';
import { consumeWaitEntry, emitOrphanedReply } from './liveness/wait_gate';
import { flushPendingGroupReplies } from './liveness/wait_reply';
import { mergeResumeMetadata } from './resume_metadata';
import { getRedis } from './redis_client';
import { MessageHeader } from './protocol/message_header';
import { WorkerRegistry } from './registry';
import { JsonValue, ProcessCommandResult, WireContent, normalizeProcessResult } from './protocol/results';

export type ContextHandler = (command: GatewayCommand, context: AgentContext) => Promise<ProcessCommandResult>;

export class GatewayProcessor {
    private workerId: string;
    private redis: Redis;

    constructor(workerId: string, redisClient?: Redis) {
        this.workerId = workerId;
        this.redis = redisClient || getRedis();
    }

    /**
     * Process a single message using the provided handler function.
     *
     * Returns the handler's result, or `null` when the message was a reply to a
     * wait that is already resolved (see the idempotency gate below); such a
     * message is fully handled and should be acknowledged by the caller's
     * consume loop like any other.
     */
    async process(command: GatewayCommand, handler: ContextHandler): Promise<any> {
        const traceId = command.header.traceId || uuidv4().replace(/-/g, '');
        const isAgentReturn = command instanceof ResumeCommand;

        if (isAgentReturn) {
            // Same idempotency gate as WorkerRunner.processAndAck, and for the
            // same reason: this is a second, independent entry point for replies
            // (callers that drive their own consume loop instead of subclassing
            // GatewayWorker). A gate on only one entry point is not a gate —
            // replies arriving via the other one would both wake an
            // already-resolved caller and leave the wait-index entry behind for
            // a sweep to resolve all over again.
            const gate = await consumeWaitEntry(this.redis, command);
            if (!gate.allow) {
                console.warn(
                    `[${this.workerId}] Dropping reply for an already-resolved wait `
                    + `(${gate.reason}): message_id=${command.header.messageId}, `
                    + `child_message_id=${command.header.parentMessageId}, `
                    + `session_id=${command.header.sessionId}`
                );
                // Reporting only, after the decision — never blocks the drop.
                await emitOrphanedReply(this.redis, command, {
                    workerId: this.workerId,
                    reason: gate.reason,
                });
                return null;
            }
        }

        // Same rules as GatewayWorker, and for the same reason: this is a
        // second, independent entry point for replies (callers that drive their
        // own consume loop instead of subclassing GatewayWorker).
        // `!!sourceAgentType && !isAgentReturn` denied every resumed execution a
        // reply here too. One read of the execution record feeds both
        // directions — the header this execution replies with, and the header
        // its own handler reads — which are different rules over the same data.
        const snapshot = isAgentReturn ? await this.loadExecutionSnapshot(command) : null;
        const rawCommand = command;
        const replyHeader = this.resolveReplyHeader(command, snapshot);
        const hasSourceAgent = replyHeader !== null;
        const sourceAgentType = replyHeader?.sourceAgentType ?? '';
        // Runs regardless of hasSourceAgent: a client-dispatched root is owed
        // no reply but still has its own metadata to get back, which is exactly
        // the case that motivated this.
        const inboundCommand = GatewayProcessor.restoreInboundMetadata(command, snapshot);

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

            const result = await handler(inboundCommand, context);
            // Same rule as GatewayWorker.handleMessage: stand-ins for Task Group
            // members that never reached a worker go out only once the handler
            // has returned normally, and never when it threw.
            await flushPendingGroupReplies(this.redis, context, this.workerId);
            const taskResult = normalizeProcessResult(result);

            // A suspended execution has no result yet — replying with the value
            // the handler returned so it could unwind would wake the caller
            // early and consume the one reply it waits for. Same rule as
            // GatewayWorker.handleMessage, including its exception: a handler
            // that returned a terminal status is finished and will never be
            // resumed to reply later, so it must reply now.
            const isSuspended = context.isSuspended() && !isTerminalState(taskResult.status);
            if (hasSourceAgent && !isSuspended) {
                // rawCommand, not the inbound-restored one: the reply is the
                // outbound direction and must not inherit the inbound merge.
                await this.enqueueCallback(rawCommand, taskResult.status, taskResult.replyData, {
                    content: taskResult.content,
                    metadata: taskResult.metadata,
                    extraPayload: taskResult.extraPayload,
                    replyHeader: replyHeader!,
                });
                await context.emitState({ state: `${AgentState.QUEUED}: ${sourceAgentType}` });
            } else if (!hasSourceAgent) {
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
                // Sent regardless of suspension: the execution died, so no later
                // resume will produce the reply the caller awaits.
                await this.enqueueCallback(rawCommand, 'FAILED', { error: String(error) }, {
                    replyHeader: replyHeader!,
                });
            }
            await context.emitState({ state: `${AgentState.FAILED}: ${error}` });
            throw error;
        }
    }

    /**
     * Return the header describing the caller this execution owes a reply to,
     * or null when nobody is waiting.
     *
     * Mirrors GatewayWorker.resolveReplyCommand: a resume's header describes the
     * sub-agent that just finished, so the caller has to be read back from the
     * execution record the original dispatch wrote — and, for the same reason as
     * there, CLIENT_SOURCE_AGENT_TYPE is not a caller. It is what a client
     * writes on a root execution's record, and nothing consumes its control
     * stream.
     *
     * Four fields come from that record, not from the waking message:
     * sourceAgentType, parentMessageId, taskGroupId and metadata. The last one
     * is restored as a full REPLACEMENT rather than a merge, exactly as
     * GatewayWorker.resolveReplyCommand does it: the waking message (an askUser
     * answer, or a sub-call's reply) is transient plumbing for that one hop, not
     * something the caller ever sent. A record missing the field (an execution
     * written before it existed) degrades to an empty object rather than leaking
     * the waking message's metadata to the caller.
     *
     * This is the OUTBOUND direction only. What the handler itself reads is
     * restoreInboundMetadata, which merges rather than replaces — and which
     * must run even when this returns null, since a client-dispatched root has
     * no caller but still has its own metadata to get back.
     *
     * Unlike the worker path the record is queried here rather than handed in:
     * a GatewayProcessor has no runner feeding it an execution snapshot. The
     * query itself lives in loadExecutionSnapshot so one read feeds both
     * directions. Mirrors Python processor.py's _resolve_reply_header.
     */
    private resolveReplyHeader(
        command: GatewayCommand,
        snapshot: Record<string, any> | null
    ): MessageHeader | null {
        const header = command.header;
        if (!(command instanceof ResumeCommand)) {
            return header.sourceAgentType ? header : null;
        }

        const execution = snapshot;
        const callerAgentType = String(execution?.source_agent_type || '');
        if (!callerAgentType || callerAgentType === CLIENT_SOURCE_AGENT_TYPE) {
            return null;
        }
        return new MessageHeader(header.messageId, header.sessionId, header.traceId, {
            sourceAgentType: callerAgentType,
            targetAgentType: header.targetAgentType,
            parentMessageId: String(execution?.parent_message_id || ''),
            taskGroupId: String(execution?.task_group_id || ''),
            userCode: header.userCode,
            userName: header.userName,
            // Replacement, not a merge with header.metadata — see above.
            metadata: { ...(execution?.metadata || {}) },
            traceParentSpanId: header.traceParentSpanId,
            langfuseParentObservationId: header.langfuseParentObservationId,
        });
    }

    /**
     * Read the execution record the original dispatch wrote.
     *
     * Fetched once per resume and shared by both restore directions. Fail-soft:
     * a registry error degrades to "no record", which each caller then handles
     * as its own no-op. Mirrors Python processor.py's _load_execution_snapshot.
     */
    private async loadExecutionSnapshot(
        command: GatewayCommand
    ): Promise<Record<string, any> | null> {
        const header = command.header;
        try {
            return await new WorkerRegistry(this.redis).getExecutionByMessageId(
                header.messageId,
                header.sessionId
            );
        } catch (error) {
            console.warn(
                `[${this.workerId}] Could not load the execution record of resumed `
                + `execution ${header.messageId}: ${error}`
            );
            return null;
        }
    }

    /**
     * Give a resumed handler its own dispatch metadata back.
     *
     * Mirrors GatewayWorker.restoreInboundMetadata, including its
     * merge-don't-replace rule and its no-mutation rule; see that method for
     * why the inbound direction differs from the outbound one.
     */
    private static restoreInboundMetadata(
        command: GatewayCommand,
        snapshot: Record<string, any> | null
    ): GatewayCommand {
        if (!(command instanceof ResumeCommand)) {
            return command;
        }
        const header = command.header;
        return new ResumeCommand(
            new MessageHeader(header.messageId, header.sessionId, header.traceId, {
                sourceAgentType: header.sourceAgentType,
                targetAgentType: header.targetAgentType,
                parentMessageId: header.parentMessageId,
                taskGroupId: header.taskGroupId,
                userCode: header.userCode,
                userName: header.userName,
                metadata: mergeResumeMetadata(snapshot?.metadata, header.metadata),
                traceParentSpanId: header.traceParentSpanId,
                langfuseParentObservationId: header.langfuseParentObservationId,
            }),
            command.content,
            command.status,
            command.replyData,
            command.extraPayload
        );
    }

    private async enqueueCallback(
        originalCommand: GatewayCommand,
        status: string,
        replyData: JsonValue,
        options: {
            readonly content?: WireContent;
            readonly metadata?: Readonly<Record<string, JsonValue>>;
            readonly extraPayload?: Readonly<Record<string, JsonValue>>;
            /** Rebuilt caller header; see resolveReplyHeader. */
            readonly replyHeader?: MessageHeader;
        } = {}
    ): Promise<void> {
        const header = options.replyHeader ?? originalCommand.header;
        const mergedMetadata = {
            ...header.metadata,
            ...(options.metadata ?? {}),
        };
        const callbackMsg = new ResumeCommand(
            // The caller reattaches its suspended execution by this id, so it
            // must be the caller's own message_id (this dispatch's
            // parentMessageId) — a freshly minted id resolves to no execution
            // and orphans the caller.
            new MessageHeader(header.parentMessageId || `msg-${uuidv4().slice(0, 8)}`, header.sessionId, header.traceId || uuidv4().replace(/-/g, ''), {
                sourceAgentType: header.targetAgentType || this.workerId,
                targetAgentType: header.sourceAgentType || '',
                parentMessageId: header.messageId,
                // Absent before: a reply for a Task Group member that carries no
                // taskGroupId never reaches the caller's Group Join, so the
                // group's `completed` never reaches `total`.
                taskGroupId: header.taskGroupId || '',
                userCode: header.userCode,
                userName: header.userName,
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

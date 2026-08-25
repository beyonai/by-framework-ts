/**
 * Replies built by someone other than the callee that owed them.
 *
 * Two places have to produce a `ResumeCommand` on behalf of a sub-task that
 * will never send one itself:
 *
 * - the wait sweeper — the callee's worker died, or it ran past its renewal
 *   ceiling, or its reply was lost after it finished;
 * - `AgentContext.dispatchGroup` — a member fanned out to a target agent type
 *   that was not available, so that sub-task never reached a worker at all.
 *
 * Both must produce the *same* message a real sub-agent would have produced,
 * because everything downstream (the runner's execution reattachment, the
 * idempotency gate, Task Group join) keys off that exact shape. Two independent
 * constructions of it drift, and the drift is invisible until a caller hangs —
 * which is why the construction lives here once rather than being spelled out
 * at each site.
 *
 * The three load-bearing details, all of them easy to get backwards:
 *
 * - `header.messageId` is the **caller's** message id. `WorkerRunner`
 *   reattaches the suspended execution with
 *   `getExecutionByMessageId(header.messageId)`, so any other value starts a
 *   fresh, disconnected execution instead of resuming the caller.
 * - `header.parentMessageId` is the **sub-task's** dispatch-time message id. It
 *   is the only per-sibling-unique id a reply carries, so Task Group join keys
 *   results by it and `wait_index.memberFromResume` rebuilds the wait-index
 *   member from it.
 * - The failure detail rides in `replyData`, where a sub-agent that ran and
 *   threw puts it (`GatewayWorker.handleMessage`'s error path). A caller must
 *   not be able to tell "failed" from "never got to fail"; putting the error
 *   anywhere else grows a second error path in every caller.
 *
 * Mirrors by-framework-python `core/wait_reply.py`.
 */

import type { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { QueueNames } from '../constants';
import { ResumeCommand } from '../protocol/commands';
import { MessageHeader } from '../protocol/message_header';
import type { JsonValue, WireContent } from '../protocol/results';

/**
 * Who built a stand-in, carried on the reply's metadata (never in `replyData`,
 * which is business-visible payload).
 */
export const SYNTHESIZED_BY_SWEEPER = 'wait_sweeper';
export const SYNTHESIZED_BY_DISPATCH = 'dispatch';

/**
 * The `replyData` of a stand-in failure.
 *
 * `child_message_id` is duplicated out of the header so an operator reading the
 * payload alone can still identify the sub-task that vanished.
 */
export function failureReplyData(params: {
    readonly error: string;
    readonly errorCode: string;
    readonly childMessageId: string;
}): Record<string, JsonValue> {
    return {
        error: params.error,
        error_code: params.errorCode,
        child_message_id: params.childMessageId,
    };
}

export interface StandInReplyParams {
    readonly sessionId: string;
    /** The suspended caller's own message id — what the resume reattaches by. */
    readonly callerMessageId: string;
    /** The caller's agent type; the reply is addressed to its control stream. */
    readonly callerAgentType: string;
    /** The sub-task's dispatch-time message id. */
    readonly childMessageId: string;
    /** The sub-task's target agent type, i.e. who *would* have replied. */
    readonly childAgentType?: string;
    readonly taskGroupId?: string;
    readonly traceId?: string;
    readonly status: string;
    readonly content?: WireContent;
    readonly replyData?: JsonValue;
    readonly extraPayload?: Readonly<Record<string, JsonValue>>;
    readonly metadata?: Readonly<Record<string, JsonValue>>;
    /** Liveness error code, recorded on metadata for operators. */
    readonly errorCode?: string;
    readonly synthesizedBy: string;
    readonly userCode?: string;
    readonly userName?: string;
}

/**
 * Build the reply the callee would have sent, addressed at its caller.
 *
 * Field-for-field the same command `GatewayWorker.enqueueAgentReturn` produces
 * — see the module comment for the three ids that must not be swapped. It is
 * deliberately *not* marked in `replyData`: business code reads that, and a
 * caller that behaves differently for a synthesized failure than for a real one
 * has two error paths again. The provenance goes on `header.metadata` instead,
 * for operators.
 *
 * A reply carrying `taskGroupId` is stored and counted by the group's existing
 * join like any other, so an orphaned member resolves the caller only when it is
 * the last sibling outstanding.
 */
export function standInReply(params: StandInReplyParams): ResumeCommand {
    const mergedMetadata: Record<string, JsonValue> = {
        ...(params.metadata ?? {}),
        synthesized_by: params.synthesizedBy,
        liveness_error_code: params.errorCode ?? '',
        child_message_id: params.childMessageId,
    };
    return new ResumeCommand(
        new MessageHeader(
            params.callerMessageId,
            params.sessionId,
            params.traceId || uuidv4().replace(/-/g, ''),
            {
                sourceAgentType: params.childAgentType ?? '',
                targetAgentType: params.callerAgentType,
                parentMessageId: params.childMessageId,
                taskGroupId: params.taskGroupId ?? '',
                userCode: params.userCode ?? '',
                userName: params.userName ?? '',
                metadata: mergedMetadata,
            }
        ),
        params.content ?? '',
        params.status,
        params.replyData ?? null,
        params.extraPayload ?? {}
    );
}

/** Minimal view of AgentContext this module needs; keeps the import one-way. */
export interface PendingGroupReplyHolder {
    takePendingGroupReplies(): ResumeCommand[];
}

/**
 * Send the stand-ins `dispatchGroup` queued, once the caller's handler is done.
 *
 * `AgentContext.dispatchGroup` queues a reply for every sub-task whose target
 * agent type was unavailable instead of sending it inline. Sending inline would
 * put a reply on the caller's own control stream strictly *before* the caller's
 * handler returns and its execution is recorded as suspended — turning the
 * pre-existing "a very fast sub-agent replies first" race from unlikely into
 * certain. Flushing after the handler returns leaves that race exactly as bad as
 * it is for real sub-agents, and no worse.
 *
 * Only called when the handler returned normally. If it threw, the caller
 * execution these replies would resume is the one that just failed, and they
 * must not go out at all.
 *
 * Fail-soft per reply: a stand-in that cannot be delivered costs this group its
 * dispatch-time failure notice — which the wait index and its sweep can still
 * compensate, because `dispatchGroup` registered a wait entry for the member
 * before queueing the stand-in — whereas throwing here would also destroy the
 * caller's own result.
 */
export async function flushPendingGroupReplies(
    redis: Redis,
    context: unknown,
    workerId: string = ''
): Promise<void> {
    const holder = context as Partial<PendingGroupReplyHolder> | null | undefined;
    if (!holder || typeof holder.takePendingGroupReplies !== 'function') {
        return;
    }
    const replies = holder.takePendingGroupReplies();
    for (const reply of replies) {
        try {
            await redis.xadd(
                QueueNames.ctrl_stream(reply.header.targetAgentType),
                '*',
                'data',
                JSON.stringify(reply.toDict())
            );
        } catch (error) {
            console.error(
                `[${workerId}] Failed to deliver the dispatch-failure reply for Task Group `
                + `${reply.header.taskGroupId} sub-task ${reply.header.parentMessageId}: ${error}`
            );
        }
    }
}

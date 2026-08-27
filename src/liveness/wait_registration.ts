/**
 * Writing side of the wait index: "this execution is suspended waiting for a
 * reply".
 *
 * Split out of AgentContext (where Python keeps it as `_register_wait`) because
 * TS has two dispatch paths that must register identically — `AgentContext`
 * (askUser / dispatchGroup) and the `src/dispatch/` publish pipeline
 * (callAgent) — and a second copy of this is exactly the drift the cross-SDK
 * contract exists to prevent.
 */

import type { Redis } from 'ioredis';
import { encodeMember, waitIndexKey } from './wait_index';
import { consumedMarkerKey } from './wait_gate';

export interface RegisterWaitParams {
    readonly sessionId: string;
    /**
     * The message_id the awaited reply will carry in `header.messageId` — i.e.
     * the id the suspended execution is reattached by.
     */
    readonly parentMessageId: string;
    /**
     * The reply's `header.parentMessageId`, i.e. the dispatched sub-task's own
     * message_id. Empty for askUser, which has no sub-task.
     */
    readonly childMessageId: string;
    readonly taskGroupId?: string;
    /** How long this execution may stay suspended before a sweep resolves it. */
    readonly timeoutMs: number;
    /** Injectable clock, for tests. */
    readonly now?: () => number;
}

/**
 * Record a suspended caller in the wait index, so a reply that never arrives
 * can still be resolved.
 *
 * `parentMessageId` must be the message_id the awaited reply will carry in
 * `header.messageId`, and `childMessageId` the reply's
 * `header.parentMessageId`. That reversal is what makes the entry
 * reconstructible from the reply alone (see `wait_index.memberFromResume`).
 *
 * Fail-soft: a failed registration costs the liveness safety net for this one
 * call, but the dispatch itself is what the caller is actually waiting on —
 * never let bookkeeping break it.
 */
export async function registerWait(redis: Redis, params: RegisterWaitParams): Promise<void> {
    const clock = params.now ?? Date.now;
    try {
        const deadlineMs = clock() + Math.max(0, Number(params.timeoutMs) || 0);
        const member = encodeMember({
            sessionId: params.sessionId,
            parentMessageId: params.parentMessageId,
            childMessageId: params.childMessageId,
            taskGroupId: params.taskGroupId || '',
        });
        await redis.zadd(waitIndexKey(params.sessionId), deadlineMs, member);
        // A member can legitimately repeat: consecutive askUser rounds in one
        // execution all encode to the same member (no sub-task id to
        // distinguish them). Registering a wait therefore has to void the
        // previous round's "already consumed" verdict, or the gate would read
        // it as a duplicate the moment this entry goes missing.
        //
        // Deliberately after the ZADD and separately guarded: the entry is what
        // matters, and while it exists the marker is never consulted.
        //
        // Two independent commands on purpose — the ZADD targets an untagged
        // cross-entity shard key and the DEL a session-tagged key, so they
        // cannot share a Cluster slot and must never be pipelined together.
        try {
            await redis.del(consumedMarkerKey(params.sessionId, member));
        } catch (error) {
            console.warn(
                `[wait_index] Failed to clear consumed marker for wait entry `
                + `(session=${params.sessionId}, parent=${params.parentMessageId}): ${error}`
            );
        }
    } catch (error) {
        console.warn(
            `[wait_index] Failed to register wait index entry (session=${params.sessionId}, `
            + `parent=${params.parentMessageId}, child=${params.childMessageId}): ${error}`
        );
    }
}

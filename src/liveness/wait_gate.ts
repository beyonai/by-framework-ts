/**
 * Idempotency gate for replies that resume a suspended caller.
 *
 * A suspended caller is woken by exactly one `ResumeCommand`. Once a sweep can
 * *synthesize* that reply (a callee whose worker died will never send one), two
 * copies can exist for the same wait: the synthesized one and the real one that
 * shows up late. Waking the caller twice re-runs a finished execution and, in a
 * Task Group, pushes `completed` past `total` and aggregates a second time.
 *
 * The gate is the single place that decides which copy wins. It runs at the one
 * point every reply passes through — right after a reply is parsed, before any
 * registry lookup and before Task Group join accounting — and claims the
 * caller's wait-index entry with a `ZREM`. Exactly one claimant can win,
 * because `ZREM` is atomic.
 *
 * The hard part is what `ZREM` returning 0 means, since it conflates two
 * opposite situations:
 *
 * - the entry existed and someone else already claimed it — a true duplicate,
 *   drop it;
 * - the entry never existed — the reply belongs to a dispatch made before this
 *   version shipped, or to a wait whose entry expired. Dropping it silently
 *   loses a real reply, and during a rolling upgrade *every* in-flight reply
 *   looks like this.
 *
 * A short-lived "consumed" marker written by the winner separates them: a 0
 * with a marker is a duplicate, a 0 without one is unregistered and must be let
 * through. When in doubt the gate lets the message through — a spurious extra
 * wake-up is recoverable, a dropped reply is permanent silence. The same rule
 * makes the gate fail *open*: any Redis error here allows the message.
 *
 * Mirrors by-framework-python `core/wait_gate.py`.
 */

import type { Redis } from 'ioredis';
import { RegistryKeys, WAIT_CONSUMED_TTL_SECONDS } from '../constants';
import type { GatewayCommand } from '../protocol/commands';
import { encodeMember, memberDigest, memberFromResume, waitIndexKey } from './wait_index';

// Why a reply was allowed through / dropped. Carried on the decision so the
// caller can log it and put it on the orphanedReply event.
export const ALLOW_CLAIMED = 'claimed';
export const ALLOW_UNREGISTERED = 'unregistered';
export const ALLOW_GATE_ERROR = 'gate_error';
export const DENY_ALREADY_CONSUMED = 'already_consumed';

/** Outcome of the gate. */
export interface WaitGateDecision {
    /**
     * Whether the reply may be processed. False only when the wait it targets
     * is provably already resolved.
     */
    readonly allow: boolean;
    /** One of the ALLOW_ / DENY_ constants above. */
    readonly reason: string;
    /**
     * The wait-index member the decision was made about ('' when no candidate
     * matched).
     */
    readonly member: string;
}

/**
 * Redis key of the "already consumed" marker for one wait-index member.
 *
 * Part of the cross-SDK contract, since any SDK's worker may gate another SDK's
 * reply — see `wait_index.memberDigest` for why the member is hashed rather
 * than embedded.
 */
export function consumedMarkerKey(sessionId: string, member: string): string {
    return RegistryKeys.wait_consumed(sessionId, memberDigest(member));
}

/**
 * Wait-index members this reply could be clearing, most-specific first.
 *
 * Normally there is exactly one: the member rebuilt from the reply's own
 * header. The second candidate covers `askUser`, which registers with an empty
 * `childMessageId` because it has no sub-task — while the matching reply comes
 * from a client that is free to put anything in `header.parentMessageId`
 * (existing callers put the caller's own parent there, not an empty string), so
 * it cannot be rebuilt exactly.
 *
 * ORDER IS LOAD-BEARING, and so is the fact that `consumeWaitEntry` fully
 * resolves each candidate (claim, then check *its own* marker) before trying
 * the next. Reversed, a duplicate sub-agent reply — whose own entry is long
 * gone — would fall straight through to the askUser candidate and `ZREM` a wait
 * that is still live, so the user's real answer would later be dropped as
 * "already consumed" and the caller would hang forever. Caught at its own
 * candidate instead, the duplicate never reaches the askUser member.
 *
 * A reply carrying a `taskGroupId` is a sub-agent reply by construction, so the
 * askUser variant is not even considered for it.
 */
export function candidateMembers(command: GatewayCommand): string[] {
    const header = command.header;
    const members = [memberFromResume(command)];
    if (!header.taskGroupId) {
        const askUserMember = encodeMember({
            sessionId: header.sessionId,
            parentMessageId: header.messageId,
            childMessageId: '',
            taskGroupId: '',
        });
        if (!members.includes(askUserMember)) {
            members.push(askUserMember);
        }
    }
    return members;
}

/**
 * Record that this wait was resolved, so a late twin can be recognized.
 *
 * Fail-soft: losing the marker only means a much later duplicate would be
 * allowed through (one extra wake-up), which is the direction this whole module
 * errs in anyway.
 */
async function markConsumed(redis: Redis, sessionId: string, member: string): Promise<void> {
    try {
        // Deliberately its own command rather than part of a pipeline with the
        // ZREM above: the wait index is an untagged cross-entity shard key and
        // this one is session-tagged, so under Redis Cluster they resolve to
        // different slots and a shared pipeline would be a CROSSSLOT error.
        await redis.set(
            consumedMarkerKey(sessionId, member),
            '1',
            'EX',
            WAIT_CONSUMED_TTL_SECONDS
        );
    } catch (error) {
        console.warn(`[wait_gate] Failed to mark wait entry consumed (session=${sessionId}): ${error}`);
    }
}

/**
 * Claim the wait a reply resolves; report whether it may be processed.
 *
 * Call once per `ResumeCommand`, before the execution lookup and before Task
 * Group join accounting.
 */
export async function consumeWaitEntry(
    redis: Redis,
    command: GatewayCommand
): Promise<WaitGateDecision> {
    const sessionId = command.header.sessionId;
    try {
        const indexKey = waitIndexKey(sessionId);
        for (const member of candidateMembers(command)) {
            const removed = Number((await redis.zrem(indexKey, member)) || 0);
            if (removed > 0) {
                await markConsumed(redis, sessionId, member);
                return { allow: true, reason: ALLOW_CLAIMED, member };
            }
            if (await redis.exists(consumedMarkerKey(sessionId, member))) {
                return { allow: false, reason: DENY_ALREADY_CONSUMED, member };
            }
        }
        // No entry, no marker: nobody ever registered this wait (a dispatch
        // from before this version, or an entry that outlived its index).
        // Unknown is not the same as duplicate — let it through.
        return { allow: true, reason: ALLOW_UNREGISTERED, member: '' };
    } catch (error) {
        // Fail open. A gate that drops messages when Redis hiccups is worse
        // than the duplicate it was built to prevent.
        console.warn(
            `[wait_gate] Wait-index gate unavailable for session=${sessionId}, allowing reply: ${error}`
        );
        return { allow: true, reason: ALLOW_GATE_ERROR, member: '' };
    }
}

/**
 * Announce on the session data stream that a reply was dropped.
 *
 * A dropped reply is not noise: the sub-agent ran, produced a result, and may
 * have had side effects that nobody will now account for. Emitting it on the
 * existing data plane keeps it visible without inventing a second reporting
 * mechanism.
 *
 * Fail-soft by construction — the drop/allow decision has already been made,
 * and reporting it must never change or block it.
 */
export async function emitOrphanedReply(
    redis: Redis,
    command: GatewayCommand,
    options: { readonly workerId?: string; readonly reason?: string } = {}
): Promise<void> {
    const header = command.header;
    try {
        // Imported lazily for the same reason Python does: emitter.ts pulls in
        // the Redis client factory, and wait_gate is imported by the dispatch
        // path where that would be a cycle.
        const { GatewayDataEmitter } = await import('../emitter');
        const { EventType } = await import('../protocol/event_type');
        await new GatewayDataEmitter(redis).emitEvent({
            sessionId: header.sessionId,
            traceId: header.traceId,
            eventType: EventType.ORPHANED_REPLY,
            sourceAgentType: header.sourceAgentType,
            messageId: header.messageId,
            parentMessageId: header.parentMessageId,
            data: {
                reason: options.reason || DENY_ALREADY_CONSUMED,
                // The suspended caller this reply was addressed to...
                caller_message_id: header.messageId,
                // ...and the sub-task that produced it (empty for askUser).
                child_message_id: header.parentMessageId,
                task_group_id: header.taskGroupId,
                status: String((command as { status?: string }).status || ''),
                worker_id: options.workerId || '',
            },
        });
    } catch (error) {
        console.warn(
            `[wait_gate] Failed to emit orphanedReply event (session=${header.sessionId}, `
            + `message_id=${header.messageId}): ${error}`
        );
    }
}

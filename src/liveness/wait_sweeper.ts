/**
 * Resolves suspended callers whose reply is never going to arrive.
 *
 * A caller that suspends on `callAgent(waitForReply=true)` has *ended*: its
 * handler returned, the message was acked, and the only thing that can ever run
 * it again is a `ResumeCommand` landing on its agent type's control stream. If
 * the callee's worker is killed mid-task, nobody sends that command — there is
 * no timer left anywhere in the process that dispatched it. This module is that
 * missing timer, and it lives outside any single execution: the wait index
 * (`wait_index.ts`) records *who is waiting on whom*, and this sweeper walks the
 * entries that came due.
 *
 * Three properties shape the whole design:
 *
 * - **Pull, don't push.** Whether a callee is still alive is already knowable
 *   from data the system keeps anyway — its execution record and its worker's
 *   heartbeat lease. A sweep reads that evidence when a deadline expires, so the
 *   happy path pays one ZADD and one ZREM and no periodic writes at all.
 * - **The sweeper never resolves a wait itself.** It synthesizes the reply the
 *   callee would have sent and puts it on the caller's control stream, then
 *   leaves the wait-index entry alone. `wait_gate.ts` claims the entry when
 *   *some* copy of the reply is consumed. Whichever copy arrives first wins, in
 *   either order, and the loser is dropped — one accounting path, so a caller
 *   cannot be woken twice or (worse) left with its entry cleared and no reply on
 *   the way. See {@link WaitIndexSweeper.emitReply} for why the alternative —
 *   clearing the entry here — quietly needs a second, ungated delivery channel.
 * - **Deeper waits expire first, and that is load-bearing.** A callee that is
 *   itself suspended waiting on *its* callee gets renewed rather than failed.
 *   The innermost wait times out, its failure travels up hop by hop through
 *   ordinary replies, and each level fails for a reason it can report. Failing
 *   every level of a chain at once would turn one dead worker into a chain-wide
 *   outage and lose the causal chain with it.
 *
 * Compensation is deliberately narrower than cleanup:
 *
 * - `askUser` waits are **never compensated**. A human taking three days is not
 *   a fault, so there is nothing to compensate; the entry exists only so the
 *   gate can recognize a duplicate answer. They are still cleaned up once the
 *   caller is gone.
 * - A Task Group orphan is compensated by the *same* synthesized reply as any
 *   other, carrying the group id — so it is counted by the group's existing join
 *   and wakes the caller only if it is the last sibling outstanding. The sweeper
 *   never touches task_group_results / `completed` itself: a second writer of
 *   that accounting is what hangs a caller when its increment is the one that
 *   reaches `total` and no reply is left to trigger the join.
 *
 * One action is taken beyond resolving the caller: a callee that ran past its
 * renewal ceiling (`CHILD_TIMEOUT`) is asked to stop, since it is the only
 * triage outcome with a live process on the other end. That request is
 * best-effort in the strict sense — it happens *after* the reply is emitted,
 * every failure is swallowed, and nothing about the wake-up depends on it. See
 * {@link WaitIndexSweeper.cancelTimedOutChild}.
 *
 * The pass has two halves, switched separately, because only one of them decides
 * anything:
 *
 * - **Compensation** — the triage above. It synthesizes replies and changes
 *   observable behaviour, so it is opt-in and carries the rollback switch for
 *   the whole liveness feature.
 * - **Pruning** — deleting entries whose score is old enough to prove nothing
 *   can be learned from them any more. That is not a decision, and it has to run
 *   *regardless*: an entry is only ever removed by a reply or by a sweep, so
 *   with compensation off, every call whose reply never arrives leaks one entry
 *   forever — the exact failure this subsystem exists for, accumulating without
 *   bound in the structure meant to bound it. See
 *   {@link WaitIndexSweeper.pruneShard}.
 *
 * Everything here is fail-soft: a sweep that raises must never take down the
 * worker hosting it, and any uncertainty resolves toward "leave the entry alone
 * and look again next cycle".
 *
 * Mirrors by-framework-python `core/wait_sweeper.py`. The sweeper is
 * runtime-agnostic by construction — everything it reads and writes is plain
 * Redis data — so a TS sweeper resolves suspended callers created by the Python
 * and Java SDKs too, including cross-language call chains.
 */

import type { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import {
    DEFAULT_REPLY_TIMEOUT_MS,
    LivenessErrorCode,
    QueueNames,
    RegistryKeys,
    TASK_GROUP_FIELD_ABORTED,
    TASK_GROUP_FIELD_TOTAL,
    WAIT_INDEX_SHARDS,
    WAIT_PRUNE_AFTER_SECONDS,
    WAIT_PRUNE_INTERVAL_SECONDS,
    WAIT_RENEW_INCREMENT_MS,
    WAIT_RENEW_MAX_MULTIPLE,
    WAIT_RENEW_ORIGIN_TTL_SECONDS,
    WAIT_SWEEP_BATCH_LIMIT,
    WAIT_SWEEP_INTERVAL_SECONDS,
    WAIT_SWEEP_LOCK_TTL_SECONDS,
    singleCallTaskGroupId,
} from '../constants';
import { AgentState, isTerminalState } from '../protocol/agent_state';
import type { JsonValue, WireContent } from '../protocol/results';
import { WorkerRegistry, acquireScopedLock, releaseScopedLock } from '../registry';
import { WaitIndexMember, decodeMember, fnv1a32, memberDigest } from './wait_index';
import { SYNTHESIZED_BY_SWEEPER, failureReplyData, standInReply } from './wait_reply';

// Environment switches. The BY_FRAMEWORK_ prefix is deliberate even though the
// rest of this SDK reads REDIS_* / BYAI_*: these six knobs govern one subsystem
// whose state lives in Redis and is shared by the Python/TS/Java workers of a
// single deployment, so a deployment has to be able to set them once. Renaming
// them here would mean operating the same switch under two names.
export const SWEEPER_ENABLED_ENV = 'BY_FRAMEWORK_WAIT_SWEEPER_ENABLED';
export const SWEEPER_INTERVAL_ENV = 'BY_FRAMEWORK_WAIT_SWEEP_INTERVAL_SECONDS';
export const SWEEPER_RENEW_MULTIPLE_ENV = 'BY_FRAMEWORK_WAIT_RENEW_MAX_MULTIPLE';
export const SWEEPER_CANCEL_ON_TIMEOUT_ENV = 'BY_FRAMEWORK_WAIT_CANCEL_ON_TIMEOUT';
export const SWEEPER_PRUNE_ENABLED_ENV = 'BY_FRAMEWORK_WAIT_PRUNE_ENABLED';
export const SWEEPER_PRUNE_INTERVAL_ENV = 'BY_FRAMEWORK_WAIT_PRUNE_INTERVAL_SECONDS';

// What a sweep decided about one entry. Returned (and counted) so both the logs
// and the tests can assert on the triage rather than on side effects.
export const OUTCOME_MALFORMED = 'malformed';
export const OUTCOME_CALLER_MISSING = 'caller_missing';
export const OUTCOME_CALLER_TERMINAL = 'caller_terminal';
export const OUTCOME_CALLER_LOST = 'caller_lost';
export const OUTCOME_CALLER_NOT_SUSPENDED = 'caller_not_suspended';
export const OUTCOME_ASK_USER_SKIPPED = 'ask_user_skipped';
export const OUTCOME_GROUP_GONE = 'group_gone';
export const OUTCOME_GROUP_ABORTED = 'group_aborted';
export const OUTCOME_GROUP_ALREADY_JOINED = 'group_already_joined';
export const OUTCOME_CHILD_WAITING = 'child_waiting';
export const OUTCOME_CHILD_ALIVE = 'child_alive';
export const OUTCOME_RECOVERED = 'recovered';
export const OUTCOME_WORKER_LOST = 'worker_lost';
export const OUTCOME_NEVER_STARTED = 'never_started';
export const OUTCOME_TIMED_OUT = 'timed_out';
export const OUTCOME_UNROUTABLE = 'unroutable';
export const OUTCOME_ERROR = 'error';
/**
 * Not a triage outcome: counts entries deleted by the prune half of the pass,
 * which never looks at an entry's contents.
 */
export const OUTCOME_PRUNED = 'pruned';

/**
 * States that mean "this execution is parked on a reply", as opposed to queued
 * behind a worker or actively running. Written by
 * GatewayWorker.applySuspendedStatus.
 */
const SUSPENDED_STATES: ReadonlySet<string> = new Set<string>([
    AgentState.WAITING_AGENT,
    AgentState.WAITING_USER,
]);

function envInt(name: string, fallback: number): number {
    const raw = (process.env[name] || '').trim();
    if (!raw) return fallback;
    const parsed = Number.parseInt(raw, 10);
    return Number.isFinite(parsed) ? parsed : fallback;
}

function envBool(name: string, fallback: boolean): boolean {
    const raw = (process.env[name] || '').trim().toLowerCase();
    if (['1', 'true', 'yes', 'on', 'enabled'].includes(raw)) return true;
    if (['0', 'false', 'no', 'off', 'disabled'].includes(raw)) return false;
    return fallback;
}

function text(value: unknown): string {
    if (value === null || value === undefined) return '';
    return String(value);
}

/** Best-effort epoch-ms parse; 0 for anything unusable. */
function asInt(value: unknown): number {
    const parsed = Number(text(value) || 0);
    return Number.isFinite(parsed) ? Math.trunc(parsed) : 0;
}

/**
 * Strip the `": reason"` suffix some statuses are decorated with.
 *
 * A caller parked on a Task Group persists as `"WAITING_AGENT: waiting_for_group"`,
 * and an aborted group's reply produces `"CANCELLED: group_aborted"`. Comparing
 * those raw makes a suspended caller look running and a cancelled one look live,
 * so every status this module branches on goes through here first.
 */
export function baseStatus(value: unknown): string {
    return text(value).split(':')[0].trim();
}

/**
 * One wait-index entry that came due, as this pass found it.
 *
 * `deadlineMs` is the score the entry carried when it was read — the caller's
 * original deadline until a renewal overwrites it, which is why the first
 * renewal is what records the renewal budget's origin.
 */
interface DueEntry {
    readonly indexKey: string;
    readonly member: string;
    readonly deadlineMs: number;
    readonly entry: WaitIndexMember;
}

/** Delivers a cancellation to a timed-out callee. Injectable for tests. */
export type CancelChildFn = (params: {
    readonly messageId: string;
    readonly sessionId: string;
    readonly reason: string;
    readonly requestedBy: string;
}) => Promise<unknown>;

export interface WaitIndexSweeperOptions {
    readonly workerId?: string;
    readonly registry?: WorkerRegistry;
    readonly intervalSeconds?: number;
    readonly enabled?: boolean;
    readonly renewMaxMultiple?: number;
    readonly cancelOnTimeout?: boolean;
    readonly pruneEnabled?: boolean;
    readonly pruneIntervalSeconds?: number;
    /** Injectable clock (epoch ms), for tests. */
    readonly now?: () => number;
    /** Injectable cancellation delivery; defaults to GatewayClient.cancelTask. */
    readonly cancelChild?: CancelChildFn;
}

export class WaitIndexSweeper {
    private readonly redis: Redis;
    readonly workerId: string;
    private readonly registry: WorkerRegistry;
    readonly intervalSeconds: number;
    readonly enabled: boolean;
    readonly pruneEnabled: boolean;
    readonly pruneIntervalSeconds: number;
    readonly renewMaxMultiple: number;
    readonly cancelOnTimeout: boolean;
    private readonly lockTtlSeconds: number;
    private readonly now: () => number;
    private readonly cancelChild?: CancelChildFn;
    private shardCursor: number;
    /** null means "never pruned in this process", so the first pass does. */
    private lastPruneMs: number | null = null;
    private loopTask: Promise<void> | null = null;
    private running = false;
    private wake: (() => void) | null = null;

    constructor(redis: Redis, options: WaitIndexSweeperOptions = {}) {
        this.redis = redis;
        this.workerId = options.workerId || `sweeper-${uuidv4().slice(0, 8)}`;
        this.registry = options.registry || new WorkerRegistry(redis);
        this.now = options.now || (() => Date.now());
        this.cancelChild = options.cancelChild;
        this.intervalSeconds = options.intervalSeconds
            ?? envInt(SWEEPER_INTERVAL_ENV, WAIT_SWEEP_INTERVAL_SECONDS);
        // Compensation is off by default: it is the first thing in the liveness
        // chain that changes observable behaviour, so it carries the rollback
        // switch for all of it.
        this.enabled = options.enabled ?? envBool(SWEEPER_ENABLED_ENV, false);
        // Pruning is on by default and cannot ride on the switch above — see the
        // module comment and pruneShard().
        this.pruneEnabled = options.pruneEnabled ?? envBool(SWEEPER_PRUNE_ENABLED_ENV, true);
        this.pruneIntervalSeconds = Math.max(
            1,
            options.pruneIntervalSeconds
            ?? envInt(SWEEPER_PRUNE_INTERVAL_ENV, WAIT_PRUNE_INTERVAL_SECONDS)
        );
        // Never below 1: a multiple of 1 means "the deadline is the ceiling",
        // i.e. no renewal ever happens and every callee that is merely slow is
        // killed at its first deadline.
        this.renewMaxMultiple = Math.max(
            1,
            options.renewMaxMultiple ?? envInt(SWEEPER_RENEW_MULTIPLE_ENV, WAIT_RENEW_MAX_MULTIPLE)
        );
        // Deployment-wide, not per call. See cancelTimedOutChild for why this
        // knob cannot be a callAgent argument without paying for it on every
        // happy-path dispatch.
        this.cancelOnTimeout = options.cancelOnTimeout
            ?? envBool(SWEEPER_CANCEL_ON_TIMEOUT_ENV, true);
        this.lockTtlSeconds = Math.max(this.intervalSeconds * 3, WAIT_SWEEP_LOCK_TTL_SECONDS);
        // Start each worker at a different shard so a fleet spreads out instead
        // of every member queuing on shard 0's lock every cycle.
        this.shardCursor = fnv1a32(this.workerId) % WAIT_INDEX_SHARDS;
    }

    /**
     * How long the background loop sleeps between passes.
     *
     * Compensation is what needs a short cadence (a due entry should not wait
     * long for its triage). With it off, the only work left is a garbage
     * collector with a multi-day horizon, so the loop drops to the prune cadence
     * rather than waking every 30 seconds to do nothing.
     */
    get loopIntervalSeconds(): number {
        return this.enabled ? this.intervalSeconds : this.pruneIntervalSeconds;
    }

    /**
     * Start the background pass. No-op when both halves are switched off.
     *
     * Deliberately not `async`: the loop outlives the call, exactly like
     * WorkerRunner's control loop, and `stop()` is what awaits it.
     */
    start(): void {
        if (this.loopTask) return;
        if (!this.enabled && !this.pruneEnabled) {
            console.log(`[${this.workerId}] WaitIndexSweeper fully disabled, not starting.`);
            return;
        }
        console.log(
            `[${this.workerId}] WaitIndexSweeper started `
            + `(interval=${this.loopIntervalSeconds}s, compensate=${this.enabled}, `
            + `prune=${this.pruneEnabled})`
        );
        this.running = true;
        this.loopTask = (async () => {
            while (this.running) {
                try {
                    await this.sweepOnce();
                } catch (error) {
                    // Unreachable in practice (sweepShard swallows), but a sweep
                    // must never be able to kill the worker hosting it.
                    console.warn(`[${this.workerId}] Wait sweep pass failed: ${error}`);
                }
                if (!this.running) break;
                await this.sleep(this.loopIntervalSeconds * 1000);
            }
        })();
    }

    /** Stop the background pass and wait for the in-flight one to finish. */
    async stop(): Promise<void> {
        this.running = false;
        // Cancel the pending sleep rather than waiting it out: an interval is up
        // to an hour when only pruning is on, and a shutdown must not hang on it.
        if (this.wake) this.wake();
        const task = this.loopTask;
        this.loopTask = null;
        if (task) {
            await task.catch(() => undefined);
        }
    }

    /** Interruptible sleep; the timer is cleared so it cannot hold the process open. */
    private sleep(ms: number): Promise<void> {
        return new Promise<void>((resolve) => {
            const timer = setTimeout(() => {
                this.wake = null;
                resolve();
            }, ms);
            // Never let the sweeper's idle timer be the reason a process stays
            // alive; the runner's loop owns liveness.
            if (typeof timer.unref === 'function') timer.unref();
            this.wake = () => {
                clearTimeout(timer);
                this.wake = null;
                resolve();
            };
        });
    }

    /**
     * Run one pass over every shard this worker can claim right now.
     *
     * Returns a count per triage outcome, which is also what makes the pass
     * observable in tests.
     */
    async sweepOnce(): Promise<Record<string, number>> {
        const outcomes: Record<string, number> = {};
        const pruning = this.pruneIsDue();
        const start = this.shardCursor;
        for (let offset = 0; offset < WAIT_INDEX_SHARDS; offset += 1) {
            const shard = (start + offset) % WAIT_INDEX_SHARDS;
            const shardOutcomes = await this.sweepShard(shard, pruning);
            for (const [outcome, count] of Object.entries(shardOutcomes)) {
                outcomes[outcome] = (outcomes[outcome] || 0) + count;
            }
        }
        // Rotate so the shard a busy worker starts on (and therefore claims
        // first) differs each cycle; nothing is pinned to one owner.
        this.shardCursor = (start + 1) % WAIT_INDEX_SHARDS;
        return outcomes;
    }

    /**
     * Whether this pass should prune, on the coarser prune cadence.
     *
     * Wall clock rather than Python's `time.monotonic()`, because this is the
     * one injectable clock the tests drive and the stake is small: a backwards
     * clock jump delays or repeats one garbage-collection pass, and the pass
     * itself is idempotent.
     */
    private pruneIsDue(): boolean {
        if (!this.pruneEnabled) return false;
        const now = this.now();
        const last = this.lastPruneMs;
        if (last !== null && now - last < this.pruneIntervalSeconds * 1000) {
            return false;
        }
        this.lastPruneMs = now;
        return true;
    }

    /**
     * Claim one shard, prune it and/or resolve its due entries.
     *
     * The lock is taken for both halves even though pruning is idempotent
     * (ZREMRANGEBYSCORE over a fixed range yields the same state no matter how
     * many workers run it): holding it just keeps a fleet from issuing the same
     * command N times a cycle. Correctness does not depend on it, which is why a
     * lost or expired claim costs nothing here.
     */
    private async sweepShard(shard: number, pruning: boolean): Promise<Record<string, number>> {
        if (!pruning && !this.enabled) return {};
        const lockKey = RegistryKeys.wait_sweep_lock(shard);
        const token = uuidv4().replace(/-/g, '');
        let claimed = false;
        try {
            claimed = await acquireScopedLock(this.redis, lockKey, token, this.lockTtlSeconds);
        } catch (error) {
            console.warn(`[${this.workerId}] Wait sweep could not claim shard ${shard}: ${error}`);
            return {};
        }
        if (!claimed) return {};
        const outcomes: Record<string, number> = {};
        try {
            if (pruning) {
                const pruned = await this.pruneShard(shard);
                if (pruned) outcomes[OUTCOME_PRUNED] = pruned;
            }
            if (this.enabled) {
                for (const [outcome, count] of Object.entries(await this.resolveDueEntries(shard))) {
                    outcomes[outcome] = (outcomes[outcome] || 0) + count;
                }
            }
            return outcomes;
        } catch (error) {
            // A sweep is a safety net; it must never be able to take down the
            // worker that hosts it.
            console.warn(`[${this.workerId}] Wait sweep failed on shard ${shard}: ${error}`);
            outcomes[OUTCOME_ERROR] = (outcomes[OUTCOME_ERROR] || 0) + 1;
            return outcomes;
        } finally {
            try {
                await releaseScopedLock(this.redis, lockKey, token);
            } catch (error) {
                console.debug(`[${this.workerId}] Wait sweep lock release failed (shard ${shard}): ${error}`);
            }
        }
    }

    /**
     * Delete entries old enough that no interrogation could succeed.
     *
     * Runs whether or not compensation does, and that is the point. Nothing else
     * ever removes a wait-index entry except the reply it was waiting for, so
     * with compensation off every call whose reply never arrives — precisely the
     * failures this subsystem exists for — leaves an entry behind forever, in a
     * structure that has no TTL of its own (the shard ZSET is shared by every
     * session, so it cannot carry one).
     *
     * Unlike the triage, this reads nothing: no member is decoded, no execution
     * record is fetched, no reply is produced. It is one ZREMRANGEBYSCORE over a
     * range whose upper bound is a *proof* rather than a guess. Every writer of
     * an entry sets its score to its own clock plus a non-negative offset —
     * registration adds the caller's timeout, extendDeadline adds
     * WAIT_RENEW_INCREMENT_MS — and only ever writes while the caller's execution
     * record exists. So a score more than WAIT_PRUNE_AFTER_SECONDS in the past
     * means the entry was last touched longer than a session TTL ago, hence its
     * session registry is gone and the only outcome triage could ever reach for
     * it is "caller missing", which deletes it anyway. That holds for renewed
     * entries too: a renewal *raises* the score, so an old score is evidence
     * about the most recent renewal, not about registration.
     *
     * Because it decides nothing, it needs no opt-in — and because the threshold
     * sits a day beyond DEFAULT_SESSION_TTL, the longest deadline anything
     * registers (DEFAULT_ASK_USER_TIMEOUT_MS, which equals the session TTL
     * exactly) is never near it. Trimming the threshold to the session TTL
     * instead would put it exactly on the boundary of a live askUser wait, and
     * lose to any clock skew between the worker that registered the entry and
     * the one sweeping it.
     */
    private async pruneShard(shard: number): Promise<number> {
        const cutoffMs = this.now() - WAIT_PRUNE_AFTER_SECONDS * 1000;
        const removed = Number(
            (await this.redis.zremrangebyscore(RegistryKeys.wait_index(shard), 0, cutoffMs)) || 0
        );
        if (removed) {
            console.log(
                `[${this.workerId}] Wait sweep pruned ${removed} abandoned wait-index `
                + `entr${removed === 1 ? 'y' : 'ies'} from shard ${shard} `
                + `(older than ${WAIT_PRUNE_AFTER_SECONDS}s)`
            );
        }
        return removed;
    }

    private async resolveDueEntries(shard: number): Promise<Record<string, number>> {
        const indexKey = RegistryKeys.wait_index(shard);
        const nowMs = this.now();
        // Scores come back with the members because the score *is* the evidence
        // for how long this wait has already run: it is the caller's original
        // deadline until the first renewal replaces it, and that original is what
        // the renewal budget is measured from.
        const due = (await this.redis.zrangebyscore(
            indexKey,
            0,
            nowMs,
            'WITHSCORES',
            'LIMIT',
            0,
            WAIT_SWEEP_BATCH_LIMIT
        )) as string[] | null;
        const outcomes: Record<string, number> = {};
        for (let i = 0; i + 1 < (due || []).length; i += 2) {
            const member = text(due![i]);
            let outcome: string;
            try {
                outcome = await this.resolveEntry(indexKey, member, due![i + 1]);
            } catch (error) {
                // One poisonous entry must not stop the rest of the shard.
                console.warn(
                    `[${this.workerId}] Wait sweep could not resolve entry `
                    + `${JSON.stringify(member)}: ${error}`
                );
                outcome = OUTCOME_ERROR;
            }
            outcomes[outcome] = (outcomes[outcome] || 0) + 1;
        }
        return outcomes;
    }

    /**
     * Triage one due entry against evidence that already exists.
     *
     * Ordering is deliberate: everything that establishes *there is still
     * somebody waiting* is checked before anything that could produce a reply. A
     * reply nobody is waiting for is not free — it re-enters a finished
     * execution.
     */
    private async resolveEntry(
        indexKey: string,
        member: string,
        rawScore: unknown
    ): Promise<string> {
        let entry: WaitIndexMember;
        try {
            entry = decodeMember(member);
        } catch (error) {
            console.warn(
                `[${this.workerId}] Dropping malformed wait-index member `
                + `${JSON.stringify(member)}: ${error}`
            );
            await this.redis.zrem(indexKey, member);
            return OUTCOME_MALFORMED;
        }

        const due: DueEntry = { indexKey, member, deadlineMs: asInt(rawScore), entry };
        const caller = await this.registry.getExecutionByMessageId(
            entry.parentMessageId,
            entry.sessionId
        );
        if (!caller) {
            // The session registry entry expired out from under it: no reply,
            // synthesized or real, can reattach this caller any more.
            console.log(
                `[${this.workerId}] Wait sweep dropping entry for a caller with no execution `
                + `record (session=${entry.sessionId}, caller=${entry.parentMessageId})`
            );
            await this.redis.zrem(indexKey, member);
            return OUTCOME_CALLER_MISSING;
        }

        const callerStatus = baseStatus(caller.status);
        if (isTerminalState(callerStatus)) {
            // Reachable through a narrow but real window: the entry is
            // registered before the dispatch xadd, so an xadd that throws fails
            // the caller and leaves the entry behind. Waking a finished
            // execution is exactly what the idempotency work exists to prevent,
            // so clean up and synthesize nothing.
            console.log(
                `[${this.workerId}] Wait sweep dropping entry for an already-${callerStatus} `
                + `caller (session=${entry.sessionId}, caller=${entry.parentMessageId})`
            );
            await this.redis.zrem(indexKey, member);
            return OUTCOME_CALLER_TERMINAL;
        }

        if (!SUSPENDED_STATES.has(callerStatus)) {
            return this.resolveUnsuspendedCaller(due, caller);
        }

        if (!entry.childMessageId) {
            // askUser. "The human hasn't answered yet" is not a fault and has no
            // compensation; the entry stays purely so a repeated answer is
            // recognized as a duplicate. Note this sits *below* the caller
            // checks above on purpose: an askUser entry whose caller is gone or
            // finished is still cleaned up, or the index would grow without
            // bound in exactly the deployments that use askUser most.
            return OUTCOME_ASK_USER_SKIPPED;
        }

        if (entry.taskGroupId) {
            const blocked = await this.groupBlocksCompensation(due);
            if (blocked !== null) return blocked;
        }

        return this.triageChild(due, caller);
    }

    /**
     * Reasons a Task Group orphan must be cleaned up, not compensated.
     *
     * A group orphan is otherwise compensated exactly like any other: the
     * synthesized reply carries the group id, so GatewayWorker's join stores it,
     * increments `completed`, and aggregates only if it was the last sibling
     * outstanding. That is the whole point of routing it as a reply — writing the
     * result and the counter from here would be a second implementation of the
     * group's accounting, and when *that* copy is the increment reaching `total`
     * there is no reply left to trigger the join and the caller hangs forever.
     * (Same shape as the dispatch-time double-accounting bug this repo has
     * already shipped once.)
     *
     * Returns an outcome when the entry was resolved here, or null to let the
     * normal triage run.
     */
    private async groupBlocksCompensation(due: DueEntry): Promise<string | null> {
        const entry = due.entry;
        const groupKey = QueueNames.task_group(entry.taskGroupId);
        const total = await this.redis.hget(groupKey, TASK_GROUP_FIELD_TOTAL);
        if (total === null) {
            // The group tracker expired (TASK_GROUP_TTL_SECONDS) or was never
            // written. A reply then finds no group to join, so it would fall
            // through as a lone result and resume the caller with one sibling's
            // payload where the aggregate belongs.
            console.log(
                `[${this.workerId}] Wait sweep dropping entry whose task group no longer exists `
                + `(session=${entry.sessionId}, caller=${entry.parentMessageId}, `
                + `group=${entry.taskGroupId})`
            );
            await this.redis.zrem(due.indexKey, due.member);
            return OUTCOME_GROUP_GONE;
        }

        if (await this.redis.hget(groupKey, TASK_GROUP_FIELD_ABORTED)) {
            // Dispatch failed partway through the fan-out, so the caller was
            // already failed and every reply for this group is discarded on
            // arrival. Synthesizing one more changes nothing and re-enters a
            // terminated execution on the way to being discarded.
            console.log(
                `[${this.workerId}] Wait sweep dropping entry for aborted task group `
                + `(session=${entry.sessionId}, caller=${entry.parentMessageId}, `
                + `group=${entry.taskGroupId})`
            );
            await this.redis.zrem(due.indexKey, due.member);
            return OUTCOME_GROUP_ABORTED;
        }

        const recorded = await this.redis.hget(
            QueueNames.task_group_results(entry.taskGroupId),
            entry.childMessageId
        );
        if (recorded !== null) {
            // A result under this sub-task's id can only have been written by
            // the join, which means its reply already arrived and was counted.
            // The entry outliving that is a gate ZREM that did not land; a second
            // synthesized reply would be counted a second time.
            console.log(
                `[${this.workerId}] Wait sweep dropping entry already joined by its reply `
                + `(session=${entry.sessionId}, caller=${entry.parentMessageId}, `
                + `child=${entry.childMessageId}, group=${entry.taskGroupId})`
            );
            await this.redis.zrem(due.indexKey, due.member);
            return OUTCOME_GROUP_ALREADY_JOINED;
        }

        return null;
    }

    /**
     * Handle an entry whose caller is not (yet, or ever) suspended.
     *
     * Two very different situations share this shape. The caller may still be
     * inside the handler that registered the wait, in which case a reply now
     * would run alongside it — so back off and look again. Or the caller's own
     * worker died before it could record its suspension, in which case no reply
     * can ever reattach it and the entry is garbage; that chain gets rescued one
     * level up, by the wait its own caller registered.
     */
    private async resolveUnsuspendedCaller(
        due: DueEntry,
        caller: Record<string, any>
    ): Promise<string> {
        const callerWorkerId = text(caller.worker_id);
        if (callerWorkerId && !(await this.registry.isWorkerOnline(callerWorkerId))) {
            console.log(
                `[${this.workerId}] Wait sweep dropping entry whose caller was lost with worker `
                + `${callerWorkerId} (caller=${text(caller.message_id)}, `
                + `status=${baseStatus(caller.status)})`
            );
            await this.redis.zrem(due.indexKey, due.member);
            return OUTCOME_CALLER_LOST;
        }
        await this.extendDeadline(due);
        return OUTCOME_CALLER_NOT_SUSPENDED;
    }

    /** Decide the callee's fate from its execution record and its lease. */
    private async triageChild(due: DueEntry, caller: Record<string, any>): Promise<string> {
        const entry = due.entry;
        const child = await this.registry.getExecutionByMessageId(
            entry.childMessageId,
            entry.sessionId
        );
        if (!child) {
            return this.synthesizeFailure(due, caller, {
                child: {},
                errorCode: LivenessErrorCode.CHILD_NEVER_STARTED,
                message: `No execution was ever recorded for sub-task ${entry.childMessageId}`,
                outcome: OUTCOME_NEVER_STARTED,
            });
        }

        const childStatus = baseStatus(child.status);
        if (isTerminalState(childStatus)) {
            return this.recoverFinishedChild(due, caller, child);
        }

        if (SUSPENDED_STATES.has(childStatus)) {
            // The callee is itself waiting on someone. Its own entry has a
            // deadline of its own and will fail first if that wait breaks;
            // killing this one now would collapse the whole chain at once and
            // report the wrong cause at every level.
            //
            // Deliberately exempt from the renewal ceiling below: this wait was
            // registered *before* the deeper one it is blocked on, so its ceiling
            // would be reached first and the chain would fail from the top down —
            // inverting the propagation order the whole design rests on. The
            // chain still terminates, because the deepest wait is blocked on real
            // work and is subject to the ceiling.
            await this.extendDeadline(due);
            return OUTCOME_CHILD_WAITING;
        }

        const childWorkerId = text(child.worker_id);
        if (!childWorkerId) {
            return this.synthesizeFailure(due, caller, {
                child,
                errorCode: LivenessErrorCode.CHILD_NEVER_STARTED,
                message: `Sub-task ${entry.childMessageId} was never picked up by a worker `
                    + `(status=${childStatus})`,
                outcome: OUTCOME_NEVER_STARTED,
            });
        }

        if (await this.registry.isWorkerOnline(childWorkerId)) {
            // Running long is not the same as being dead, and the lease is the
            // only signal that tells them apart — so a live lease buys more time,
            // which is what keeps slow work from being killed.
            //
            // But only up to a ceiling. Renewing on a live lease alone answers
            // "is the process up", not "is the work progressing", so a callee
            // deadlocked or stuck in a call that never returns would be renewed
            // forever and its caller would never be resolved — precisely the hang
            // this subsystem exists to bound. There is no signal that separates
            // that from a genuinely long call (both sit still), so the ceiling is
            // deliberately crude: generous, absolute, and therefore predictable.
            const limitMs = await this.renewalCeilingMs(due, child);
            if (this.now() < limitMs) {
                await this.extendDeadline(due);
                return OUTCOME_CHILD_ALIVE;
            }
            const outcome = await this.synthesizeFailure(due, caller, {
                child,
                errorCode: LivenessErrorCode.CHILD_TIMEOUT,
                message: `Sub-task ${entry.childMessageId} is still ${childStatus} on live worker `
                    + `${childWorkerId} but produced no reply within ${this.renewMaxMultiple}x its `
                    + `reply timeout`,
                outcome: OUTCOME_TIMED_OUT,
            });
            // Strictly after the caller has been resolved, and strictly
            // best-effort: this is the one branch with a live process on the
            // other end, so it is the only one where stopping the work is even
            // meaningful — and the caller's wake-up must not depend on whether it
            // lands.
            await this.cancelTimedOutChild(due, child);
            return outcome;
        }

        return this.synthesizeFailure(due, caller, {
            child,
            errorCode: LivenessErrorCode.CHILD_WORKER_LOST,
            message: `Worker ${childWorkerId} running sub-task ${entry.childMessageId} is no longer `
                + `alive (status=${childStatus})`,
            outcome: OUTCOME_WORKER_LOST,
        });
    }

    /**
     * Stop a callee that ran past its ceiling. Best-effort, by nature.
     *
     * Only CHILD_TIMEOUT gets here. CHILD_WORKER_LOST and CHILD_NEVER_STARTED
     * have nothing on the other end to cancel, and a callee that already finished
     * has nothing left to stop.
     *
     * The whole path is delegated to `GatewayClient.cancelTask`, which already
     * gets the two hard parts right: it delivers to
     * `QueueNames.worker_ctrl_stream(workerId)` — WorkerRunner's cancel handling
     * looks the execution up in its *own worker's in-memory* map, so a cancel put
     * on the agent type's competitive stream is claimed by an arbitrary worker,
     * finds nothing, and cancels nothing while still recording that it did — and
     * it walks the sub-tree, so a callee's own callees stop too.
     *
     * Two things it deliberately does not do:
     *
     * - **It does not silence the callee.** GatewayWorker's TaskCancelledError
     *   branch still sends a CANCELLED reply. That copy is dropped by the
     *   idempotency gate, which is why cancellation can never be a substitute for
     *   the synthesized reply.
     * - **It does not affect the caller.** Cancellation is cooperative, and the
     *   archetypal CHILD_TIMEOUT is a callee wedged in a blocking call, i.e.
     *   exactly the case where it cannot land. The synthesized reply has already
     *   gone out above; every failure here is swallowed.
     *
     * The opt-out (BY_FRAMEWORK_WAIT_CANCEL_ON_TIMEOUT) is per deployment rather
     * than per callAgent. A per-call flag has to reach a sweeper that only sees
     * the wait-index member, and the member is a cross-SDK wire format that must
     * stay rebuildable from a reply alone — so it would need a side key written
     * on *every* dispatch to serve a decision taken only after a timeout, which
     * is the periodic happy-path cost this whole design avoids. A knob whose
     * blast radius is "a timed-out callee keeps burning CPU" does not earn that.
     */
    private async cancelTimedOutChild(due: DueEntry, child: Record<string, any>): Promise<boolean> {
        if (!this.cancelOnTimeout) return false;
        const childMessageId = due.entry.childMessageId;
        if (child.cancel_requested) {
            // A previous sweep already asked. Repeating it every renewal window
            // adds messages, not cancellation.
            return false;
        }
        try {
            const cancel = this.cancelChild || (await this.defaultCancelChild());
            const response = (await cancel({
                messageId: childMessageId,
                sessionId: due.entry.sessionId,
                reason: `${LivenessErrorCode.CHILD_TIMEOUT}: no reply before deadline`,
                requestedBy: `wait_sweeper:${this.workerId}`,
            })) as { status?: string; success?: boolean } | undefined;
            console.log(
                `[${this.workerId}] Wait sweep requested cancellation of timed-out sub-task `
                + `${childMessageId} (session=${due.entry.sessionId}): ${response?.status || ''}`
            );
            return Boolean(response?.success);
        } catch (error) {
            console.warn(
                `[${this.workerId}] Wait sweep could not cancel timed-out sub-task `
                + `${childMessageId} (session=${due.entry.sessionId}): ${error}. `
                + `The caller was resolved regardless.`
            );
            return false;
        }
    }

    /**
     * GatewayClient.cancelTask, imported lazily.
     *
     * Lazy for the same reason Python's is: client.ts pulls in the dispatch
     * pipeline, which imports this subsystem's registration side, so a top-level
     * import here would be a cycle.
     */
    private async defaultCancelChild(): Promise<CancelChildFn> {
        const { GatewayClient } = await import('../client');
        const client = new GatewayClient(this.registry, this.redis);
        return (params) => client.cancelTask({
            messageId: params.messageId,
            sessionId: params.sessionId,
            reason: params.reason,
            requestedBy: params.requestedBy,
        });
    }

    /**
     * Absolute instant past which this wait stops being renewed.
     *
     * Measured from the wait's *original* deadline, not from a renewal count:
     * renewals happen at a fixed increment that is a tunable, so counting them
     * would let a config change silently move the bound. The original deadline
     * survives renewals in RegistryKeys.wait_renew_origin (written by the first
     * renewal, see extendDeadline); before the first renewal the score still is
     * it.
     *
     * The caller's own timeout is recovered as the span between the sub-task's
     * `created_at` — written by initializeExecution immediately before the wait
     * is registered — and that original deadline, so a caller that asked for ten
     * minutes is not held to the same budget as one that asked for four hours.
     * When that span is unusable the default timeout is assumed, erring toward
     * waiting longer: killing a healthy callee is worse than resolving a dead one
     * late.
     *
     * Progress is deliberately *not* used as evidence. A callee's `updated_at`
     * stands just as still during a legitimate 20-minute model call as during a
     * deadlock, so a ceiling keyed on it would kill exactly the work it is meant
     * to protect.
     */
    private async renewalCeilingMs(due: DueEntry, child: Record<string, any>): Promise<number> {
        const originMs = await this.renewalOriginMs(due);
        const registeredMs = asInt(child.created_at);
        let timeoutMs = originMs - registeredMs;
        if (registeredMs <= 0 || timeoutMs <= 0) {
            timeoutMs = DEFAULT_REPLY_TIMEOUT_MS;
        }
        // One renewal increment is the floor so a degenerate (zero, or very
        // short) timeout still buys the callee one look.
        const graceMs = Math.max(timeoutMs * (this.renewMaxMultiple - 1), WAIT_RENEW_INCREMENT_MS);
        return originMs + graceMs;
    }

    /** The deadline this wait's renewal budget is measured from. */
    private async renewalOriginMs(due: DueEntry): Promise<number> {
        let stored: string | null = null;
        try {
            stored = await this.redis.get(
                RegistryKeys.wait_renew_origin(due.entry.sessionId, memberDigest(due.member))
            );
        } catch (error) {
            console.debug(`[${this.workerId}] Wait sweep could not read renewal origin: ${error}`);
        }
        // Nothing recorded yet means nothing has renewed this entry yet, so the
        // score it came due with still is the original deadline.
        return asInt(stored) || due.deadlineMs;
    }

    /**
     * Rebuild the reply of a callee that finished but never got heard.
     *
     * The callee stores its result before it sends the reply (see
     * GatewayWorker.persistSingleCallResult), precisely so this case loses a
     * *message* rather than an *answer*. When the stored result is there the
     * caller gets the real thing and never learns a message was lost, beyond a
     * marker on the metadata.
     *
     * When it is not there, the honest outcome is a failure. Reporting an empty
     * COMPLETED would hand the caller a fabricated answer, which is the one
     * outcome worse than a reported failure.
     */
    private async recoverFinishedChild(
        due: DueEntry,
        caller: Record<string, any>,
        child: Record<string, any>
    ): Promise<string> {
        const entry = due.entry;
        const stored = await this.loadStoredResult(entry.childMessageId);
        const childStatus = baseStatus(child.status);
        if (!stored) {
            return this.synthesizeFailure(due, caller, {
                child,
                errorCode: LivenessErrorCode.REPLY_LOST_RECOVERED,
                message: `Sub-task ${entry.childMessageId} finished with ${childStatus} but neither `
                    + `its reply nor its stored result is available`,
                outcome: OUTCOME_RECOVERED,
                status: childStatus === AgentState.FAILED || childStatus === AgentState.CANCELLED
                    ? childStatus
                    : AgentState.FAILED,
            });
        }

        await this.emitReply(due, caller, {
            child,
            status: text(stored.status) || childStatus,
            content: (stored.content as WireContent) || '',
            replyData: (stored.reply_data ?? null) as JsonValue,
            extraPayload: (stored.extra_payload || {}) as Record<string, JsonValue>,
            errorCode: LivenessErrorCode.REPLY_LOST_RECOVERED,
            metadata: (stored.metadata || {}) as Record<string, JsonValue>,
        });
        return OUTCOME_RECOVERED;
    }

    /** Read the callee's persisted result, or null if there is none. */
    private async loadStoredResult(childMessageId: string): Promise<Record<string, any> | null> {
        const resultsKey = QueueNames.task_group_results(singleCallTaskGroupId(childMessageId));
        const raw = await this.redis.hget(resultsKey, childMessageId);
        if (!raw) return null;
        try {
            const decoded = JSON.parse(text(raw));
            return decoded && typeof decoded === 'object' && !Array.isArray(decoded) ? decoded : null;
        } catch (error) {
            console.warn(
                `[${this.workerId}] Stored result for sub-task ${childMessageId} is unreadable: ${error}`
            );
            return null;
        }
    }

    /**
     * Emit the failure reply the callee would have sent, had it lived.
     *
     * `replyData` carries error/error_code because that is where a dispatch-time
     * failure already puts them: a caller must not be able to tell a callee that
     * failed from one that never got to fail, or the two shapes drift apart and
     * callers grow a second error path.
     */
    private async synthesizeFailure(
        due: DueEntry,
        caller: Record<string, any>,
        params: {
            readonly child: Record<string, any>;
            readonly errorCode: string;
            readonly message: string;
            readonly outcome: string;
            readonly status?: string;
        }
    ): Promise<string> {
        const entry = due.entry;
        console.warn(
            `[${this.workerId}] Wait sweep synthesizing ${params.errorCode} reply for `
            + `caller=${entry.parentMessageId} (child=${entry.childMessageId}, `
            + `session=${entry.sessionId}): ${params.message}`
        );
        const emitted = await this.emitReply(due, caller, {
            child: params.child,
            status: params.status || AgentState.FAILED,
            content: '',
            replyData: failureReplyData({
                error: params.message,
                errorCode: params.errorCode,
                childMessageId: entry.childMessageId,
            }),
            extraPayload: {},
            errorCode: params.errorCode,
            metadata: {},
        });
        return emitted ? params.outcome : OUTCOME_UNROUTABLE;
    }

    /**
     * Put a stand-in reply on the caller's control stream.
     *
     * Field-for-field it is what GatewayWorker.enqueueAgentReturn produces —
     * built by the shared standInReply() for that reason, since two independent
     * constructions of the same reply drift invisibly until a caller hangs. Same
     * id reversal (header.messageId is the caller's, header.parentMessageId the
     * sub-task's), same stream. That is not tidiness: the runner reattaches the
     * suspended execution by header.messageId, and the gate rebuilds the
     * wait-index member from the header, so anything else either fails to resume
     * the caller or fails to clear its entry.
     *
     * The wait-index entry is left in place on purpose. Removing it here would
     * leave the synthesized reply as the only copy that must not be gated — and a
     * reply that bypasses the gate is a second wake-up path, which is what makes
     * double-resumes possible in the first place. Instead the deadline is pushed
     * out, so a caller whose control stream is not being consumed gets at most
     * one stand-in per renewal window instead of one per sweep.
     *
     * A reply for a Task Group member carries the group id like any other, so it
     * is stored and counted by the group's existing join and resolves the caller
     * only when it is the last sibling outstanding. Booking the result here
     * instead would be a second writer of that accounting, and the copy that
     * reaches `total` would leave no reply to run the join.
     */
    private async emitReply(
        due: DueEntry,
        caller: Record<string, any>,
        params: {
            readonly child: Record<string, any>;
            readonly status: string;
            readonly content: WireContent;
            readonly replyData: JsonValue;
            readonly extraPayload: Record<string, JsonValue>;
            readonly errorCode: string;
            readonly metadata: Record<string, JsonValue>;
        }
    ): Promise<boolean> {
        const entry = due.entry;
        const callerAgentType = text(params.child.source_agent_type)
            || text(caller.target_agent_type);
        if (!callerAgentType) {
            console.warn(
                `[${this.workerId}] Wait sweep cannot route a reply for `
                + `caller=${entry.parentMessageId} (session=${entry.sessionId}): the execution `
                + `records name no caller agent type`
            );
            await this.extendDeadline(due);
            return false;
        }

        const command = standInReply({
            sessionId: entry.sessionId,
            callerMessageId: entry.parentMessageId,
            callerAgentType,
            childMessageId: entry.childMessageId,
            childAgentType: text(params.child.target_agent_type),
            taskGroupId: entry.taskGroupId,
            traceId: text(params.child.trace_id) || text(caller.trace_id),
            status: params.status,
            content: params.content,
            replyData: params.replyData,
            extraPayload: params.extraPayload,
            metadata: { ...params.metadata, sweeper_worker_id: this.workerId },
            errorCode: params.errorCode,
            synthesizedBy: SYNTHESIZED_BY_SWEEPER,
        });
        await this.redis.xadd(
            QueueNames.ctrl_stream(callerAgentType),
            '*',
            'data',
            JSON.stringify(command.toDict())
        );
        await this.extendDeadline(due);
        return true;
    }

    /**
     * Push an entry's deadline out by a fixed increment.
     *
     * Fixed rather than the caller's original timeout: the member has to stay
     * rebuildable from a reply alone, and a reply cannot know what timeout its
     * caller chose, so the timeout is not encoded in it.
     *
     * Overwriting the score destroys the only record of the original deadline, so
     * the first renewal saves it first (SET NX, so later renewals leave it
     * alone). Without that, renewalCeilingMs would re-measure from the deadline
     * it just pushed out and no ceiling could ever be reached. Fail-soft: if the
     * write is lost the budget merely restarts from the current deadline —
     * bounded, just more generous.
     */
    private async extendDeadline(due: DueEntry): Promise<void> {
        try {
            await this.redis.set(
                RegistryKeys.wait_renew_origin(due.entry.sessionId, memberDigest(due.member)),
                String(due.deadlineMs),
                'EX',
                WAIT_RENEW_ORIGIN_TTL_SECONDS,
                'NX'
            );
        } catch (error) {
            console.debug(`[${this.workerId}] Wait sweep could not record renewal origin: ${error}`);
        }
        await this.redis.zadd(due.indexKey, this.now() + WAIT_RENEW_INCREMENT_MS, due.member);
    }

    /** Synchronous diagnostic snapshot for health checks. */
    snapshot(): Record<string, unknown> {
        return {
            worker_id: this.workerId,
            enabled: this.enabled,
            interval_seconds: this.intervalSeconds,
            shards: WAIT_INDEX_SHARDS,
            lock_ttl_seconds: this.lockTtlSeconds,
            renew_max_multiple: this.renewMaxMultiple,
            cancel_on_timeout: this.cancelOnTimeout,
            prune_enabled: this.pruneEnabled,
            prune_interval_seconds: this.pruneIntervalSeconds,
            prune_after_seconds: WAIT_PRUNE_AFTER_SECONDS,
        };
    }
}

import { WorkerRunner } from '../src/runner';
import { AnonymousWorker } from '../src/worker';
import { WorkerRegistry, acquireScopedLock } from '../src/registry';
import { AgentState } from '../src/protocol/agent_state';
import { CancelTaskCommand, ResumeCommand, commandFromDict } from '../src/protocol/commands';
import { PluginRegistry } from '../src/extensions/registry';
import {
    DEFAULT_ASK_USER_TIMEOUT_MS,
    LivenessErrorCode,
    QueueNames,
    RegistryKeys,
    TASK_GROUP_FIELD_ABORTED,
    TASK_GROUP_FIELD_COMPLETED,
    TASK_GROUP_FIELD_TOTAL,
    WAIT_PRUNE_AFTER_SECONDS,
    WAIT_RENEW_INCREMENT_MS,
    singleCallTaskGroupId,
} from '../src/constants';
import { encodeMember, memberDigest, waitIndexKey, waitIndexShard } from '../src/liveness/wait_index';
import { registerWait } from '../src/liveness/wait_registration';
import {
    OUTCOME_ASK_USER_SKIPPED,
    OUTCOME_CALLER_MISSING,
    OUTCOME_CALLER_TERMINAL,
    OUTCOME_CHILD_ALIVE,
    OUTCOME_CHILD_WAITING,
    OUTCOME_GROUP_ABORTED,
    OUTCOME_GROUP_ALREADY_JOINED,
    OUTCOME_GROUP_GONE,
    OUTCOME_MALFORMED,
    OUTCOME_NEVER_STARTED,
    OUTCOME_PRUNED,
    OUTCOME_RECOVERED,
    OUTCOME_TIMED_OUT,
    OUTCOME_WORKER_LOST,
    WaitIndexSweeper,
} from '../src/liveness/wait_sweeper';
import { MockRedis, bringAgentTypeOnline } from './helpers/mock_redis';

/**
 * The wait sweep (PRD D5). Runs the REAL WorkerRegistry / GatewayClient /
 * WorkerRunner over the shared in-memory Redis, because every assertion here is
 * about a decision made from evidence that other production code writes — a
 * hand-built fixture would prove only that the sweeper agrees with the test.
 *
 * Time is driven by the fixture's injectable clock (`advanceTime`), never by
 * sleeping: deadlines in this subsystem are hours to days apart.
 */

const SESSION = 'sess-sweeper';
const CALLER_AGENT = 'agent-a';
const CHILD_AGENT = 'agent-b';

interface Harness {
    readonly redis: MockRedis;
    readonly registry: WorkerRegistry;
    readonly sweeper: WaitIndexSweeper;
}

function buildHarness(options: Partial<ConstructorParameters<typeof WaitIndexSweeper>[1]> = {}): Harness {
    // Frozen base clock: every deadline this suite asserts on is exact, and a
    // wall clock ticking under the test turns those into flakes. advanceTime()
    // is the only thing that moves time.
    const base = Date.now();
    const redis = new MockRedis({ now: () => base });
    const registry = new WorkerRegistry(redis as any);
    const sweeper = new WaitIndexSweeper(redis as any, {
        workerId: 'worker-sweeper',
        registry,
        // Explicit rather than env-derived: these tests assert the triage, not
        // the switch defaults (which have their own test below).
        enabled: true,
        pruneEnabled: false,
        // Same clock the fixture expires keys on, so advanceTime() moves both.
        now: () => redis.nowMs(),
        ...options,
    });
    return { redis, registry, sweeper };
}

async function seedExecution(
    harness: Harness,
    params: {
        readonly messageId: string;
        readonly status: string;
        readonly sourceAgentType?: string;
        readonly targetAgentType?: string;
        readonly workerId?: string;
        readonly parentMessageId?: string;
        readonly taskGroupId?: string;
        readonly sessionId?: string;
    }
): Promise<string> {
    const executionId = `exec-${params.messageId}`;
    const sessionId = params.sessionId ?? SESSION;
    await harness.registry.initializeExecution({
        execution_id: executionId,
        message_id: params.messageId,
        session_id: sessionId,
        trace_id: 'trace-sweeper',
        parent_message_id: params.parentMessageId ?? '',
        source_agent_type: params.sourceAgentType ?? '',
        target_agent_type: params.targetAgentType ?? '',
        task_group_id: params.taskGroupId ?? '',
        stream_name: QueueNames.ctrl_stream(params.targetAgentType ?? ''),
        worker_id: params.workerId ?? '',
        status: 'QUEUED',
        cancel_requested: false,
        cancel_reason: '',
    });
    if (params.status !== 'QUEUED') {
        // The same call WorkerRunner makes when a handler returns, so the record
        // the sweep reads is the record production writes.
        await harness.registry.markExecutionFinished(executionId, sessionId, params.status);
    }
    return executionId;
}

async function register(
    harness: Harness,
    params: {
        readonly parentMessageId: string;
        readonly childMessageId: string;
        readonly timeoutMs: number;
        readonly taskGroupId?: string;
        readonly sessionId?: string;
    }
): Promise<void> {
    await registerWait(harness.redis as any, {
        sessionId: params.sessionId ?? SESSION,
        parentMessageId: params.parentMessageId,
        childMessageId: params.childMessageId,
        taskGroupId: params.taskGroupId,
        timeoutMs: params.timeoutMs,
        now: () => harness.redis.nowMs(),
    });
}

/**
 * Stand-ins waiting on an agent type's control stream, parsed exactly the way
 * WorkerRunner parses them — so a reply that would not survive the wire round
 * trip fails here rather than looking fine.
 */
function callerReplies(harness: Harness, agentType: string = CALLER_AGENT): ResumeCommand[] {
    return harness.redis
        .getStreamPayloads(QueueNames.ctrl_stream(agentType))
        .map((payload) => commandFromDict(payload) as ResumeCommand);
}

/** A stand-in's replyData, typed `unknown` on the wire. */
function replyDataOf(reply: ResumeCommand): Record<string, any> {
    return reply.replyData as Record<string, any>;
}

async function indexMembers(harness: Harness, sessionId: string = SESSION): Promise<string[]> {
    return harness.redis.zrange(waitIndexKey(sessionId), 0, -1);
}

/** A caller suspended on one callAgent, with the callee still running. */
async function seedSingleCall(
    harness: Harness,
    options: {
        readonly childStatus?: string;
        readonly childWorkerId?: string;
        readonly timeoutMs?: number;
        readonly callerStatus?: string;
    } = {}
): Promise<void> {
    await seedExecution(harness, {
        messageId: 'msg-caller',
        status: options.callerStatus ?? AgentState.WAITING_AGENT,
        sourceAgentType: 'client',
        targetAgentType: CALLER_AGENT,
        workerId: 'worker-caller',
    });
    await seedExecution(harness, {
        messageId: 'msg-child',
        status: options.childStatus ?? 'RUNNING',
        sourceAgentType: CALLER_AGENT,
        targetAgentType: CHILD_AGENT,
        parentMessageId: 'msg-caller',
        workerId: options.childWorkerId ?? 'worker-child',
    });
    await register(harness, {
        parentMessageId: 'msg-caller',
        childMessageId: 'msg-child',
        timeoutMs: options.timeoutMs ?? 60_000,
    });
}

describe('a callee whose worker died resolves its caller (AC1)', () => {
    test('synthesizes FAILED + CHILD_WORKER_LOST on the caller control stream', async () => {
        const harness = buildHarness();
        await seedSingleCall(harness);
        // No lease was ever written for worker-child, i.e. it is not online.

        harness.redis.advanceTime(61_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_WORKER_LOST]).toBe(1);
        const replies = callerReplies(harness);
        expect(replies).toHaveLength(1);
        expect(replies[0].status).toBe(AgentState.FAILED);
        expect(replyDataOf(replies[0]).error_code).toBe(LivenessErrorCode.CHILD_WORKER_LOST);
        expect(replyDataOf(replies[0]).child_message_id).toBe('msg-child');
    });

    test('the reply reattaches by the CALLER id and identifies the sub-task by the child id', async () => {
        // Swapping these two is the failure this whole subsystem is built on:
        // the runner reattaches the suspended execution by header.message_id,
        // and the gate rebuilds the wait-index member from the pair.
        const harness = buildHarness();
        await seedSingleCall(harness);

        harness.redis.advanceTime(61_000);
        await harness.sweeper.sweepOnce();

        const [reply] = callerReplies(harness);
        expect(reply.header.messageId).toBe('msg-caller');
        expect(reply.header.parentMessageId).toBe('msg-child');
        expect(reply.header.targetAgentType).toBe(CALLER_AGENT);
        expect(reply.header.sourceAgentType).toBe(CHILD_AGENT);
    });

    test('leaves the wait entry for the gate to claim, and pushes its deadline out', async () => {
        // Clearing it here would make the synthesized reply the one copy that
        // bypasses the gate — i.e. a second, ungated wake-up path.
        const harness = buildHarness();
        await seedSingleCall(harness);

        harness.redis.advanceTime(61_000);
        await harness.sweeper.sweepOnce();

        const member = encodeMember({
            sessionId: SESSION,
            parentMessageId: 'msg-caller',
            childMessageId: 'msg-child',
            taskGroupId: '',
        });
        expect(await indexMembers(harness)).toEqual([member]);
        const score = Number(await harness.redis.zscore(waitIndexKey(SESSION), member));
        expect(score).toBe(harness.redis.nowMs() + WAIT_RENEW_INCREMENT_MS);
    });

    test('a second pass inside the renewal window does not send a second stand-in', async () => {
        const harness = buildHarness();
        await seedSingleCall(harness);

        harness.redis.advanceTime(61_000);
        await harness.sweeper.sweepOnce();
        await harness.sweeper.sweepOnce();

        expect(callerReplies(harness)).toHaveLength(1);
    });

    test('a sub-task no worker ever claimed is CHILD_NEVER_STARTED', async () => {
        const harness = buildHarness();
        await seedSingleCall(harness, { childStatus: 'QUEUED', childWorkerId: '' });

        harness.redis.advanceTime(61_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_NEVER_STARTED]).toBe(1);
        expect(replyDataOf(callerReplies(harness)[0]).error_code)
            .toBe(LivenessErrorCode.CHILD_NEVER_STARTED);
    });
});

describe('a callee that finished but was never heard (AC2)', () => {
    test('the caller gets the REAL stored answer, not an error', async () => {
        const harness = buildHarness();
        await seedSingleCall(harness, { childStatus: AgentState.COMPLETED });
        await harness.redis.hset(
            QueueNames.task_group_results(singleCallTaskGroupId('msg-child')),
            'msg-child',
            JSON.stringify({
                status: AgentState.COMPLETED,
                content: 'the real answer',
                reply_data: { answer: 42 },
                target_agent_type: CHILD_AGENT,
                metadata: { from: 'child' },
                extra_payload: { tokens: 7 },
            })
        );

        harness.redis.advanceTime(61_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_RECOVERED]).toBe(1);
        const [reply] = callerReplies(harness);
        expect(reply.status).toBe(AgentState.COMPLETED);
        expect(reply.content).toBe('the real answer');
        expect(reply.replyData).toEqual({ answer: 42 });
        expect(reply.extraPayload).toEqual({ tokens: 7 });
        // Provenance rides on metadata, never in reply_data: business code reads
        // reply_data, and a caller that behaves differently for a recovered
        // reply than for a real one has grown a second result path.
        expect(reply.header.metadata.liveness_error_code)
            .toBe(LivenessErrorCode.REPLY_LOST_RECOVERED);
        expect(reply.header.metadata.from).toBe('child');
    });

    test('a finished callee with no stored result fails honestly rather than fabricating COMPLETED', async () => {
        const harness = buildHarness();
        await seedSingleCall(harness, { childStatus: AgentState.COMPLETED });

        harness.redis.advanceTime(61_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_RECOVERED]).toBe(1);
        const [reply] = callerReplies(harness);
        expect(reply.status).toBe(AgentState.FAILED);
        expect(replyDataOf(reply).error_code).toBe(LivenessErrorCode.REPLY_LOST_RECOVERED);
    });

    test('a CANCELLED callee is reported CANCELLED, not FAILED', async () => {
        const harness = buildHarness();
        await seedSingleCall(harness, { childStatus: AgentState.CANCELLED });

        harness.redis.advanceTime(61_000);
        await harness.sweeper.sweepOnce();

        expect(callerReplies(harness)[0].status).toBe(AgentState.CANCELLED);
    });
});

describe('nested chains fail one hop at a time (AC3)', () => {
    /**
     * A -> B -> C. Both waits (A on B, B on C) are registered, A's first. When
     * C's worker dies, only B may be resolved: A learns about it through B's own
     * reply, one hop at a time. Failing every level at once turns one dead
     * worker into a chain-wide outage and reports the wrong cause at each level.
     */
    async function seedChain(harness: Harness, options: { readonly cWorkerOnline: boolean }): Promise<void> {
        await seedExecution(harness, {
            messageId: 'msg-a', status: AgentState.WAITING_AGENT,
            sourceAgentType: 'client', targetAgentType: 'agent-a', workerId: 'worker-a',
        });
        await register(harness, { parentMessageId: 'msg-a', childMessageId: 'msg-b', timeoutMs: 60_000 });

        await seedExecution(harness, {
            messageId: 'msg-b', status: AgentState.WAITING_AGENT,
            sourceAgentType: 'agent-a', targetAgentType: 'agent-b',
            parentMessageId: 'msg-a', workerId: 'worker-b',
        });
        await register(harness, { parentMessageId: 'msg-b', childMessageId: 'msg-c', timeoutMs: 60_000 });

        await seedExecution(harness, {
            messageId: 'msg-c', status: 'RUNNING',
            sourceAgentType: 'agent-b', targetAgentType: 'agent-c',
            parentMessageId: 'msg-b', workerId: 'worker-c',
        });
        if (options.cWorkerOnline) {
            await bringAgentTypeOnline(harness.redis, 'agent-c', 'worker-c');
        }
    }

    test('the deepest wait fails; the outer one is renewed, not failed', async () => {
        const harness = buildHarness();
        await seedChain(harness, { cWorkerOnline: false });

        harness.redis.advanceTime(61_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_WORKER_LOST]).toBe(1);
        expect(outcomes[OUTCOME_CHILD_WAITING]).toBe(1);
        // Exactly one reply, and it is addressed to B — A hears about this only
        // once B has resumed, failed, and replied for itself.
        expect(callerReplies(harness, 'agent-a')).toHaveLength(0);
        const toB = callerReplies(harness, 'agent-b');
        expect(toB).toHaveLength(1);
        expect(toB[0].header.messageId).toBe('msg-b');
        expect(replyDataOf(toB[0]).error_code).toBe(LivenessErrorCode.CHILD_WORKER_LOST);
    });

    test('a caller blocked on a suspended callee is exempt from the renewal ceiling', async () => {
        // The outer wait was registered FIRST, so its ceiling comes first. If it
        // were enforced, the chain would fail top-down and invert the whole
        // propagation order.
        const harness = buildHarness({ renewMaxMultiple: 2 });
        await seedChain(harness, { cWorkerOnline: true });

        // Far past any ceiling derived from a 60s timeout.
        harness.redis.advanceTime(61_000);
        await harness.sweeper.sweepOnce();
        harness.redis.advanceTime(30 * 60_000);
        const outcomes = await harness.sweeper.sweepOnce();

        // The DEEPEST wait hit its ceiling and was resolved; the outer one was
        // renewed again in the same pass despite having been registered first
        // (and therefore having the earlier ceiling).
        expect(outcomes[OUTCOME_CHILD_WAITING]).toBe(1);
        expect(outcomes[OUTCOME_TIMED_OUT]).toBe(1);
        expect(callerReplies(harness, 'agent-a')).toHaveLength(0);
        expect(callerReplies(harness, 'agent-b')).toHaveLength(1);
    });
});

describe('a live callee is renewed, up to a ceiling (AC6, D10)', () => {
    test('slow but alive is renewed and never woken (AC6)', async () => {
        const harness = buildHarness();
        await seedSingleCall(harness, { timeoutMs: 600_000 });
        await bringAgentTypeOnline(harness.redis, CHILD_AGENT, 'worker-child');

        harness.redis.advanceTime(601_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_CHILD_ALIVE]).toBe(1);
        expect(callerReplies(harness)).toHaveLength(0);
        const member = encodeMember({
            sessionId: SESSION, parentMessageId: 'msg-caller',
            childMessageId: 'msg-child', taskGroupId: '',
        });
        expect(Number(await harness.redis.zscore(waitIndexKey(SESSION), member)))
            .toBe(harness.redis.nowMs() + WAIT_RENEW_INCREMENT_MS);
    });

    test('the first renewal records the ORIGINAL deadline in the side key, once (D10)', async () => {
        // Without this, every sweep would re-measure the budget from the
        // deadline it just pushed out and no ceiling could ever be reached.
        const harness = buildHarness();
        await seedSingleCall(harness, { timeoutMs: 600_000 });
        await bringAgentTypeOnline(harness.redis, CHILD_AGENT, 'worker-child');
        const registeredAt = harness.redis.nowMs();
        const member = encodeMember({
            sessionId: SESSION, parentMessageId: 'msg-caller',
            childMessageId: 'msg-child', taskGroupId: '',
        });
        const originKey = RegistryKeys.wait_renew_origin(SESSION, memberDigest(member));

        harness.redis.advanceTime(601_000);
        await harness.sweeper.sweepOnce();
        expect(Number(await harness.redis.get(originKey))).toBe(registeredAt + 600_000);

        harness.redis.advanceTime(WAIT_RENEW_INCREMENT_MS + 1_000);
        await harness.sweeper.sweepOnce();
        // SET NX: a later renewal must not move the origin forward.
        expect(Number(await harness.redis.get(originKey))).toBe(registeredAt + 600_000);
    });

    test('a callee alive but making no progress is eventually CHILD_TIMEOUT (D10)', async () => {
        const harness = buildHarness();
        await seedSingleCall(harness, { timeoutMs: 600_000 });
        await bringAgentTypeOnline(harness.redis, CHILD_AGENT, 'worker-child');

        // Ceiling = origin + max(timeout * (3 - 1), WAIT_RENEW_INCREMENT_MS)
        //         = registeredAt + 600_000 + 1_200_000.
        harness.redis.advanceTime(601_000);
        expect((await harness.sweeper.sweepOnce())[OUTCOME_CHILD_ALIVE]).toBe(1);
        harness.redis.advanceTime(1_300_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_TIMED_OUT]).toBe(1);
        const [reply] = callerReplies(harness);
        expect(reply.status).toBe(AgentState.FAILED);
        expect(replyDataOf(reply).error_code).toBe(LivenessErrorCode.CHILD_TIMEOUT);
    });
});

describe('timing out asks the callee to stop, but never depends on it (D8, AC9)', () => {
    test('the cancel is addressed to the callee worker own control stream', async () => {
        // A cancel put on the agent type competitive stream is claimed by an
        // arbitrary worker, finds no such execution in memory, and cancels
        // nothing while still reporting success.
        const harness = buildHarness();
        await seedSingleCall(harness, { timeoutMs: 600_000 });
        await bringAgentTypeOnline(harness.redis, CHILD_AGENT, 'worker-child');

        harness.redis.advanceTime(601_000);
        await harness.sweeper.sweepOnce();
        harness.redis.advanceTime(1_300_000);
        await harness.sweeper.sweepOnce();

        const direct = harness.redis.getStreamPayloads(QueueNames.worker_ctrl_stream('worker-child'));
        expect(direct).toHaveLength(1);
        const command = commandFromDict(direct[0]);
        expect(command).toBeInstanceOf(CancelTaskCommand);
        expect((command as CancelTaskCommand).targetMessageId).toBe('msg-child');
        expect((command as CancelTaskCommand).reason)
            .toContain(LivenessErrorCode.CHILD_TIMEOUT);
        expect(harness.redis.getStreamPayloads(QueueNames.ctrl_stream(CHILD_AGENT))).toHaveLength(0);
    });

    test('a lost callee is NOT asked to cancel — there is nothing on the other end', async () => {
        const harness = buildHarness();
        await seedSingleCall(harness);

        harness.redis.advanceTime(61_000);
        expect((await harness.sweeper.sweepOnce())[OUTCOME_WORKER_LOST]).toBe(1);

        expect(harness.redis.getStreamPayloads(QueueNames.worker_ctrl_stream('worker-child')))
            .toHaveLength(0);
    });

    test('the caller is resolved even when the cancel cannot be delivered (AC9)', async () => {
        const harness = buildHarness();
        await seedSingleCall(harness, { timeoutMs: 600_000 });
        await bringAgentTypeOnline(harness.redis, CHILD_AGENT, 'worker-child');
        const realXadd = harness.redis.xadd.bind(harness.redis);
        jest.spyOn(harness.redis, 'xadd').mockImplementation(async (stream: string, ...rest: any[]) => {
            if (stream === QueueNames.worker_ctrl_stream('worker-child')) {
                throw new Error('cancel delivery exploded');
            }
            return realXadd(stream, ...(rest as [any, ...any[]]));
        });

        harness.redis.advanceTime(601_000);
        await harness.sweeper.sweepOnce();
        harness.redis.advanceTime(1_300_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_TIMED_OUT]).toBe(1);
        expect(replyDataOf(callerReplies(harness)[0]).error_code)
            .toBe(LivenessErrorCode.CHILD_TIMEOUT);
    });

    test('cancelOnTimeout=false still resolves the caller', async () => {
        const harness = buildHarness({ cancelOnTimeout: false });
        await seedSingleCall(harness, { timeoutMs: 600_000 });
        await bringAgentTypeOnline(harness.redis, CHILD_AGENT, 'worker-child');

        harness.redis.advanceTime(601_000);
        await harness.sweeper.sweepOnce();
        harness.redis.advanceTime(1_300_000);
        await harness.sweeper.sweepOnce();

        expect(callerReplies(harness)).toHaveLength(1);
        expect(harness.redis.getStreamPayloads(QueueNames.worker_ctrl_stream('worker-child')))
            .toHaveLength(0);
    });
});

describe('askUser waits are cleaned up but never compensated (D9)', () => {
    async function seedAskUser(harness: Harness, callerStatus: string): Promise<void> {
        await seedExecution(harness, {
            messageId: 'msg-caller', status: callerStatus,
            sourceAgentType: 'client', targetAgentType: CALLER_AGENT, workerId: 'worker-caller',
        });
        await register(harness, {
            parentMessageId: 'msg-caller',
            // askUser has no sub-task; the empty child id IS the marker.
            childMessageId: '',
            timeoutMs: 60_000,
        });
    }

    test('a due askUser entry is skipped, with no reply and no deletion', async () => {
        // A human taking three days is not a fault, so there is nothing to
        // compensate; the entry stays so a repeated answer is recognized.
        const harness = buildHarness();
        await seedAskUser(harness, AgentState.WAITING_USER);

        harness.redis.advanceTime(61_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_ASK_USER_SKIPPED]).toBe(1);
        expect(callerReplies(harness)).toHaveLength(0);
        expect(await indexMembers(harness)).toHaveLength(1);
    });

    test('but an askUser entry whose caller is gone IS cleaned up', async () => {
        // Otherwise the global index grows without bound in exactly the
        // deployments that use askUser most.
        const harness = buildHarness();
        await register(harness, { parentMessageId: 'msg-vanished', childMessageId: '', timeoutMs: 60_000 });

        harness.redis.advanceTime(61_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_CALLER_MISSING]).toBe(1);
        expect(await indexMembers(harness)).toHaveLength(0);
    });
});

describe('the two switches are independent (D13)', () => {
    test('with compensation OFF, pruning still runs', async () => {
        // An entry is removed only by a reply or by a sweep. With compensation
        // off, every call whose reply never arrives — the very failures this
        // subsystem exists for — would leak one entry forever.
        const harness = buildHarness({ enabled: false, pruneEnabled: true });
        await seedSingleCall(harness, { timeoutMs: 0 });

        harness.redis.advanceTime((WAIT_PRUNE_AFTER_SECONDS + 3600) * 1000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_PRUNED]).toBe(1);
        expect(await indexMembers(harness)).toHaveLength(0);
        // ...and it compensated nothing while doing it.
        expect(callerReplies(harness)).toHaveLength(0);
        expect(outcomes[OUTCOME_WORKER_LOST] || 0).toBe(0);
    });

    test('an entry old enough to prune is one triage could only ever call "caller missing"', async () => {
        // This is the whole argument for pruning without an opt-in. The prune
        // threshold sits past the session TTL, so by the time an entry crosses
        // it the session registry holding its caller is gone — and the only
        // verdict triage can reach for a caller with no record is "delete".
        // Pruning therefore decides nothing that compensation would decide
        // differently; it just does it without reading anything.
        const harness = buildHarness({ enabled: true, pruneEnabled: false });
        await seedSingleCall(harness, { timeoutMs: 0 });

        harness.redis.advanceTime((WAIT_PRUNE_AFTER_SECONDS + 3600) * 1000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_PRUNED] || 0).toBe(0);
        expect(outcomes[OUTCOME_CALLER_MISSING]).toBe(1);
        expect(callerReplies(harness)).toHaveLength(0);
        expect(await indexMembers(harness)).toHaveLength(0);
    });

    test('pruning does NOT delete an askUser wait that only just came due', async () => {
        // DEFAULT_ASK_USER_TIMEOUT_MS equals the session TTL exactly, so a
        // prune threshold trimmed to the session TTL would sit on the boundary
        // of a live askUser wait and lose to any clock skew. The margin is what
        // makes "nothing can be learned from this entry" a proof.
        const harness = buildHarness({ enabled: false, pruneEnabled: true });
        await register(harness, {
            parentMessageId: 'msg-asker', childMessageId: '',
            timeoutMs: DEFAULT_ASK_USER_TIMEOUT_MS,
        });

        harness.redis.advanceTime(DEFAULT_ASK_USER_TIMEOUT_MS + 60_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_PRUNED] || 0).toBe(0);
        expect(await indexMembers(harness)).toHaveLength(1);
    });

    test('a sweeper with both halves off never starts a loop', async () => {
        const harness = buildHarness({ enabled: false, pruneEnabled: false });
        harness.sweeper.start();
        expect((harness.sweeper as any).loopTask).toBeNull();
        await harness.sweeper.stop();
    });

    test('a started loop stops promptly even though its interval is an hour', async () => {
        const harness = buildHarness({ enabled: false, pruneEnabled: true, pruneIntervalSeconds: 3600 });
        expect(harness.sweeper.loopIntervalSeconds).toBe(3600);
        harness.sweeper.start();
        await harness.sweeper.stop();
        expect((harness.sweeper as any).loopTask).toBeNull();
    });

    test('the default switch matrix is compensate=off / prune=on', async () => {
        const redis = new MockRedis();
        for (const key of [
            'BY_FRAMEWORK_WAIT_SWEEPER_ENABLED',
            'BY_FRAMEWORK_WAIT_PRUNE_ENABLED',
            'BY_FRAMEWORK_WAIT_CANCEL_ON_TIMEOUT',
            'BY_FRAMEWORK_WAIT_RENEW_MAX_MULTIPLE',
        ]) {
            delete process.env[key];
        }
        const sweeper = new WaitIndexSweeper(redis as any, { workerId: 'w' });
        expect(sweeper.enabled).toBe(false);
        expect(sweeper.pruneEnabled).toBe(true);
        expect(sweeper.cancelOnTimeout).toBe(true);
        expect(sweeper.renewMaxMultiple).toBe(3);
        // With compensation off there is nothing to do on a 30s cadence.
        expect(sweeper.loopIntervalSeconds).toBe(3600);
    });

    test('the environment switches are read', async () => {
        const redis = new MockRedis();
        process.env.BY_FRAMEWORK_WAIT_SWEEPER_ENABLED = 'on';
        process.env.BY_FRAMEWORK_WAIT_PRUNE_ENABLED = 'off';
        process.env.BY_FRAMEWORK_WAIT_CANCEL_ON_TIMEOUT = '0';
        process.env.BY_FRAMEWORK_WAIT_RENEW_MAX_MULTIPLE = '5';
        try {
            const sweeper = new WaitIndexSweeper(redis as any, { workerId: 'w' });
            expect(sweeper.enabled).toBe(true);
            expect(sweeper.pruneEnabled).toBe(false);
            expect(sweeper.cancelOnTimeout).toBe(false);
            expect(sweeper.renewMaxMultiple).toBe(5);
        } finally {
            delete process.env.BY_FRAMEWORK_WAIT_SWEEPER_ENABLED;
            delete process.env.BY_FRAMEWORK_WAIT_PRUNE_ENABLED;
            delete process.env.BY_FRAMEWORK_WAIT_CANCEL_ON_TIMEOUT;
            delete process.env.BY_FRAMEWORK_WAIT_RENEW_MAX_MULTIPLE;
        }
    });
});

describe('a Task Group orphan is compensated through the group join, never around it (D11)', () => {
    const GROUP = 'tg-orphan';

    async function seedGroup(harness: Harness): Promise<void> {
        await seedExecution(harness, {
            messageId: 'msg-caller', status: `${AgentState.WAITING_AGENT}: waiting_for_group`,
            sourceAgentType: 'client', targetAgentType: CALLER_AGENT, workerId: 'worker-caller',
        });
        await harness.redis.hset(QueueNames.task_group(GROUP), {
            [TASK_GROUP_FIELD_TOTAL]: '2',
            [TASK_GROUP_FIELD_COMPLETED]: '1',
            source_agent_type: CALLER_AGENT,
        });
        // Sibling 1 already replied and was counted by the join.
        await harness.redis.hset(
            QueueNames.task_group_results(GROUP),
            'msg-sib1',
            JSON.stringify({ status: AgentState.COMPLETED, content: 'first', reply_data: null })
        );
        // Sibling 2's worker died.
        await seedExecution(harness, {
            messageId: 'msg-sib2', status: 'RUNNING',
            sourceAgentType: CALLER_AGENT, targetAgentType: CHILD_AGENT,
            parentMessageId: 'msg-caller', taskGroupId: GROUP, workerId: 'worker-dead',
        });
        await register(harness, {
            parentMessageId: 'msg-caller', childMessageId: 'msg-sib2',
            taskGroupId: GROUP, timeoutMs: 60_000,
        });
    }

    test('the sweeper writes no group result and no counter of its own', async () => {
        // A second writer of the group accounting is what hangs a caller: when
        // that copy is the increment that reaches `total`, no reply is left to
        // trigger the join.
        const harness = buildHarness();
        await seedGroup(harness);

        harness.redis.advanceTime(61_000);
        expect((await harness.sweeper.sweepOnce())[OUTCOME_WORKER_LOST]).toBe(1);

        expect(await harness.redis.hget(QueueNames.task_group(GROUP), TASK_GROUP_FIELD_COMPLETED))
            .toBe('1');
        expect(await harness.redis.hget(QueueNames.task_group_results(GROUP), 'msg-sib2')).toBeNull();
        const [reply] = callerReplies(harness);
        expect(reply.header.taskGroupId).toBe(GROUP);
    });

    test('the synthesized reply completes the group through the REAL join, exactly once', async () => {
        const harness = buildHarness();
        await seedGroup(harness);
        harness.redis.advanceTime(61_000);
        await harness.sweeper.sweepOnce();

        const worker = new AnonymousWorker({
            workerId: 'worker-caller',
            agentTypes: [CALLER_AGENT],
            registry: harness.registry,
            redisClient: harness.redis as any,
            pluginRegistry: new PluginRegistry(),
            onTask: async () => 'aggregated',
        });
        const runner = new WorkerRunner(worker, {
            redisClient: harness.redis as any,
            groupName: 'group-sweeper',
        });
        const [standIn] = callerReplies(harness);
        await (runner as any).processAndAck(QueueNames.ctrl_stream(CALLER_AGENT), '1-0', standIn);

        const group = await harness.redis.hgetall(QueueNames.task_group(GROUP));
        expect(group[TASK_GROUP_FIELD_COMPLETED]).toBe('2');
        expect(Number(group[TASK_GROUP_FIELD_COMPLETED]))
            .toBeLessThanOrEqual(Number(group[TASK_GROUP_FIELD_TOTAL]));
        // The join stored it under the sub-task id, so siblings cannot collide.
        expect(await harness.redis.hget(QueueNames.task_group_results(GROUP), 'msg-sib2'))
            .not.toBeNull();
    });

    test('a group whose tracker has expired is cleaned up, not compensated', async () => {
        const harness = buildHarness();
        await seedGroup(harness);
        await harness.redis.del(QueueNames.task_group(GROUP));

        harness.redis.advanceTime(61_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_GROUP_GONE]).toBe(1);
        expect(callerReplies(harness)).toHaveLength(0);
        expect(await indexMembers(harness)).toHaveLength(0);
    });

    test('an aborted group is cleaned up, not compensated', async () => {
        const harness = buildHarness();
        await seedGroup(harness);
        await harness.redis.hset(QueueNames.task_group(GROUP), TASK_GROUP_FIELD_ABORTED, '1');

        harness.redis.advanceTime(61_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_GROUP_ABORTED]).toBe(1);
        expect(callerReplies(harness)).toHaveLength(0);
        expect(await indexMembers(harness)).toHaveLength(0);
    });

    test('a member whose reply was already joined is cleaned up, not counted twice', async () => {
        const harness = buildHarness();
        await seedGroup(harness);
        await harness.redis.hset(
            QueueNames.task_group_results(GROUP),
            'msg-sib2',
            JSON.stringify({ status: AgentState.COMPLETED })
        );

        harness.redis.advanceTime(61_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_GROUP_ALREADY_JOINED]).toBe(1);
        expect(callerReplies(harness)).toHaveLength(0);
    });
});

describe('entries nobody is waiting on are cleaned up, never compensated', () => {
    test('a caller already in a terminal state gets no stand-in', async () => {
        // Reachable: the wait entry is registered before the dispatch xadd, so
        // an xadd that throws fails the caller and strands the entry.
        const harness = buildHarness();
        await seedSingleCall(harness, { callerStatus: AgentState.FAILED });

        harness.redis.advanceTime(61_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_CALLER_TERMINAL]).toBe(1);
        expect(callerReplies(harness)).toHaveLength(0);
        expect(await indexMembers(harness)).toHaveLength(0);
    });

    test('a caller with no execution record at all gets no stand-in', async () => {
        const harness = buildHarness();
        await register(harness, { parentMessageId: 'msg-ghost', childMessageId: 'msg-child', timeoutMs: 0 });

        harness.redis.advanceTime(1_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_CALLER_MISSING]).toBe(1);
        expect(await indexMembers(harness)).toHaveLength(0);
    });

    test('a malformed member is dropped rather than poisoning the shard', async () => {
        const harness = buildHarness();
        await harness.redis.zadd(waitIndexKey(SESSION), 1, 'not|four|fields');
        await seedSingleCall(harness);

        harness.redis.advanceTime(61_000);
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_MALFORMED]).toBe(1);
        // The rest of the shard was still swept.
        expect(outcomes[OUTCOME_WORKER_LOST]).toBe(1);
    });
});

describe('shards are claimed, not owned (no leader election)', () => {
    test('a shard already claimed by another sweeper is skipped this pass', async () => {
        const harness = buildHarness();
        await seedSingleCall(harness);
        const shard = waitIndexShard(SESSION);

        harness.redis.advanceTime(61_000);
        await acquireScopedLock(
            harness.redis as any, RegistryKeys.wait_sweep_lock(shard), 'someone-else', 60
        );
        const outcomes = await harness.sweeper.sweepOnce();

        expect(outcomes[OUTCOME_WORKER_LOST] || 0).toBe(0);
        expect(callerReplies(harness)).toHaveLength(0);
    });

    test('the claim is released at the end of the pass, so the next worker gets it', async () => {
        const harness = buildHarness();
        await seedSingleCall(harness);
        const shard = waitIndexShard(SESSION);

        harness.redis.advanceTime(61_000);
        await harness.sweeper.sweepOnce();

        expect(await harness.redis.get(RegistryKeys.wait_sweep_lock(shard))).toBeNull();
    });

    test('a lock is only released by its holder', async () => {
        const redis = new MockRedis();
        const key = RegistryKeys.wait_sweep_lock(0);
        expect(await acquireScopedLock(redis as any, key, 'token-a', 60)).toBe(true);
        expect(await acquireScopedLock(redis as any, key, 'token-b', 60)).toBe(false);
        const { releaseScopedLock } = await import('../src/registry');
        expect(await releaseScopedLock(redis as any, key, 'token-b')).toBe(false);
        expect(await releaseScopedLock(redis as any, key, 'token-a')).toBe(true);
        expect(await redis.get(key)).toBeNull();
    });

    test('the stored lock value keeps the cross-SDK {"token": ...} shape', async () => {
        // Python releases these with a Lua script that cjson-decodes the value
        // and reads .token; a bare string would be unparseable legacy data
        // there, leaving the holder unable to release its own lock.
        const redis = new MockRedis();
        const key = RegistryKeys.wait_sweep_lock(3);
        await acquireScopedLock(redis as any, key, 'token-x', 60);
        expect(JSON.parse((await redis.get(key)) as string)).toEqual({ token: 'token-x' });
    });
});

describe('the sweep is hosted by every worker (no singleton service)', () => {
    test('initialize() starts a sweeper and release() stops it', async () => {
        // Nothing else would notice if the runner stopped hosting it: the
        // subsystem would keep registering wait entries and never act on any.
        const redis = new MockRedis();
        const registry = new WorkerRegistry(redis as any);
        const worker = new AnonymousWorker({
            workerId: 'worker-host',
            agentTypes: [CALLER_AGENT],
            registry,
            redisClient: redis as any,
            pluginRegistry: new PluginRegistry(),
            onTask: async () => 'ok',
        });
        const runner = new WorkerRunner(worker, {
            redisClient: redis as any,
            groupName: 'group-host',
        });

        await runner.initialize();
        try {
            const sweeper = (runner as any).waitSweeper as WaitIndexSweeper;
            expect(sweeper).toBeInstanceOf(WaitIndexSweeper);
            // Default matrix: the loop exists because pruning is on, and it
            // compensates nothing until the deployment opts in.
            expect(sweeper.pruneEnabled).toBe(true);
            expect(sweeper.enabled).toBe(false);
            expect(sweeper.workerId).toBe('worker-host');
        } finally {
            await runner.release();
        }
        expect((runner as any).waitSweeper).toBeNull();
    });
});

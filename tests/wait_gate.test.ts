import { WorkerRunner } from '../src/runner';
import { AnonymousWorker } from '../src/worker';
import { GatewayProcessor } from '../src/processor';
import { WorkerRegistry } from '../src/registry';
import { AgentContext } from '../src/context';
import { ResumeCommand, GatewayCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { AgentState } from '../src/protocol/agent_state';
import { EventType } from '../src/protocol/event_type';
import { PluginRegistry } from '../src/extensions/registry';
import {
    DEFAULT_REPLY_TIMEOUT_MS,
    QueueNames,
    TASK_GROUP_FIELD_COMPLETED,
    TASK_GROUP_FIELD_TOTAL,
    WAIT_CONSUMED_TTL_SECONDS,
} from '../src/constants';
import {
    ALLOW_CLAIMED,
    ALLOW_GATE_ERROR,
    ALLOW_UNREGISTERED,
    DENY_ALREADY_CONSUMED,
    candidateMembers,
    consumeWaitEntry,
    consumedMarkerKey,
} from '../src/liveness/wait_gate';
import { encodeMember, memberFromResume, waitIndexKey } from '../src/liveness/wait_index';
import { registerWait } from '../src/liveness/wait_registration';
import { MockRedis } from './helpers/mock_redis';

/**
 * Idempotency gate (contract §4-§5, PRD D4). Runs the REAL WorkerRunner /
 * GatewayWorker / GatewayProcessor / WorkerRegistry over the shared in-memory
 * Redis, so what is asserted is the decision the production path actually makes
 * — a gate re-implemented in the test would prove nothing about where it sits.
 */

interface Harness {
    redis: MockRedis;
    registry: WorkerRegistry;
    worker: AnonymousWorker;
    runner: WorkerRunner;
    handled: GatewayCommand[];
}

function buildHarness(
    onTask?: (command: GatewayCommand, context: AgentContext) => Promise<any>
): Harness {
    const redis = new MockRedis();
    const registry = new WorkerRegistry(redis as any);
    const worker = new AnonymousWorker({
        workerId: 'worker-gate',
        agentTypes: ['gate-agent'],
        registry,
        redisClient: redis as any,
        pluginRegistry: new PluginRegistry(),
        onTask: onTask ?? (async () => 'ok'),
    });

    const handled: GatewayCommand[] = [];
    const original = worker.handleMessage.bind(worker);
    jest.spyOn(worker, 'handleMessage').mockImplementation(async (command: any, options: any = {}) => {
        handled.push(command);
        return original(command, options);
    });

    const runner = new WorkerRunner(worker, {
        redisClient: redis as any,
        groupName: 'group-gate',
    });
    return { redis, registry, worker, runner, handled };
}

function reply(overrides: {
    /** The caller's own message id — what the resume reattaches by. */
    callerMessageId: string;
    sessionId: string;
    /** The sub-task's dispatch-time message id. */
    childMessageId?: string;
    taskGroupId?: string;
    status?: string;
    replyData?: any;
}): ResumeCommand {
    return new ResumeCommand(
        new MessageHeader(overrides.callerMessageId, overrides.sessionId, 'trace-gate', {
            targetAgentType: 'gate-agent',
            sourceAgentType: 'child-agent',
            parentMessageId: overrides.childMessageId ?? '',
            taskGroupId: overrides.taskGroupId ?? '',
        }),
        'reply content',
        overrides.status ?? AgentState.COMPLETED,
        overrides.replyData ?? { answer: 42 }
    );
}

async function seedCallerExecution(
    registry: WorkerRegistry,
    params: { executionId: string; messageId: string; sessionId: string }
): Promise<void> {
    await registry.initializeExecution({
        execution_id: params.executionId,
        message_id: params.messageId,
        session_id: params.sessionId,
        source_agent_type: 'upstream-agent',
        parent_message_id: 'msg-grandparent',
        target_agent_type: 'gate-agent',
        status: 'QUEUED',
    });
}

describe('the gate distinguishes the two meanings of ZREM returning 0', () => {
    // RED LINE 1. `ZREM` -> 0 conflates "already claimed by someone else"
    // (drop) with "never registered" (allow). Conflating them drops every reply
    // that was dispatched before the wait index existed, i.e. every in-flight
    // reply during a rolling upgrade.

    test('a registered wait is claimed exactly once (ALLOW_CLAIMED)', async () => {
        const redis = new MockRedis();
        await registerWait(redis as any, {
            sessionId: 'sess-gate',
            parentMessageId: 'msg-caller',
            childMessageId: 'msg-child',
            timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
        });

        const decision = await consumeWaitEntry(
            redis as any,
            reply({ callerMessageId: 'msg-caller', sessionId: 'sess-gate', childMessageId: 'msg-child' })
        );

        expect(decision.allow).toBe(true);
        expect(decision.reason).toBe(ALLOW_CLAIMED);
        expect(decision.member).toBe(
            encodeMember({
                sessionId: 'sess-gate',
                parentMessageId: 'msg-caller',
                childMessageId: 'msg-child',
                taskGroupId: '',
            })
        );
        // The entry is gone, so no second claimant can win.
        expect(await redis.zrangebyscore(waitIndexKey('sess-gate'), '-inf', '+inf')).toEqual([]);
    });

    test('a duplicate of a claimed reply is dropped (DENY_ALREADY_CONSUMED)', async () => {
        const redis = new MockRedis();
        await registerWait(redis as any, {
            sessionId: 'sess-gate',
            parentMessageId: 'msg-caller',
            childMessageId: 'msg-child',
            timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
        });
        const command = reply({
            callerMessageId: 'msg-caller', sessionId: 'sess-gate', childMessageId: 'msg-child',
        });

        expect((await consumeWaitEntry(redis as any, command)).reason).toBe(ALLOW_CLAIMED);
        const second = await consumeWaitEntry(redis as any, command);

        expect(second.allow).toBe(false);
        expect(second.reason).toBe(DENY_ALREADY_CONSUMED);
    });

    test('a reply for a wait that was never registered is ALLOWED (ALLOW_UNREGISTERED)', async () => {
        // The rolling-upgrade case: the dispatch predates the wait index, so
        // there is no entry AND no marker. Dropping this is a permanently lost
        // reply and a caller suspended forever.
        const redis = new MockRedis();

        const decision = await consumeWaitEntry(
            redis as any,
            reply({ callerMessageId: 'msg-caller', sessionId: 'sess-gate', childMessageId: 'msg-child' })
        );

        expect(decision.allow).toBe(true);
        expect(decision.reason).toBe(ALLOW_UNREGISTERED);
        expect(decision.member).toBe('');
    });

    test('the consumed marker carries WAIT_CONSUMED_TTL_SECONDS, not the entry TTL', async () => {
        // The marker is what tells the two zeroes apart, so it has to outlive
        // every wait it could be asked about — including an askUser wait, whose
        // deadline is the whole session TTL.
        const redis = new MockRedis();
        const member = encodeMember({
            sessionId: 'sess-gate',
            parentMessageId: 'msg-caller',
            childMessageId: 'msg-child',
            taskGroupId: '',
        });
        await registerWait(redis as any, {
            sessionId: 'sess-gate',
            parentMessageId: 'msg-caller',
            childMessageId: 'msg-child',
            timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
        });

        await consumeWaitEntry(
            redis as any,
            reply({ callerMessageId: 'msg-caller', sessionId: 'sess-gate', childMessageId: 'msg-child' })
        );

        const key = consumedMarkerKey('sess-gate', member);
        expect(await redis.get(key)).toBe('1');
        expect(await redis.ttl(key)).toBe(WAIT_CONSUMED_TTL_SECONDS);
    });

    test('once the marker expires the duplicate is allowed again, never dropped', async () => {
        const redis = new MockRedis();
        await registerWait(redis as any, {
            sessionId: 'sess-gate',
            parentMessageId: 'msg-caller',
            childMessageId: 'msg-child',
            timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
        });
        const command = reply({
            callerMessageId: 'msg-caller', sessionId: 'sess-gate', childMessageId: 'msg-child',
        });
        await consumeWaitEntry(redis as any, command);

        redis.advanceTime((WAIT_CONSUMED_TTL_SECONDS + 1) * 1000);

        // Erring towards one extra wake-up rather than a silent drop is the
        // whole module's bias, so an expired marker must degrade to ALLOW.
        expect((await consumeWaitEntry(redis as any, command)).reason).toBe(ALLOW_UNREGISTERED);
    });
});

describe('the gate fails open', () => {
    // RED LINE 2. Losing a message is permanent silence; an extra wake-up is
    // something downstream already tolerates.

    test('a Redis error on ZREM allows the reply (ALLOW_GATE_ERROR)', async () => {
        const redis = new MockRedis();
        jest.spyOn(redis, 'zrem').mockRejectedValue(new Error('READONLY: redis is loading'));

        const decision = await consumeWaitEntry(
            redis as any,
            reply({ callerMessageId: 'msg-caller', sessionId: 'sess-gate', childMessageId: 'msg-child' })
        );

        expect(decision.allow).toBe(true);
        expect(decision.reason).toBe(ALLOW_GATE_ERROR);
    });

    test('a Redis error on the marker lookup allows the reply too', async () => {
        const redis = new MockRedis();
        jest.spyOn(redis, 'exists').mockRejectedValue(new Error('CLUSTERDOWN'));

        const decision = await consumeWaitEntry(
            redis as any,
            reply({ callerMessageId: 'msg-caller', sessionId: 'sess-gate', childMessageId: 'msg-child' })
        );

        expect(decision.allow).toBe(true);
        expect(decision.reason).toBe(ALLOW_GATE_ERROR);
    });

    test('a failed marker write still lets the claim through', async () => {
        // Marking is fail-soft on its own: losing the marker only costs the
        // ability to recognize a much later duplicate.
        const redis = new MockRedis();
        await registerWait(redis as any, {
            sessionId: 'sess-gate',
            parentMessageId: 'msg-caller',
            childMessageId: 'msg-child',
            timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
        });
        jest.spyOn(redis, 'set').mockRejectedValue(new Error('OOM'));

        const decision = await consumeWaitEntry(
            redis as any,
            reply({ callerMessageId: 'msg-caller', sessionId: 'sess-gate', childMessageId: 'msg-child' })
        );

        expect(decision.allow).toBe(true);
        expect(decision.reason).toBe(ALLOW_CLAIMED);
    });
});

describe('candidate order is load-bearing (AC-TS-3)', () => {
    test('the sub-agent member comes first and the askUser member second', () => {
        const members = candidateMembers(
            reply({ callerMessageId: 'msg-caller', sessionId: 'sess-gate', childMessageId: 'msg-child' })
        );
        expect(members).toEqual([
            encodeMember({
                sessionId: 'sess-gate', parentMessageId: 'msg-caller',
                childMessageId: 'msg-child', taskGroupId: '',
            }),
            encodeMember({
                sessionId: 'sess-gate', parentMessageId: 'msg-caller',
                childMessageId: '', taskGroupId: '',
            }),
        ]);
    });

    test('a group reply never considers the askUser candidate', () => {
        // A reply carrying a taskGroupId is a sub-agent reply by construction.
        const members = candidateMembers(reply({
            callerMessageId: 'msg-caller', sessionId: 'sess-gate',
            childMessageId: 'msg-child', taskGroupId: 'tg-1',
        }));
        expect(members).toHaveLength(1);
    });

    test('a reply whose childMessageId is already empty produces one candidate', () => {
        const members = candidateMembers(
            reply({ callerMessageId: 'msg-caller', sessionId: 'sess-gate', childMessageId: '' })
        );
        expect(members).toHaveLength(1);
    });

    test('a consumed duplicate sub-agent reply must NOT clear a live askUser wait', async () => {
        // AC-TS-3, the failure the ordering exists to prevent. Same caller has
        // (a) a resolved call_agent wait and (b) a LIVE askUser wait. A late
        // duplicate of the sub-agent's reply must stop at its own marker; if it
        // fell through to the askUser candidate it would ZREM a wait the user
        // has not answered yet, and the user's real answer would then be dropped
        // as "already consumed" — the caller hangs forever.
        const redis = new MockRedis();
        await registerWait(redis as any, {
            sessionId: 'sess-gate', parentMessageId: 'msg-caller',
            childMessageId: 'msg-child', timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
        });
        const subAgentReply = reply({
            callerMessageId: 'msg-caller', sessionId: 'sess-gate', childMessageId: 'msg-child',
        });
        expect((await consumeWaitEntry(redis as any, subAgentReply)).reason).toBe(ALLOW_CLAIMED);

        // The caller resumed, asked the user something, and is now waiting.
        await registerWait(redis as any, {
            sessionId: 'sess-gate', parentMessageId: 'msg-caller',
            childMessageId: '', timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
        });
        const askUserMember = encodeMember({
            sessionId: 'sess-gate', parentMessageId: 'msg-caller',
            childMessageId: '', taskGroupId: '',
        });

        const duplicate = await consumeWaitEntry(redis as any, subAgentReply);

        expect(duplicate.allow).toBe(false);
        expect(duplicate.member).not.toBe(askUserMember);
        // The live askUser entry is untouched...
        expect(await redis.zrangebyscore(waitIndexKey('sess-gate'), '-inf', '+inf'))
            .toEqual([askUserMember]);
        // ...and the user's own answer still gets through afterwards.
        const userAnswer = new ResumeCommand(
            // A client is free to put anything in parentMessageId, which is
            // exactly why the askUser member cannot be rebuilt exactly and needs
            // the fallback candidate.
            new MessageHeader('msg-caller', 'sess-gate', 'trace-gate', {
                targetAgentType: 'gate-agent',
                sourceAgentType: 'client',
                parentMessageId: 'whatever-the-client-put-here',
            }),
            'the user answered', AgentState.COMPLETED, { answer: 'yes' }
        );
        const answered = await consumeWaitEntry(redis as any, userAnswer);
        expect(answered.allow).toBe(true);
        expect(answered.reason).toBe(ALLOW_CLAIMED);
        expect(answered.member).toBe(askUserMember);
    });

    test('registering a new askUser round voids the previous round\'s marker', async () => {
        // Consecutive askUser rounds encode to the SAME member, so the previous
        // round's "consumed" verdict has to be cleared or round two's answer is
        // dropped as a duplicate.
        const redis = new MockRedis();
        const askUser = new ResumeCommand(
            new MessageHeader('msg-caller', 'sess-gate', 'trace-gate', {
                targetAgentType: 'gate-agent', sourceAgentType: 'client', parentMessageId: 'x',
            }),
            'answer', AgentState.COMPLETED, {}
        );
        for (const round of [1, 2]) {
            await registerWait(redis as any, {
                sessionId: 'sess-gate', parentMessageId: 'msg-caller',
                childMessageId: '', timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
            });
            const decision = await consumeWaitEntry(redis as any, askUser);
            expect([round, decision.reason]).toEqual([round, ALLOW_CLAIMED]);
        }
    });
});

describe('the gate sits on the runner path, upstream of the worker', () => {
    test('a duplicate resume wakes the caller exactly once', async () => {
        const { redis, registry, runner, handled } = buildHarness();
        await seedCallerExecution(registry, {
            executionId: 'exec-once', messageId: 'msg-caller', sessionId: 'sess-once',
        });
        await registerWait(redis as any, {
            sessionId: 'sess-once', parentMessageId: 'msg-caller',
            childMessageId: 'msg-child', timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
        });
        const command = reply({
            callerMessageId: 'msg-caller', sessionId: 'sess-once', childMessageId: 'msg-child',
        });

        await runner.processAndAck(QueueNames.ctrl_stream('gate-agent'), '1-0', command);
        await runner.processAndAck(QueueNames.ctrl_stream('gate-agent'), '2-0', command);

        expect(handled).toHaveLength(1);
        // The dropped copy is still acked, or it would be redelivered forever.
        expect(redis.ackCalls).toContainEqual([QueueNames.ctrl_stream('gate-agent'), 'group-gate', '2-0']);
    });

    test('an unregistered resume still reaches the worker', async () => {
        const { registry, runner, handled } = buildHarness();
        await seedCallerExecution(registry, {
            executionId: 'exec-unreg', messageId: 'msg-caller', sessionId: 'sess-unreg',
        });

        await runner.processAndAck(
            QueueNames.ctrl_stream('gate-agent'), '1-0',
            reply({ callerMessageId: 'msg-caller', sessionId: 'sess-unreg', childMessageId: 'msg-child' })
        );

        expect(handled).toHaveLength(1);
    });

    test('a dropped reply is announced as an orphanedReply data-plane event', async () => {
        const { redis, registry, runner } = buildHarness();
        await seedCallerExecution(registry, {
            executionId: 'exec-orph', messageId: 'msg-caller', sessionId: 'sess-orph',
        });
        await registerWait(redis as any, {
            sessionId: 'sess-orph', parentMessageId: 'msg-caller',
            childMessageId: 'msg-child', timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
        });
        const command = reply({
            callerMessageId: 'msg-caller', sessionId: 'sess-orph', childMessageId: 'msg-child',
        });
        await runner.processAndAck(QueueNames.ctrl_stream('gate-agent'), '1-0', command);

        await runner.processAndAck(QueueNames.ctrl_stream('gate-agent'), '2-0', command);

        const events = redis.getStreamPayloads(QueueNames.session_data_stream('sess-orph'))
            .filter((msg: any) => msg?.event_type === EventType.ORPHANED_REPLY);
        expect(events).toHaveLength(1);
        expect(events[0].data).toMatchObject({
            reason: DENY_ALREADY_CONSUMED,
            caller_message_id: 'msg-caller',
            child_message_id: 'msg-child',
            worker_id: 'worker-gate',
        });
    });

    test('a failure to emit the event does not resurrect the dropped reply', async () => {
        // Reporting happens after the decision and must never change it.
        const { redis, registry, runner, handled } = buildHarness();
        await seedCallerExecution(registry, {
            executionId: 'exec-emitfail', messageId: 'msg-caller', sessionId: 'sess-emitfail',
        });
        await registerWait(redis as any, {
            sessionId: 'sess-emitfail', parentMessageId: 'msg-caller',
            childMessageId: 'msg-child', timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
        });
        const command = reply({
            callerMessageId: 'msg-caller', sessionId: 'sess-emitfail', childMessageId: 'msg-child',
        });
        await runner.processAndAck(QueueNames.ctrl_stream('gate-agent'), '1-0', command);
        jest.spyOn(redis, 'pipeline').mockImplementation(() => {
            throw new Error('data stream unavailable');
        });

        await expect(
            runner.processAndAck(QueueNames.ctrl_stream('gate-agent'), '2-0', command)
        ).resolves.toBeUndefined();

        expect(handled).toHaveLength(1);
    });

    test('a Task Group duplicate cannot push completed past total', async () => {
        // The gate runs before Group Join's HINCRBY, which is the whole reason
        // it lives in the runner rather than inside the worker.
        const { redis, registry, runner } = buildHarness();
        await seedCallerExecution(registry, {
            executionId: 'exec-group', messageId: 'msg-caller', sessionId: 'sess-group',
        });
        const groupKey = QueueNames.task_group('tg-gate');
        await redis.hset(groupKey, {
            [TASK_GROUP_FIELD_TOTAL]: '2',
            [TASK_GROUP_FIELD_COMPLETED]: '0',
        });
        for (const child of ['msg-child-a', 'msg-child-b']) {
            await registerWait(redis as any, {
                sessionId: 'sess-group', parentMessageId: 'msg-caller',
                childMessageId: child, taskGroupId: 'tg-gate',
                timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
            });
        }
        const a = reply({
            callerMessageId: 'msg-caller', sessionId: 'sess-group',
            childMessageId: 'msg-child-a', taskGroupId: 'tg-gate',
        });
        const b = reply({
            callerMessageId: 'msg-caller', sessionId: 'sess-group',
            childMessageId: 'msg-child-b', taskGroupId: 'tg-gate',
        });

        await runner.processAndAck(QueueNames.ctrl_stream('gate-agent'), '1-0', a);
        await runner.processAndAck(QueueNames.ctrl_stream('gate-agent'), '2-0', a);
        await runner.processAndAck(QueueNames.ctrl_stream('gate-agent'), '3-0', b);
        await runner.processAndAck(QueueNames.ctrl_stream('gate-agent'), '4-0', b);

        expect(await redis.hget(groupKey, TASK_GROUP_FIELD_COMPLETED)).toBe('2');
        // Each sibling stored once, keyed by the sub-task id.
        expect(Object.keys(await redis.hgetall(QueueNames.task_group_results('tg-gate'))).sort())
            .toEqual(['msg-child-a', 'msg-child-b']);
    });
});

describe('the gate is on the processor path too', () => {
    // GatewayProcessor is a second, independent reply entry point (callers that
    // drive their own consume loop). A gate on only one entry point is not a
    // gate: replies arriving via the other one would wake an already-resolved
    // caller AND leave the wait-index entry behind for a sweep to redo.

    test('a duplicate resume is dropped and the handler never runs', async () => {
        const redis = new MockRedis();
        const processor = new GatewayProcessor('worker-proc', redis as any);
        const handled: GatewayCommand[] = [];
        const handler = async (command: GatewayCommand) => {
            handled.push(command);
            return 'ok';
        };
        await registerWait(redis as any, {
            sessionId: 'sess-proc', parentMessageId: 'msg-caller',
            childMessageId: 'msg-child', timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
        });
        const command = reply({
            callerMessageId: 'msg-caller', sessionId: 'sess-proc', childMessageId: 'msg-child',
        });

        await processor.process(command, handler);
        const dropped = await processor.process(command, handler);

        expect(handled).toHaveLength(1);
        expect(dropped).toBeNull();
    });

    test('an unregistered resume still reaches the handler', async () => {
        const redis = new MockRedis();
        const processor = new GatewayProcessor('worker-proc', redis as any);
        const handled: GatewayCommand[] = [];

        await processor.process(
            reply({ callerMessageId: 'msg-caller', sessionId: 'sess-proc2', childMessageId: 'msg-child' }),
            async (command) => {
                handled.push(command);
                return 'ok';
            }
        );

        expect(handled).toHaveLength(1);
    });

    test('a dropped reply emits orphanedReply here as well', async () => {
        const redis = new MockRedis();
        const processor = new GatewayProcessor('worker-proc', redis as any);
        await registerWait(redis as any, {
            sessionId: 'sess-proc3', parentMessageId: 'msg-caller',
            childMessageId: 'msg-child', timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
        });
        const command = reply({
            callerMessageId: 'msg-caller', sessionId: 'sess-proc3', childMessageId: 'msg-child',
        });
        await processor.process(command, async () => 'ok');

        await processor.process(command, async () => 'ok');

        const events = redis.getStreamPayloads(QueueNames.session_data_stream('sess-proc3'))
            .filter((msg: any) => msg?.event_type === EventType.ORPHANED_REPLY);
        expect(events).toHaveLength(1);
        expect(events[0].data.worker_id).toBe('worker-proc');
    });
});

describe('memberFromResume and the gate agree on the member', () => {
    test('the member a reply rebuilds is the member the gate claims', async () => {
        const redis = new MockRedis();
        const command = reply({
            callerMessageId: 'msg-caller', sessionId: 'sess-agree',
            childMessageId: 'msg-child', taskGroupId: 'tg-agree',
        });
        await registerWait(redis as any, {
            sessionId: 'sess-agree', parentMessageId: 'msg-caller',
            childMessageId: 'msg-child', taskGroupId: 'tg-agree',
            timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
        });

        const decision = await consumeWaitEntry(redis as any, command);

        expect(decision.member).toBe(memberFromResume(command));
    });
});

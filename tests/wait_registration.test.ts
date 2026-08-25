import { AgentContext } from '../src/context';
import { AnonymousWorker } from '../src/worker';
import { WorkerRegistry } from '../src/registry';
import { PluginRegistry } from '../src/extensions/registry';
import { RoutePolicy } from '../src/availability';
import { AgentState } from '../src/protocol/agent_state';
import { AskAgentCommand, ResumeCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import {
    DEFAULT_ASK_USER_TIMEOUT_MS,
    DEFAULT_REPLY_TIMEOUT_MS,
    QueueNames,
    TASK_GROUP_TTL_SECONDS,
    singleCallTaskGroupId,
} from '../src/constants';
import {
    decodeMember,
    encodeMember,
    memberFromResume,
    waitIndexKey,
} from '../src/liveness/wait_index';
import { consumedMarkerKey } from '../src/liveness/wait_gate';
import { registerWait } from '../src/liveness/wait_registration';
import { MockRedis, bringAgentTypeOnline } from './helpers/mock_redis';

/**
 * Writing side of the wait index (contract §1-§3, PRD D1/D2). Runs the REAL
 * AgentContext / dispatch pipeline / GatewayWorker over the shared in-memory
 * Redis, so the member a dispatch registers and the member a reply rebuilds are
 * produced by the production code paths rather than restated in the test.
 */

function context(redis: MockRedis, overrides: {
    sessionId?: string;
    agentType?: string;
    messageId?: string;
    currentCommand?: unknown;
} = {}): AgentContext {
    return new AgentContext(
        overrides.sessionId ?? 'sess-wait',
        'trace-wait',
        redis as any,
        overrides.agentType ?? 'caller-agent',
        overrides.messageId ?? 'msg-caller',
        overrides.currentCommand,
    );
}

async function waitEntries(redis: MockRedis, sessionId: string): Promise<string[]> {
    return redis.zrangebyscore(waitIndexKey(sessionId), '-inf', '+inf');
}

describe('callAgent registers one wait-index entry per dispatch', () => {
    test('member matches what the eventual reply rebuilds (AC-TS-2 round trip)', async () => {
        const redis = new MockRedis();
        const ctx = context(redis);

        const result = await ctx.callAgent({
            targetAgentType: 'child-agent',
            content: 'work',
            routePolicy: RoutePolicy.SEND_ANYWAY,
        });
        expect(result.status).toBe(AgentState.QUEUED);

        const entries = await waitEntries(redis, 'sess-wait');
        expect(entries).toHaveLength(1);
        expect(decodeMember(entries[0])).toEqual({
            sessionId: 'sess-wait',
            parentMessageId: 'msg-caller',
            childMessageId: result.messageId,
            taskGroupId: '',
        });

        // The reply a sub-agent will send, built by the REAL enqueueAgentReturn
        // shape: header.messageId = caller id, header.parentMessageId = child id.
        const reply = new ResumeCommand(
            new MessageHeader('msg-caller', 'sess-wait', 'trace-wait', {
                sourceAgentType: 'child-agent',
                targetAgentType: 'caller-agent',
                parentMessageId: result.messageId,
            }),
            '',
            AgentState.COMPLETED,
            null
        );
        expect(memberFromResume(reply)).toBe(entries[0]);
    });

    test('score is now + DEFAULT_REPLY_TIMEOUT_MS by default', async () => {
        const redis = new MockRedis();
        const before = Date.now();
        await context(redis).callAgent({
            targetAgentType: 'child-agent', content: 'work', routePolicy: RoutePolicy.SEND_ANYWAY,
        });
        const entries = await waitEntries(redis, 'sess-wait');
        const score = Number(await redis.zscore(waitIndexKey('sess-wait'), entries[0]));
        expect(score).toBeGreaterThanOrEqual(before + DEFAULT_REPLY_TIMEOUT_MS);
        expect(score).toBeLessThanOrEqual(Date.now() + DEFAULT_REPLY_TIMEOUT_MS);
    });

    test('replyTimeoutMs overrides the default deadline', async () => {
        const redis = new MockRedis();
        const before = Date.now();
        await context(redis).callAgent({
            targetAgentType: 'child-agent', content: 'work',
            routePolicy: RoutePolicy.SEND_ANYWAY, replyTimeoutMs: 5_000,
        });
        const entries = await waitEntries(redis, 'sess-wait');
        const score = Number(await redis.zscore(waitIndexKey('sess-wait'), entries[0]));
        expect(score).toBeGreaterThanOrEqual(before + 5_000);
        expect(score).toBeLessThan(before + DEFAULT_REPLY_TIMEOUT_MS);
    });

    test('waitForReply=false registers nothing — that caller does not suspend', async () => {
        const redis = new MockRedis();
        await context(redis).callAgent({
            targetAgentType: 'child-agent', content: 'work',
            waitForReply: false, routePolicy: RoutePolicy.SEND_ANYWAY,
        });
        expect(await waitEntries(redis, 'sess-wait')).toEqual([]);
    });

    test('a rejected dispatch registers nothing and leaves the context unsuspended', async () => {
        const redis = new MockRedis();
        const ctx = context(redis);
        // FAIL_FAST with no online worker: the availability check rejects
        // before anything is dispatched.
        const result = await ctx.callAgent({
            targetAgentType: 'offline-agent', content: 'work', routePolicy: RoutePolicy.FAIL_FAST,
        });
        expect(result.status).toBe(AgentState.FAILED);
        expect(await waitEntries(redis, 'sess-wait')).toEqual([]);
        // TS sets _isSuspended AFTER the rejection early-return, so unlike
        // Python it needs no snapshot/rollback here. Guard that property.
        expect(ctx.isSuspended()).toBe(false);
        expect(ctx.suspendedState()).toBe('');
    });

    test('registration happens before the ctrl publish', async () => {
        // A reply landing before the entry exists would pass the idempotency
        // gate as "never registered" and then leave the entry behind it with
        // nothing left to clear it.
        const redis = new MockRedis();
        const order: string[] = [];
        const realZadd = redis.zadd.bind(redis);
        const realXadd = redis.xadd.bind(redis);
        jest.spyOn(redis, 'zadd').mockImplementation(async (...args: any[]) => {
            order.push('zadd'); return (realZadd as any)(...args);
        });
        jest.spyOn(redis, 'xadd').mockImplementation(async (...args: any[]) => {
            order.push('xadd'); return (realXadd as any)(...args);
        });

        await context(redis).callAgent({
            targetAgentType: 'child-agent', content: 'work', routePolicy: RoutePolicy.SEND_ANYWAY,
        });

        expect(order.indexOf('zadd')).toBeGreaterThanOrEqual(0);
        expect(order.indexOf('zadd')).toBeLessThan(order.indexOf('xadd'));
        jest.restoreAllMocks();
    });

    test('is fail-soft: a Redis error registering does not break the dispatch', async () => {
        const redis = new MockRedis();
        jest.spyOn(redis, 'zadd').mockRejectedValue(new Error('redis down'));
        const warn = jest.spyOn(console, 'warn').mockImplementation(() => undefined);

        const result = await context(redis).callAgent({
            targetAgentType: 'child-agent', content: 'work', routePolicy: RoutePolicy.SEND_ANYWAY,
        });

        expect(result.status).toBe(AgentState.QUEUED);
        // The dispatch itself still went out — bookkeeping must never break it.
        expect(redis.getStreamPayloads(QueueNames.ctrl_stream('child-agent'))).toHaveLength(1);
        expect(warn).toHaveBeenCalled();
        jest.restoreAllMocks();
    });

    test('the dispatch execution record carries task_group_id', async () => {
        // _resolve_reply_command reads it back off the record when this
        // sub-task's own execution later suspends and resumes.
        const redis = new MockRedis();
        const registry = new WorkerRegistry(redis as any);
        const currentCommand = new AskAgentCommand(
            new MessageHeader('msg-caller', 'sess-wait', 'trace-wait', { taskGroupId: 'tg-outer' }),
            'current'
        );
        const ctx = context(redis, { currentCommand });
        const result = await ctx.callAgent({
            targetAgentType: 'child-agent', content: 'work', routePolicy: RoutePolicy.SEND_ANYWAY,
        });
        const record = await registry.getExecutionByMessageId(result.messageId, 'sess-wait');
        expect(record!.task_group_id).toBe('tg-outer');
    });
});

describe('dispatchGroup registers one entry per sibling', () => {
    test('siblings get distinct members carrying the group id', async () => {
        const redis = new MockRedis();
        // Every target must be online, or the fan-out compensates the member
        // with a stand-in instead of dispatching it — a different code path that
        // happens to register a wait entry too, which would make this assertion
        // pass for the wrong reason.
        await bringAgentTypeOnline(redis, 'a-agent');
        await bringAgentTypeOnline(redis, 'b-agent');
        const ctx = context(redis);

        const group = await ctx.dispatchGroup({
            tasks: [
                { targetAgentType: 'a-agent', content: 'a' },
                { targetAgentType: 'b-agent', content: 'b' },
            ],
        });

        const entries = await waitEntries(redis, 'sess-wait');
        expect(entries).toHaveLength(2);
        const decoded = entries.map(decodeMember).sort(
            (x, y) => x.childMessageId.localeCompare(y.childMessageId)
        );
        const childIds = group.dispatchedTasks.map(t => t.message_id).sort();
        expect(decoded.map(d => d.childMessageId).sort()).toEqual(childIds);
        for (const member of decoded) {
            expect(member.sessionId).toBe('sess-wait');
            expect(member.parentMessageId).toBe('msg-caller');
            expect(member.taskGroupId).toBe(group.taskGroupId);
        }
    });

    test('a shared parentMessageId does not collapse the siblings onto one entry', async () => {
        // parentMessageId is identical across siblings by construction; only
        // childMessageId distinguishes them. If the member dropped it, one ZREM
        // would claim the whole group and the caller would hang short of total.
        const redis = new MockRedis();
        for (const agentType of ['a-agent', 'b-agent', 'c-agent']) {
            await bringAgentTypeOnline(redis, agentType);
        }
        await context(redis).dispatchGroup({
            tasks: [
                { targetAgentType: 'a-agent', content: 'a' },
                { targetAgentType: 'b-agent', content: 'b' },
                { targetAgentType: 'c-agent', content: 'c' },
            ],
        });
        expect(new Set(await waitEntries(redis, 'sess-wait')).size).toBe(3);
    });

    test('waitForReply=false registers nothing', async () => {
        const redis = new MockRedis();
        await context(redis).dispatchGroup({
            tasks: [{ targetAgentType: 'a-agent', content: 'a' }],
            waitForReply: false,
        });
        expect(await waitEntries(redis, 'sess-wait')).toEqual([]);
    });

    test('each sibling execution record carries the group id', async () => {
        const redis = new MockRedis();
        await bringAgentTypeOnline(redis, 'a-agent');
        const registry = new WorkerRegistry(redis as any);
        const group = await context(redis).dispatchGroup({
            tasks: [{ targetAgentType: 'a-agent', content: 'a' }],
        });
        const record = await registry.getExecutionByMessageId(
            group.dispatchedTasks[0].message_id, 'sess-wait'
        );
        expect(record!.task_group_id).toBe(group.taskGroupId);
    });
});

describe('askUser registers a wait with an empty childMessageId', () => {
    test('member shape and default deadline follow the contract', async () => {
        const redis = new MockRedis();
        const before = Date.now();
        await context(redis).askUser('what next?');

        const entries = await waitEntries(redis, 'sess-wait');
        expect(entries).toHaveLength(1);
        expect(decodeMember(entries[0])).toEqual({
            sessionId: 'sess-wait',
            // The client's ResumeCommand carries this as header.messageId.
            parentMessageId: 'msg-caller',
            // No sub-task exists, which is exactly why the gate needs a second
            // candidate for askUser instead of rebuilding this one exactly.
            childMessageId: '',
            taskGroupId: '',
        });
        const score = Number(await redis.zscore(waitIndexKey('sess-wait'), entries[0]));
        expect(score).toBeGreaterThanOrEqual(before + DEFAULT_ASK_USER_TIMEOUT_MS);
    });

    test('replyTimeoutMs overrides the human-scale default', async () => {
        const redis = new MockRedis();
        const before = Date.now();
        await context(redis).askUser('what next?', { replyTimeoutMs: 1_000 });
        const entries = await waitEntries(redis, 'sess-wait');
        const score = Number(await redis.zscore(waitIndexKey('sess-wait'), entries[0]));
        expect(score).toBeLessThan(before + DEFAULT_ASK_USER_TIMEOUT_MS);
    });

    test('registers before the prompt is emitted', async () => {
        // A human can answer the instant the prompt is visible.
        const redis = new MockRedis();
        const order: string[] = [];
        const realZadd = redis.zadd.bind(redis);
        const realXadd = redis.xadd.bind(redis);
        jest.spyOn(redis, 'zadd').mockImplementation(async (...args: any[]) => {
            order.push('zadd'); return (realZadd as any)(...args);
        });
        jest.spyOn(redis, 'xadd').mockImplementation(async (...args: any[]) => {
            order.push('xadd'); return (realXadd as any)(...args);
        });

        await context(redis).askUser('what next?');

        expect(order[0]).toBe('zadd');
        expect(order).toContain('xadd');
        jest.restoreAllMocks();
    });

    test('a second round clears the previous round\'s consumed marker', async () => {
        // Consecutive askUser rounds encode to the same member (no sub-task id
        // distinguishes them), so a stale "already consumed" verdict would make
        // the gate drop the next real answer.
        const redis = new MockRedis();
        const ctx = context(redis);
        await ctx.askUser('round 1');
        const member = (await waitEntries(redis, 'sess-wait'))[0];
        const markerKey = consumedMarkerKey('sess-wait', member);
        await redis.set(markerKey, '1');

        await ctx.askUser('round 2');

        expect(await redis.exists(markerKey)).toBe(0);
    });
});

describe('registerWait fail-soft contract', () => {
    test('a failing marker DEL still leaves the entry registered', async () => {
        // The entry is what matters; while it exists the marker is never read.
        const redis = new MockRedis();
        jest.spyOn(redis, 'del').mockRejectedValue(new Error('redis down'));
        const warn = jest.spyOn(console, 'warn').mockImplementation(() => undefined);

        await registerWait(redis as any, {
            sessionId: 'sess-soft',
            parentMessageId: 'msg-caller',
            childMessageId: 'msg-child',
            timeoutMs: 1000,
        });

        expect(await waitEntries(redis, 'sess-soft')).toEqual([
            encodeMember({
                sessionId: 'sess-soft',
                parentMessageId: 'msg-caller',
                childMessageId: 'msg-child',
                taskGroupId: '',
            }),
        ]);
        expect(warn).toHaveBeenCalled();
        jest.restoreAllMocks();
    });

    test('a negative timeout clamps to the current instant rather than the past', async () => {
        const redis = new MockRedis();
        const before = Date.now();
        await registerWait(redis as any, {
            sessionId: 'sess-soft', parentMessageId: 'p', childMessageId: 'c', timeoutMs: -5000,
        });
        const entries = await waitEntries(redis, 'sess-soft');
        const score = Number(await redis.zscore(waitIndexKey('sess-soft'), entries[0]));
        expect(score).toBeGreaterThanOrEqual(before);
    });
});

describe('single callAgent result is persisted before the reply goes out (D1)', () => {
    function buildWorker(redis: MockRedis) {
        return new AnonymousWorker({
            workerId: 'worker-persist',
            agentTypes: ['child-agent'],
            registry: new WorkerRegistry(redis as any),
            redisClient: redis as any,
            pluginRegistry: new PluginRegistry(),
            onTask: async () => ({ status: AgentState.COMPLETED, replyData: { answer: 42 }, content: 'done' }),
        });
    }

    function askCommand(taskGroupId = ''): AskAgentCommand {
        return new AskAgentCommand(
            new MessageHeader('msg-child', 'sess-persist', 'trace-persist', {
                sourceAgentType: 'caller-agent',
                targetAgentType: 'child-agent',
                parentMessageId: 'msg-caller',
                taskGroupId,
            }),
            'work',
            true
        );
    }

    test('stores the answer under the tg-single- group id, keyed by the sub-task id', async () => {
        const redis = new MockRedis();
        await buildWorker(redis).handleMessage(askCommand());

        const resultsKey = QueueNames.task_group_results(singleCallTaskGroupId('msg-child'));
        const stored = await redis.hgetall(resultsKey);
        expect(Object.keys(stored)).toEqual(['msg-child']);
        expect(JSON.parse(stored['msg-child'])).toMatchObject({
            status: AgentState.COMPLETED,
            reply_data: { answer: 42 },
            content: 'done',
            // The sub-agent that produced the result = the reply's sourceAgentType.
            target_agent_type: 'child-agent',
        });
        // Isomorphic with the group-join resultData: same six fields.
        expect(Object.keys(JSON.parse(stored['msg-child'])).sort()).toEqual([
            'content', 'extra_payload', 'metadata', 'reply_data', 'status', 'target_agent_type',
        ]);
        expect(await redis.ttl(resultsKey)).toBeLessThanOrEqual(TASK_GROUP_TTL_SECONDS);
        expect(await redis.ttl(resultsKey)).toBeGreaterThan(0);
    });

    test('is written before the reply is enqueued', async () => {
        // A reply that outruns its own result copy defeats the point: the
        // recovery path would find nothing to recover.
        const redis = new MockRedis();
        const order: string[] = [];
        const realHset = redis.hset.bind(redis);
        const realXadd = redis.xadd.bind(redis);
        jest.spyOn(redis, 'hset').mockImplementation(async (...args: any[]) => {
            if (String(args[0]).includes('tg-single-')) order.push('hset');
            return (realHset as any)(...args);
        });
        jest.spyOn(redis, 'xadd').mockImplementation(async (...args: any[]) => {
            order.push('xadd'); return (realXadd as any)(...args);
        });

        await buildWorker(redis).handleMessage(askCommand());

        expect(order.indexOf('hset')).toBeGreaterThanOrEqual(0);
        expect(order.indexOf('hset')).toBeLessThan(order.lastIndexOf('xadd'));
        jest.restoreAllMocks();
    });

    test('a real Task Group member is skipped — the join already stores it', async () => {
        const redis = new MockRedis();
        await buildWorker(redis).handleMessage(askCommand('tg-real'));
        expect(await redis.hgetall(
            QueueNames.task_group_results(singleCallTaskGroupId('msg-child'))
        )).toEqual({});
    });

    test('a persist failure still lets the reply go out', async () => {
        const redis = new MockRedis();
        jest.spyOn(redis, 'hset').mockImplementation(async (...args: any[]) => {
            if (String(args[0]).includes('tg-single-')) throw new Error('redis down');
            return 1 as any;
        });
        const warn = jest.spyOn(console, 'warn').mockImplementation(() => undefined);

        const result = await buildWorker(redis).handleMessage(askCommand());

        expect(result.status).toBe(AgentState.COMPLETED);
        expect(redis.getStreamPayloads(QueueNames.ctrl_stream('caller-agent'))).toHaveLength(1);
        expect(warn).toHaveBeenCalled();
        jest.restoreAllMocks();
    });
});

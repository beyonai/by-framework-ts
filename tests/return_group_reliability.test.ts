import { GatewayWorker } from '../src/worker';
import { AskAgentCommand, ResumeCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { AgentTaskResult } from '../src/protocol/results';
import { AgentState } from '../src/protocol/agent_state';
import { QueueNames, TASK_GROUP_FIELD_TOTAL, TASK_GROUP_EXPIRED, MAX_RETRY_COUNT } from '../src/constants';

class FlakyRedis {
    /** Attempts against the caller's ctrl stream only — emitter traffic is noise here. */
    public xaddAttempts = 0;
    public failXaddTimes = 0;
    public delivered: string[] = [];
    private readonly callerStream = QueueNames.ctrl_stream('caller');
    public hashes = new Map<string, Record<string, string>>();
    public completed = 0;

    async xadd(stream: string, _i: string, _f: string, payload: string): Promise<string> {
        if (stream !== this.callerStream) return '1-0';
        this.xaddAttempts++;
        if (this.xaddAttempts <= this.failXaddTimes) throw new Error('simulated XADD failure');
        this.delivered.push(payload);
        return '1-0';
    }
    async hget(key: string, field: string): Promise<string | null> {
        return this.hashes.get(key)?.[field] ?? null;
    }
    async hset(key: string, field: any, value?: string): Promise<number> {
        if (!this.hashes.has(key)) this.hashes.set(key, {});
        if (typeof field === 'string') this.hashes.get(key)![field] = value!;
        return 1;
    }
    async hgetall(): Promise<Record<string, string>> { return {}; }
    async hincrby(): Promise<number> { return ++this.completed; }
    async expire(): Promise<number> { return 1; }
    async get(): Promise<string | null> { return null; }
    async set(): Promise<'OK'> { return 'OK'; }
    async zrem(): Promise<number> { return 0; }
    async exists(): Promise<number> { return 0; }
    pipeline() {
        const self = this;
        const pipe: any = {
            xadd: (n: string, i: string, f: string, p: string) => { self.xadd(n, i, f, p); return pipe; },
            hset: () => pipe, expire: () => pipe, sadd: () => pipe, srem: () => pipe, del: () => pipe,
            exec: async () => [],
        };
        return pipe;
    }
}

class EchoWorker extends GatewayWorker {
    getAgentTypes(): ReadonlyArray<string> { return ['callee']; }
    async processCommand(): Promise<AgentTaskResult> {
        return new AgentTaskResult({ status: 'COMPLETED', replyData: { ok: true } });
    }
}

function subTask(): AskAgentCommand {
    return new AskAgentCommand(
        new MessageHeader('msg-child', 'sess-1', 'trace-1', {
            sourceAgentType: 'caller', targetAgentType: 'callee', parentMessageId: 'msg-caller',
        }),
        'work'
    );
}

describe('agent return delivery', () => {
    beforeEach(() => {
        jest.spyOn(console, 'log').mockImplementation(() => undefined);
        jest.spyOn(console, 'warn').mockImplementation(() => undefined);
        jest.spyOn(console, 'error').mockImplementation(() => undefined);
    });
    afterEach(() => jest.restoreAllMocks());

    test('a transient failure is retried rather than losing the caller wake-up', async () => {
        const redis = new FlakyRedis();
        redis.failXaddTimes = 1;
        const worker = new EchoWorker('worker-r', undefined, redis as any);

        await worker.handleMessage(subTask());

        // Delivered on the second attempt; without the retry the caller would
        // stay suspended until the wait sweep compensated it minutes later.
        expect(redis.xaddAttempts).toBe(2);
        expect(redis.delivered).toHaveLength(1);
        const reply = JSON.parse(redis.delivered[0]);
        expect(reply.header.message_id).toBe('msg-caller');
    }, 15_000);

    test('it gives up after a bounded number of attempts', async () => {
        const redis = new FlakyRedis();
        redis.failXaddTimes = 99;
        const worker = new EchoWorker('worker-r', undefined, redis as any);

        // The task itself reports FAILED; the message stays unacked upstream,
        // so PEL reclaim and the wait sweep remain the backstops.
        const result = await worker.handleMessage(subTask());

        expect(result.status).toBe(AgentState.FAILED);
        // Twice the retry budget, because the success path exhausts its
        // attempts and then the failure path tries to tell the caller it
        // failed, exhausting its own. Bounded either way: a fully dead Redis
        // costs this task ~6s and then releases the slot, rather than
        // retrying while the worker starves.
        expect(redis.xaddAttempts).toBe(MAX_RETRY_COUNT * 2);
        expect(redis.delivered).toHaveLength(0);
    }, 20_000);
});

describe('task group expiry semantics', () => {
    beforeEach(() => {
        jest.spyOn(console, 'log').mockImplementation(() => undefined);
        jest.spyOn(console, 'error').mockImplementation(() => undefined);
    });
    afterEach(() => jest.restoreAllMocks());

    function groupReply(): ResumeCommand {
        return new ResumeCommand(
            new MessageHeader('msg-caller', 'sess-1', 'trace-1', {
                sourceAgentType: 'callee', targetAgentType: 'caller',
                parentMessageId: 'msg-child-a', taskGroupId: 'tg-1',
            }),
            '', 'COMPLETED', { from: 'a' }
        );
    }

    test('a reply for an expired group fails loudly instead of resuming with one sibling', async () => {
        const redis = new FlakyRedis(); // no group tracker seeded => expired
        const worker = new EchoWorker('worker-r', undefined, redis as any);

        const result = await worker.handleMessage(groupReply());

        expect(result.status).toBe(AgentState.FAILED);
        expect((result.replyData as any).error_code).toBe(TASK_GROUP_EXPIRED);
        expect((result.replyData as any).task_group_id).toBe('tg-1');
    });

    test('a live group still joins normally', async () => {
        const redis = new FlakyRedis();
        await redis.hset(QueueNames.task_group('tg-1'), TASK_GROUP_FIELD_TOTAL, '2');
        const worker = new EchoWorker('worker-r', undefined, redis as any);

        const result = await worker.handleMessage(groupReply());

        // First of two siblings: the caller stays suspended rather than resuming.
        expect(result.status).toBe(`${AgentState.WAITING_AGENT}: waiting_for_group`);
    });
});

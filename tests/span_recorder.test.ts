import { RedisSpanExporter, TraceSpan } from '../src/trace/span_recorder';
import { QueueNames } from '../src/constants';

class MockRedis {
    private hashes = new Map<string, Record<string, string>>();
    private lists = new Map<string, string[]>();
    private zsets = new Map<string, Map<string, number>>();
    zaddShouldThrow = false;

    async hget(key: string, field: string): Promise<string | null> {
        return this.hashes.get(key)?.[field] ?? null;
    }

    async hgetall(key: string): Promise<Record<string, string>> {
        return { ...(this.hashes.get(key) || {}) };
    }

    private hset(key: string, field: string, value: string): number {
        if (!this.hashes.has(key)) this.hashes.set(key, {});
        this.hashes.get(key)![field] = String(value);
        return 1;
    }

    private rpush(key: string, value: string): number {
        if (!this.lists.has(key)) this.lists.set(key, []);
        this.lists.get(key)!.push(value);
        return this.lists.get(key)!.length;
    }

    async lrange(key: string, start: number, end: number): Promise<string[]> {
        const list = this.lists.get(key) || [];
        return list.slice(start, end === -1 ? undefined : end + 1);
    }

    async expire(_key: string, _seconds: number): Promise<number> {
        return 1;
    }

    async zadd(key: string, score: number, member: string): Promise<number> {
        if (this.zaddShouldThrow) {
            throw new Error('simulated CROSSSLOT failure');
        }
        if (!this.zsets.has(key)) this.zsets.set(key, new Map());
        this.zsets.get(key)!.set(member, score);
        return 1;
    }

    hasZsetMember(key: string, member: string): boolean {
        return this.zsets.get(key)?.has(member) ?? false;
    }

    pipeline() {
        const self = this;
        const commands: (() => void)[] = [];
        const pipe = {
            hset: (key: string, field: string, value: string) => {
                commands.push(() => self.hset(key, field, value));
                return pipe;
            },
            rpush: (key: string, value: string) => {
                commands.push(() => self.rpush(key, value));
                return pipe;
            },
            expire: (key: string, seconds: number) => {
                commands.push(() => self.expire(key, seconds));
                return pipe;
            },
            exec: async () => {
                for (const cmd of commands) cmd();
                return [];
            },
        };
        return pipe;
    }
}

function makeSpan(overrides: Partial<TraceSpan> = {}): TraceSpan {
    return {
        traceId: 'trace-1',
        spanId: 'span-1',
        parentSpanId: '',
        operation: 'client.dispatch',
        component: 'client',
        startTs: 1000,
        endTs: 2000,
        status: 'OK',
        sessionId: 'sess-1',
        workerId: 'worker-1',
        targetAgentType: 'chat',
        ...overrides,
    };
}

describe('RedisSpanExporter.exportSpan', () => {
    test('writes trace meta + spans and all three lookup indexes', async () => {
        const redis = new MockRedis();
        const exporter = new RedisSpanExporter(redis as any, 900);

        await exporter.exportSpan(makeSpan());

        const metaKey = QueueNames.trace_meta('trace-1');
        const spansKey = QueueNames.trace_spans('trace-1');
        expect(await redis.hget(metaKey, 'trace_id')).toBe('trace-1');
        expect(await redis.lrange(spansKey, 0, -1)).toHaveLength(1);
        expect(redis.hasZsetMember(QueueNames.trace_index_session('sess-1'), 'trace-1')).toBe(true);
        expect(redis.hasZsetMember(QueueNames.trace_index_worker('worker-1'), 'trace-1')).toBe(true);
        expect(redis.hasZsetMember(QueueNames.trace_index_agent('chat'), 'trace-1')).toBe(true);
    });

    test('a lookup-index write failure does not prevent trace meta/spans from being read back', async () => {
        const redis = new MockRedis();
        redis.zaddShouldThrow = true;
        const exporter = new RedisSpanExporter(redis as any, 900);

        await expect(exporter.exportSpan(makeSpan())).resolves.toBeUndefined();

        const metaKey = QueueNames.trace_meta('trace-1');
        const spansKey = QueueNames.trace_spans('trace-1');
        expect(await redis.hget(metaKey, 'trace_id')).toBe('trace-1');
        expect(await redis.hget(metaKey, 'session_id')).toBe('sess-1');
        expect(await redis.lrange(spansKey, 0, -1)).toHaveLength(1);
    });
});

import { WorkerRunner } from '../src/runner';
import {
    QueueNames, RegistryKeys, ORPHAN_RECLAIM_LEASE_MULTIPLE, DEAD_WORKER_CLEANUP_BATCH, deadWorkerCleanupIdleMs,
} from '../src/constants';
import { parseXinfoConsumers } from '../src/runner';
import { AskAgentCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';

const IDLE_THRESHOLD_MS = RegistryKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS * 1000 * ORPHAN_RECLAIM_LEASE_MULTIPLE;

type PendingEntry = [string, string, number, number];

class ReclaimRedis {
    /** stream -> pending entries as XPENDING returns them. */
    public pending = new Map<string, PendingEntry[]>();
    /** stream -> msgId -> serialized command. */
    public entries = new Map<string, Map<string, string>>();
    public claimCalls: Array<{ stream: string; consumer: string; ids: string[] }> = [];
    public failXpending = false;
    public failXinfo = false;
    /** stream -> consumer name -> idle ms, for this runner's group ('g'). */
    public consumers = new Map<string, Map<string, number>>();
    public delConsumerCalls: Array<{ stream: string; group: string; consumer: string }> = [];
    public deleted: string[] = [];
    public groupCreates: string[] = [];

    duplicate(): ReclaimRedis { return this; }
    async quit(): Promise<'OK'> { return 'OK'; }
    async xreadgroup(): Promise<null> { return null; }

    async xgroup(sub: string, stream: string, group: string, consumer?: string): Promise<'OK' | number> {
        if (sub === 'CREATE') {
            this.groupCreates.push(stream);
            return 'OK';
        }
        if (sub === 'DELCONSUMER') {
            this.delConsumerCalls.push({ stream, group, consumer: consumer! });
            const dropped = this.pendingOf(stream, consumer!);
            this.consumers.get(stream)?.delete(consumer!);
            this.pending.set(stream, (this.pending.get(stream) ?? []).filter(e => e[1] !== consumer));
            return dropped;
        }
        return 'OK';
    }

    async xinfo(sub: string, stream: string, _group: string): Promise<unknown[]> {
        if (this.failXinfo) throw new Error('simulated XINFO failure');
        expect(sub).toBe('CONSUMERS');
        // Redis 7.2 shape, including `inactive`, to prove fields are read by name.
        return [...(this.consumers.get(stream) ?? new Map()).entries()].map(([name, idle]) => [
            'name', name, 'pending', this.pendingOf(stream, name), 'idle', idle, 'inactive', idle,
        ]);
    }

    async del(key: string): Promise<number> {
        this.deleted.push(key);
        return 1;
    }

    async xpending(stream: string, ...args: any[]): Promise<PendingEntry[] | null> {
        if (this.failXpending) throw new Error('simulated XPENDING failure');
        const all = this.pending.get(stream) ?? [];
        // Extended form with a consumer filter: group, '-', '+', count, consumer.
        if (args.length >= 5) return all.filter(e => e[1] === args[4]);
        return all.length ? all : null;
    }

    async xclaim(stream: string, _group: string, consumer: string, _idle: number, ...ids: string[]): Promise<[string, string[]][]> {
        this.claimCalls.push({ stream, consumer, ids });
        const out: [string, string[]][] = [];
        for (const id of ids) {
            const payload = this.entries.get(stream)?.get(id);
            if (payload) out.push([id, ['data', payload]]);
            // Ownership moves to the claimant, as in Redis.
            for (const entry of this.pending.get(stream) ?? []) {
                if (entry[0] === id) entry[1] = consumer;
            }
        }
        return out;
    }

    pendingOf(stream: string, consumer: string): number {
        return (this.pending.get(stream) ?? []).filter(e => e[1] === consumer).length;
    }
}

class ReclaimRegistry {
    public online = new Set<string>();
    public forgotten: string[] = [];
    public releaseResult = true;
    async isWorkerOnline(workerId: string): Promise<boolean> {
        return this.online.has(workerId);
    }
    public failForget = false;
    async forgetWorker(workerId: string): Promise<void> {
        if (this.failForget) throw new Error('simulated forgetWorker failure');
        this.forgotten.push(workerId);
    }
    async markWorkerInactive(): Promise<boolean> {
        return this.releaseResult;
    }
    async unregisterWorkerMembership(): Promise<void> {}
}

function makeRunner(redis: ReclaimRedis, registry: ReclaimRegistry): WorkerRunner {
    const worker = {
        workerId: 'worker-live',
        getAgentTypes: () => ['probe'],
        registry: registry as any,
        get heartbeatInterval() { return RegistryKeys.WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS; },
        get heartbeatLeaseTtlSeconds() { return RegistryKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS; },
        startHeartbeat: async () => {},
        stopHeartbeat: async () => {},
        handleMessage: async () => { throw new Error('unused'); },
    } as any;
    return new WorkerRunner(worker, { redisClient: redis as any, groupName: 'g' });
}

function seedPending(
    redis: ReclaimRedis,
    params: { msgId: string; consumer: string; idleMs: number }
): void {
    const stream = QueueNames.ctrl_stream('probe');
    const command = new AskAgentCommand(
        new MessageHeader(params.msgId, 'sess-1', 'trace-1', {
            sourceAgentType: 'caller', targetAgentType: 'probe',
        }),
        'work'
    );
    if (!redis.pending.has(stream)) redis.pending.set(stream, []);
    redis.pending.get(stream)!.push([params.msgId, params.consumer, params.idleMs, 1]);
    if (!redis.entries.has(stream)) redis.entries.set(stream, new Map());
    redis.entries.get(stream)!.set(params.msgId, JSON.stringify(command.toDict()));
}

/** Force the rate-limited reclaim to run on the next poll. */
function armReclaim(runner: WorkerRunner): void {
    (runner as any).reclaimCountdown = 1;
}

describe('orphaned message reclaim', () => {
    let redis: ReclaimRedis;
    let registry: ReclaimRegistry;
    let runner: WorkerRunner;

    beforeEach(() => {
        redis = new ReclaimRedis();
        registry = new ReclaimRegistry();
        runner = makeRunner(redis, registry);
        jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    });
    afterEach(() => jest.restoreAllMocks());

    test('claims a long-idle message whose owner is gone', async () => {
        seedPending(redis, { msgId: '1-1', consumer: 'worker-dead', idleMs: IDLE_THRESHOLD_MS + 1 });
        armReclaim(runner);

        const messages = await runner.poll({ block: 1 });

        expect(messages).toHaveLength(1);
        expect(messages[0].msgId).toBe('1-1');
        expect(redis.claimCalls[0].consumer).toBe('worker-live');
    });

    test('leaves a message alone while its owner is still alive', async () => {
        // The whole reason for checking the lease rather than idle time alone:
        // a worker running a long task would otherwise have its work executed
        // a second time.
        registry.online.add('worker-busy');
        seedPending(redis, { msgId: '1-1', consumer: 'worker-busy', idleMs: IDLE_THRESHOLD_MS * 10 });
        armReclaim(runner);

        expect(await runner.poll({ block: 1 })).toHaveLength(0);
        expect(redis.claimCalls).toHaveLength(0);
    });

    test('leaves a message that has not been idle long enough', async () => {
        seedPending(redis, { msgId: '1-1', consumer: 'worker-dead', idleMs: IDLE_THRESHOLD_MS - 1 });
        armReclaim(runner);

        expect(await runner.poll({ block: 1 })).toHaveLength(0);
        expect(redis.claimCalls).toHaveLength(0);
    });

    test('never reclaims its own in-flight message', async () => {
        seedPending(redis, { msgId: '1-1', consumer: 'worker-live', idleMs: IDLE_THRESHOLD_MS * 10 });
        armReclaim(runner);

        expect(await runner.poll({ block: 1 })).toHaveLength(0);
        expect(redis.claimCalls).toHaveLength(0);
    });

    test('reclaim runs only every N idle polls, not on every one', async () => {
        seedPending(redis, { msgId: '1-1', consumer: 'worker-dead', idleMs: IDLE_THRESHOLD_MS + 1 });
        (runner as any).reclaimCountdown = 3;

        expect(await runner.poll({ block: 1 })).toHaveLength(0);
        expect(await runner.poll({ block: 1 })).toHaveLength(0);
        expect(await runner.poll({ block: 1 })).toHaveLength(1);
    });

    test('a reclaim failure does not stop the worker from consuming', async () => {
        redis.failXpending = true;
        armReclaim(runner);

        await expect(runner.poll({ block: 1 })).resolves.toEqual([]);
    });

    test('keeps the consumer when forgetting the worker fails, so a later sweep retries', async () => {
        seedConsumer(redis, 'worker-dead', CLEANUP_IDLE_MS + 1);
        registry.failForget = true;
        armReclaim(runner);
        await runner.poll({ block: 1 });
        expect(redis.delConsumerCalls).toHaveLength(0);

        registry.failForget = false;
        armReclaim(runner);
        await runner.poll({ block: 1 });
        expect(redis.delConsumerCalls.map(c => c.consumer)).toEqual(['worker-dead']);
        expect(registry.forgotten).toEqual(['worker-dead']);
    });

    test('skips streams for denied agent types', async () => {
        seedPending(redis, { msgId: '1-1', consumer: 'worker-dead', idleMs: IDLE_THRESHOLD_MS + 1 });
        (runner as any).deniedAgentTypes = new Set(['probe']);
        armReclaim(runner);

        expect(await runner.poll({ block: 1 })).toHaveLength(0);
    });
});

const STREAM = QueueNames.ctrl_stream('probe');
const CLEANUP_IDLE_MS = deadWorkerCleanupIdleMs();

function seedConsumer(redis: ReclaimRedis, name: string, idleMs: number): void {
    if (!redis.consumers.has(STREAM)) redis.consumers.set(STREAM, new Map());
    redis.consumers.get(STREAM)!.set(name, idleMs);
}

describe('dead worker cleanup', () => {
    let redis: ReclaimRedis;
    let registry: ReclaimRegistry;
    let runner: WorkerRunner;

    beforeEach(() => {
        redis = new ReclaimRedis();
        registry = new ReclaimRegistry();
        runner = makeRunner(redis, registry);
        jest.spyOn(console, 'warn').mockImplementation(() => undefined);
        jest.spyOn(console, 'log').mockImplementation(() => undefined);
    });
    afterEach(() => jest.restoreAllMocks());

    test('removes an offline, idle consumer with nothing pending, and forgets the worker', async () => {
        seedConsumer(redis, 'worker-live', 0);
        seedConsumer(redis, 'worker-dead', CLEANUP_IDLE_MS + 1);
        armReclaim(runner);

        await runner.poll({ block: 1 });

        expect(redis.delConsumerCalls).toEqual([{ stream: STREAM, group: 'g', consumer: 'worker-dead' }]);
        expect(registry.forgotten).toEqual(['worker-dead']);
    });

    test('removes a dead consumer once reclaim has taken its messages, keeping them', async () => {
        seedConsumer(redis, 'worker-live', 0);
        seedConsumer(redis, 'worker-dead', CLEANUP_IDLE_MS + 1);
        seedPending(redis, { msgId: '1-1', consumer: 'worker-dead', idleMs: CLEANUP_IDLE_MS + 1 });
        armReclaim(runner);

        const messages = await runner.poll({ block: 1 });

        // XCLAIM moved the entry to us, so the dead consumer's pending list is
        // empty by the time cleanup looks, and deleting it drops nothing.
        expect(messages).toHaveLength(1);
        expect(redis.delConsumerCalls.map(c => c.consumer)).toEqual(['worker-dead']);
        expect(redis.pendingOf(STREAM, 'worker-live')).toBe(1);
        expect(registry.forgotten).toEqual(['worker-dead']);
    });

    test('keeps a dead consumer whose pending messages reclaim did not take', async () => {
        // Too young for reclaim, old enough for cleanup by idle alone: the
        // pending check, not ordering luck, is what protects the message.
        seedConsumer(redis, 'worker-dead', CLEANUP_IDLE_MS + 1);
        seedPending(redis, { msgId: '1-1', consumer: 'worker-dead', idleMs: 0 });
        armReclaim(runner);

        await runner.poll({ block: 1 });

        expect(redis.delConsumerCalls).toHaveLength(0);
        expect(registry.forgotten).toHaveLength(0);
    });

    test('keeps online consumers, recently idle consumers, and itself', async () => {
        registry.online.add('worker-online');
        seedConsumer(redis, 'worker-online', CLEANUP_IDLE_MS * 10);
        seedConsumer(redis, 'worker-recent', CLEANUP_IDLE_MS - 1);
        seedConsumer(redis, 'worker-live', CLEANUP_IDLE_MS * 10);
        armReclaim(runner);

        await runner.poll({ block: 1 });

        expect(redis.delConsumerCalls).toHaveLength(0);
        expect(registry.forgotten).toHaveLength(0);
    });

    test('removes at most a batch per stream per sweep', async () => {
        for (let i = 0; i < DEAD_WORKER_CLEANUP_BATCH + 5; i++) {
            seedConsumer(redis, `worker-dead-${i}`, CLEANUP_IDLE_MS + 1);
        }
        armReclaim(runner);
        await runner.poll({ block: 1 });
        expect(redis.delConsumerCalls).toHaveLength(DEAD_WORKER_CLEANUP_BATCH);

        armReclaim(runner);
        await runner.poll({ block: 1 });
        expect(redis.delConsumerCalls).toHaveLength(DEAD_WORKER_CLEANUP_BATCH + 5);
    });

    test('a cleanup failure does not stop reclaim or consumption', async () => {
        redis.failXinfo = true;
        seedPending(redis, { msgId: '1-1', consumer: 'worker-dead', idleMs: IDLE_THRESHOLD_MS + 1 });
        armReclaim(runner);

        const messages = await runner.poll({ block: 1 });

        expect(messages).toHaveLength(1);
        expect(redis.delConsumerCalls).toHaveLength(0);
    });

    test('keeps the consumer when forgetting the worker fails, so a later sweep retries', async () => {
        seedConsumer(redis, 'worker-dead', CLEANUP_IDLE_MS + 1);
        registry.failForget = true;
        armReclaim(runner);
        await runner.poll({ block: 1 });
        expect(redis.delConsumerCalls).toHaveLength(0);

        registry.failForget = false;
        armReclaim(runner);
        await runner.poll({ block: 1 });
        expect(redis.delConsumerCalls.map(c => c.consumer)).toEqual(['worker-dead']);
        expect(registry.forgotten).toEqual(['worker-dead']);
    });

    test('skips streams for denied agent types', async () => {
        seedConsumer(redis, 'worker-dead', CLEANUP_IDLE_MS + 1);
        (runner as any).deniedAgentTypes = new Set(['probe']);
        armReclaim(runner);

        await runner.poll({ block: 1 });

        expect(redis.delConsumerCalls).toHaveLength(0);
    });
});

describe('graceful release cleans up after itself', () => {
    let redis: ReclaimRedis;
    let registry: ReclaimRegistry;
    let runner: WorkerRunner;

    beforeEach(() => {
        redis = new ReclaimRedis();
        registry = new ReclaimRegistry();
        runner = makeRunner(redis, registry);
        jest.spyOn(console, 'warn').mockImplementation(() => undefined);
    });
    afterEach(() => jest.restoreAllMocks());

    test('removes its own consumer and control stream', async () => {
        seedConsumer(redis, 'worker-live', 0);

        await runner.release();

        expect(redis.delConsumerCalls).toEqual([{ stream: STREAM, group: 'g', consumer: 'worker-live' }]);
        expect(redis.deleted).toContain(QueueNames.worker_ctrl_stream('worker-live'));
    });

    test('keeps its consumer while it still has pending messages', async () => {
        seedConsumer(redis, 'worker-live', 0);
        seedPending(redis, { msgId: '1-1', consumer: 'worker-live', idleMs: 0 });

        await runner.release();

        expect(redis.delConsumerCalls).toHaveLength(0);
        expect(redis.deleted).toContain(QueueNames.worker_ctrl_stream('worker-live'));
    });

    test('touches nothing when the worker id was not ours to release', async () => {
        registry.releaseResult = false;
        seedConsumer(redis, 'worker-live', 0);

        await runner.release();

        expect(redis.delConsumerCalls).toHaveLength(0);
        expect(redis.deleted).toHaveLength(0);
    });
});

describe('control loop recovers a deleted control stream', () => {
    test('recreates the group on NOGROUP and keeps going', async () => {
        const redis = new ReclaimRedis();
        const runner = makeRunner(redis, new ReclaimRegistry());
        jest.spyOn(console, 'warn').mockImplementation(() => undefined);
        let calls = 0;
        (redis as any).xreadgroup = async () => {
            if (calls++ === 0) {
                throw new Error("NOGROUP No such key 'x' or consumer group 'g' in XREADGROUP with GROUP option");
            }
            return null;
        };

        await expect(runner.runControlOnce(1)).resolves.toEqual([]);
        expect(redis.groupCreates).toEqual([QueueNames.worker_ctrl_stream('worker-live')]);
        await expect(runner.runControlOnce(1)).resolves.toEqual([]);
        jest.restoreAllMocks();
    });

    test('other read errors still propagate', async () => {
        const redis = new ReclaimRedis();
        const runner = makeRunner(redis, new ReclaimRegistry());
        (redis as any).xreadgroup = async () => { throw new Error('connection lost'); };

        await expect(runner.runControlOnce(1)).rejects.toThrow('connection lost');
        expect(redis.groupCreates).toHaveLength(0);
    });
});

describe('deadWorkerCleanupIdleMs', () => {
    const saved = process.env.BYAI_DEAD_WORKER_CLEANUP_IDLE_SECONDS;
    afterEach(() => {
        if (saved === undefined) delete process.env.BYAI_DEAD_WORKER_CLEANUP_IDLE_SECONDS;
        else process.env.BYAI_DEAD_WORKER_CLEANUP_IDLE_SECONDS = saved;
    });

    test('defaults to one hour', () => {
        delete process.env.BYAI_DEAD_WORKER_CLEANUP_IDLE_SECONDS;
        expect(deadWorkerCleanupIdleMs()).toBe(3600 * 1000);
    });

    test('follows the environment variable', () => {
        process.env.BYAI_DEAD_WORKER_CLEANUP_IDLE_SECONDS = '7200';
        expect(deadWorkerCleanupIdleMs()).toBe(7200 * 1000);
    });

    test('never drops below the orphan reclaim threshold', () => {
        process.env.BYAI_DEAD_WORKER_CLEANUP_IDLE_SECONDS = '1';
        expect(deadWorkerCleanupIdleMs()).toBe(IDLE_THRESHOLD_MS);
    });
});

describe('parseXinfoConsumers', () => {
    test('reads fields by name across reply shapes', () => {
        expect(parseXinfoConsumers([
            ['name', 'a', 'pending', 2, 'idle', 10],
            ['name', 'b', 'pending', 0, 'idle', 5, 'inactive', 5],
            'garbage',
            ['name', 'no-counts'],
            ['name', 'bad', 'pending', 'x', 'idle', 1],
        ])).toEqual([
            { name: 'a', pending: 2, idleMs: 10 },
            { name: 'b', pending: 0, idleMs: 5 },
        ]);
        expect(parseXinfoConsumers(null)).toEqual([]);
    });
});

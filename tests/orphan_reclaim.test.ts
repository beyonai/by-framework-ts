import { WorkerRunner } from '../src/runner';
import { QueueNames, RegistryKeys, ORPHAN_RECLAIM_LEASE_MULTIPLE } from '../src/constants';
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

    duplicate(): ReclaimRedis { return this; }
    async quit(): Promise<'OK'> { return 'OK'; }
    async xgroup(): Promise<'OK'> { return 'OK'; }
    async xreadgroup(): Promise<null> { return null; }

    async xpending(stream: string, ..._args: any[]): Promise<PendingEntry[] | null> {
        if (this.failXpending) throw new Error('simulated XPENDING failure');
        return this.pending.get(stream) ?? null;
    }

    async xclaim(stream: string, _group: string, consumer: string, _idle: number, ...ids: string[]): Promise<[string, string[]][]> {
        this.claimCalls.push({ stream, consumer, ids });
        const out: [string, string[]][] = [];
        for (const id of ids) {
            const payload = this.entries.get(stream)?.get(id);
            if (payload) out.push([id, ['data', payload]]);
        }
        return out;
    }
}

class ReclaimRegistry {
    public online = new Set<string>();
    async isWorkerOnline(workerId: string): Promise<boolean> {
        return this.online.has(workerId);
    }
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

    test('skips streams for denied agent types', async () => {
        seedPending(redis, { msgId: '1-1', consumer: 'worker-dead', idleMs: IDLE_THRESHOLD_MS + 1 });
        (runner as any).deniedAgentTypes = new Set(['probe']);
        armReclaim(runner);

        expect(await runner.poll({ block: 1 })).toHaveLength(0);
    });
});

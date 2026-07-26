import { EventEmitter } from 'events';

class FakeWorkerThread extends EventEmitter {
    public terminated = false;
    public posted: any[] = [];
    static instances: FakeWorkerThread[] = [];

    constructor(public workerPath: string, public options: any) {
        super();
        FakeWorkerThread.instances.push(this);
    }

    postMessage(msg: any): void {
        this.posted.push(msg);
    }

    async terminate(): Promise<number> {
        this.terminated = true;
        return 0;
    }
}

jest.mock('worker_threads', () => ({
    Worker: FakeWorkerThread,
}));

// Imported after the mock so heartbeat.ts picks up the fake Worker class.
import { WorkerHeartbeat } from '../src/heartbeat';

class FakeRegistry {
    adminState: Record<string, string> = {};
    membershipCalls: Array<[string, string[]]> = [];
    heartbeatCalls: Array<[string, number]> = [];

    async getWorkerAdminState(): Promise<Record<string, string>> {
        return this.adminState;
    }

    async registerWorkerMembership(workerId: string, agentTypes: string[]): Promise<void> {
        this.membershipCalls.push([workerId, agentTypes]);
    }

    async heartbeatWorker(workerId: string, leaseTtlSeconds: number): Promise<boolean> {
        this.heartbeatCalls.push([workerId, leaseTtlSeconds]);
        return true;
    }

    async isWorkerDeniedForType(): Promise<boolean> {
        return false;
    }
}

function fakeRedis(): any {
    return { options: { host: '127.0.0.1', port: 6379, db: 0 } };
}

describe('WorkerHeartbeat (worker_threads)', () => {
    beforeEach(() => {
        FakeWorkerThread.instances.length = 0;
    });

    test('start() spawns a worker thread carrying redis connection options, not a live client', async () => {
        const registry = new FakeRegistry();
        const heartbeat = new WorkerHeartbeat(
            'worker-1', ['agent-a'], fakeRedis(), registry as any, 5000, 30
        );

        await heartbeat.start();

        expect(FakeWorkerThread.instances).toHaveLength(1);
        const worker = FakeWorkerThread.instances[0];
        expect(worker.options.workerData.workerId).toBe('worker-1');
        expect(worker.options.workerData.agentTypes).toEqual(['agent-a']);
        expect(worker.options.workerData.leaseTtlSeconds).toBe(30);
        expect(worker.options.workerData.redisOptions).toEqual({
            mode: 'standalone', host: '127.0.0.1', port: 6379, db: 0, username: undefined, password: undefined,
        });
        // The live Redis client itself must never cross the thread boundary.
        expect(JSON.stringify(worker.options.workerData)).not.toContain('"options"'); // no nested client
    });

    test('performs the same synchronous startup sequence as before (admin state, membership, initial heartbeat)', async () => {
        const registry = new FakeRegistry();
        const heartbeat = new WorkerHeartbeat('worker-2', ['agent-a', 'agent-b'], fakeRedis(), registry as any, 5000, 30);

        await heartbeat.start();

        expect(registry.membershipCalls).toEqual([['worker-2', ['agent-a', 'agent-b']]]);
        expect(registry.heartbeatCalls).toEqual([['worker-2', 30]]);
    });

    test('a lifecycle message from the worker thread invokes the lifecycle callback', async () => {
        const registry = new FakeRegistry();
        const lifecycles: string[] = [];
        const heartbeat = new WorkerHeartbeat(
            'worker-3', ['agent-a'], fakeRedis(), registry as any, 5000, 30,
            (lifecycle) => lifecycles.push(lifecycle)
        );
        await heartbeat.start();
        const worker = FakeWorkerThread.instances[0];

        worker.emit('message', { type: 'lifecycle', lifecycle: 'suspended' });

        expect(lifecycles).toContain('suspended');
    });

    test('a denylist message from the worker thread invokes the denylist callback', async () => {
        const registry = new FakeRegistry();
        let lastDenied: Set<string> | null = null;
        const heartbeat = new WorkerHeartbeat(
            'worker-4', ['agent-a'], fakeRedis(), registry as any, 5000, 30,
            undefined, (denied) => { lastDenied = denied; }
        );
        await heartbeat.start();
        const worker = FakeWorkerThread.instances[0];

        worker.emit('message', { type: 'denylist', denied: ['agent-a'] });

        expect(lastDenied).toEqual(new Set(['agent-a']));
    });

    test('an unhealthy message stops the heartbeat and invokes onUnhealthy', async () => {
        const registry = new FakeRegistry();
        let unhealthyCalled = false;
        const heartbeat = new WorkerHeartbeat(
            'worker-5', ['agent-a'], fakeRedis(), registry as any, 5000, 30,
            undefined, undefined, () => true, () => { unhealthyCalled = true; }
        );
        await heartbeat.start();
        const worker = FakeWorkerThread.instances[0];

        worker.emit('message', { type: 'unhealthy' });
        await new Promise((resolve) => setImmediate(resolve));

        expect(unhealthyCalled).toBe(true);
        expect(worker.terminated).toBe(true);
    });

    test('when a healthCheck is provided, the main thread posts periodic health-tick messages', async () => {
        jest.useFakeTimers();
        try {
            const registry = new FakeRegistry();
            let healthy = true;
            const heartbeat = new WorkerHeartbeat(
                'worker-6', ['agent-a'], fakeRedis(), registry as any, 5000, 30,
                undefined, undefined, () => healthy
            );
            await heartbeat.start();
            const worker = FakeWorkerThread.instances[0];

            jest.advanceTimersByTime(2000);

            const ticks = worker.posted.filter((m) => m.type === 'health-tick');
            expect(ticks.length).toBeGreaterThan(0);
            expect(ticks[0].healthy).toBe(true);
        } finally {
            jest.useRealTimers();
        }
    });

    test('stop() terminates the worker thread', async () => {
        const registry = new FakeRegistry();
        const heartbeat = new WorkerHeartbeat('worker-7', ['agent-a'], fakeRedis(), registry as any, 5000, 30);
        await heartbeat.start();
        const worker = FakeWorkerThread.instances[0];

        await heartbeat.stop();

        expect(worker.terminated).toBe(true);
    });

    test('falls back to a main-thread interval when the Redis connection has no host/port (e.g. Cluster mode)', async () => {
        const registry = new FakeRegistry();
        const clusterLikeRedis = { options: {} }; // no host/port -> looks unextractable
        const heartbeat = new WorkerHeartbeat('worker-8', ['agent-a'], clusterLikeRedis as any, registry as any, 5000, 30);

        await heartbeat.start();

        expect(FakeWorkerThread.instances).toHaveLength(0);
        await heartbeat.stop();
    });
});

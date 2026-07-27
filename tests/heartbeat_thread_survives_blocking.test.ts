import Redis from 'ioredis';
import { WorkerHeartbeat } from '../src/heartbeat';
import { WorkerRegistry } from '../src/registry';
import { RegistryKeys } from '../src/constants';
import { FakeRedisTcpServer } from './helpers/fake_redis_tcp_server';

/**
 * Genuine end-to-end seam for issue #35: a real worker_threads.Worker, a real
 * ioredis connection inside it, and a real (fake-protocol) TCP server — no
 * mocking of WorkerHeartbeat's internals. Proves the lease key set by
 * WorkerRegistry.heartbeatWorker keeps getting renewed by a *separate OS
 * thread* while this test's own main thread is synchronously blocked, which
 * is exactly the failure mode issue #35 exists to fix.
 */

function busyWaitMs(ms: number): void {
    const end = Date.now() + ms;
    while (Date.now() < end) {
        // deliberately pegs the main thread's event loop
    }
}

describe('WorkerHeartbeat survives a long synchronous main-thread block (real worker_threads)', () => {
    let server: FakeRedisTcpServer;
    let redis: Redis;
    let heartbeat: WorkerHeartbeat;

    beforeEach(async () => {
        server = new FakeRedisTcpServer();
        const port = await server.listen();
        redis = new Redis({ host: '127.0.0.1', port, lazyConnect: false, maxRetriesPerRequest: 1 });
    });

    afterEach(async () => {
        await heartbeat?.stop();
        redis.disconnect();
        await server.close();
    });

    test('lease renewals keep happening on the worker thread during a 1.5s synchronous block', async () => {
        const workerId = 'worker-thread-survives';
        const registry = new WorkerRegistry(redis);
        heartbeat = new WorkerHeartbeat(workerId, ['agent-a'], redis, registry, 150, 30);

        await heartbeat.start();
        // Let a few renewals land normally before we block, to confirm the thread is alive.
        await new Promise((resolve) => setTimeout(resolve, 500));

        const leaseKey = RegistryKeys.worker_online_lease(workerId);
        const countBeforeBlock = server.getSetTimestamps(leaseKey).length;
        expect(countBeforeBlock).toBeGreaterThan(0);

        const blockStart = Date.now();
        busyWaitMs(1500); // synchronously freeze this thread's event loop
        const blockEnd = Date.now();

        // Give the worker thread's postMessage/redis round-trips a moment to be observed.
        await new Promise((resolve) => setTimeout(resolve, 200));

        const timestampsAfter = server.getSetTimestamps(leaseKey);
        const renewalsDuringBlock = timestampsAfter.filter((t) => t >= blockStart && t <= blockEnd + 50);

        expect(renewalsDuringBlock.length).toBeGreaterThanOrEqual(3); // ~1500ms / 150ms interval, generous margin
    }, 15000);
});

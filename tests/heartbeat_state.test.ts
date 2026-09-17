import { WorkerHeartbeat } from '../src/heartbeat';

/**
 * Registry stub that drives `heartbeatWorker` through a scripted sequence of
 * outcomes, so a test can say "renewal succeeds, then the lease is stolen".
 */
class ScriptedRegistry {
    public heartbeatCalls = 0;
    public membershipCalls = 0;

    constructor(private outcomes: Array<boolean | Error> = []) {}

    private next(): boolean | Error {
        const value = this.outcomes[Math.min(this.heartbeatCalls, this.outcomes.length - 1)];
        this.heartbeatCalls++;
        return value === undefined ? true : value;
    }

    async heartbeatWorker(): Promise<boolean> {
        const outcome = this.next();
        if (outcome instanceof Error) throw outcome;
        return outcome;
    }

    async getWorkerAdminState(): Promise<{ lifecycle: string }> {
        return { lifecycle: 'active' };
    }

    async registerWorkerMembership(): Promise<void> {
        this.membershipCalls++;
    }

    async isWorkerDeniedForType(): Promise<boolean> {
        return false;
    }
}

function makeHeartbeat(
    registry: ScriptedRegistry,
    observers: { onRenewOk?: (at: number) => void; onFenced?: () => void } = {},
    intervalMs = 10_000
): WorkerHeartbeat {
    return new WorkerHeartbeat(
        'worker-state',
        ['probe'],
        {} as any,
        registry as any,
        intervalMs,
        15,
        undefined,
        undefined,
        undefined,
        undefined,
        observers
    );
}

describe('WorkerHeartbeat renewal state machine', () => {
    let errorLog: jest.SpyInstance;

    beforeEach(() => {
        errorLog = jest.spyOn(console, 'error').mockImplementation(() => undefined);
        jest.spyOn(console, 'log').mockImplementation(() => undefined);
    });

    afterEach(() => {
        jest.restoreAllMocks();
    });

    test('a successful renewal records its timestamp and clears failures', async () => {
        const registry = new ScriptedRegistry([true]);
        const renewals: number[] = [];
        const heartbeat = makeHeartbeat(registry, { onRenewOk: at => renewals.push(at) });

        await heartbeat.start();

        expect(renewals.length).toBe(1);
        expect(heartbeat.lastSuccessfulRenewAt).toBe(renewals[0]);
        expect(heartbeat.failureCount).toBe(0);
        await heartbeat.stop();
    });

    test('a stolen lease fences the worker instead of retrying', async () => {
        // false = the lease carries another process's token.
        const registry = new ScriptedRegistry([false]);
        let fenced = 0;
        const heartbeat = makeHeartbeat(registry, { onFenced: () => fenced++ });

        await heartbeat.start();

        expect(fenced).toBe(1);
        expect(errorLog).toHaveBeenCalledWith(
            expect.stringContaining('worker id conflict')
        );
        // The interval must never start: a fenced worker has nothing to renew.
        expect(registry.heartbeatCalls).toBe(1);
        await heartbeat.stop();
    });

    test('a fence during a later cycle stops the running heartbeat', async () => {
        jest.useFakeTimers();
        try {
            const registry = new ScriptedRegistry([true, false]);
            let fenced = 0;
            const heartbeat = makeHeartbeat(registry, { onFenced: () => fenced++ }, 50);

            await heartbeat.start();
            expect(fenced).toBe(0);

            jest.advanceTimersByTime(50);
            await Promise.resolve();
            await Promise.resolve();
            await Promise.resolve();

            expect(fenced).toBe(1);
        } finally {
            jest.useRealTimers();
        }
    });

    test('a Redis error counts as a failure and keeps the lease timestamp stale', async () => {
        const registry = new ScriptedRegistry([new Error('ECONNRESET')]);
        const heartbeat = makeHeartbeat(registry);

        await heartbeat.start();

        expect(heartbeat.failureCount).toBe(1);
        expect(heartbeat.lastSuccessfulRenewAt).toBe(0);
        expect(errorLog).toHaveBeenCalledWith(
            expect.stringContaining('Heartbeat failed (attempt 1)'),
            expect.anything()
        );
        await heartbeat.stop();
    });

    test('the first failure inside a running loop is retried before the next interval', async () => {
        jest.useFakeTimers();
        try {
            // start() succeeds, the first interval tick fails, the retry succeeds.
            const registry = new ScriptedRegistry([true, new Error('ECONNRESET'), true]);
            const renewals: number[] = [];
            const heartbeat = makeHeartbeat(registry, { onRenewOk: at => renewals.push(at) }, 5_000);

            await heartbeat.start();
            expect(renewals.length).toBe(1);

            jest.advanceTimersByTime(5_000);
            await Promise.resolve();
            await Promise.resolve();
            await Promise.resolve();
            expect(heartbeat.failureCount).toBe(1);
            expect(renewals.length).toBe(1);

            // The compensating retry lands well before the next 5s interval.
            jest.advanceTimersByTime(500);
            await Promise.resolve();
            await Promise.resolve();
            await Promise.resolve();

            expect(renewals.length).toBe(2);
            expect(heartbeat.failureCount).toBe(0);
            await heartbeat.stop();
        } finally {
            jest.useRealTimers();
        }
    });

    test('a failed cycle skips the admin-state work that follows it', async () => {
        jest.useFakeTimers();
        try {
            const registry = new ScriptedRegistry([true, new Error('ECONNRESET')]);
            const heartbeat = makeHeartbeat(registry, {}, 5_000);

            await heartbeat.start();
            const membershipAfterStart = registry.membershipCalls;

            jest.advanceTimersByTime(5_000);
            await Promise.resolve();
            await Promise.resolve();
            await Promise.resolve();

            expect(registry.membershipCalls).toBe(membershipAfterStart);
            await heartbeat.stop();
        } finally {
            jest.useRealTimers();
        }
    });
});

import { WorkerRunner } from '../src/runner';
import { RegistryKeys } from '../src/constants';
import { HeartbeatConfigError } from '../src/exceptions';

class StubRedis {
    duplicate(): StubRedis {
        return new StubRedis();
    }
    async quit(): Promise<'OK'> { return 'OK'; }
    async xgroup(): Promise<'OK'> { return 'OK'; }
    async xreadgroup(): Promise<null> { return null; }
}

function makeRunner(maxConcurrency = 2): WorkerRunner {
    const worker = {
        workerId: 'worker-health',
        getAgentTypes: () => ['probe'],
        registry: {} as any,
        // Getters, matching GatewayWorker: the real ones resolve from env on
        // each access, and a snapshot here would hide that.
        get heartbeatInterval() {
            return RegistryKeys.WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS;
        },
        get heartbeatLeaseTtlSeconds() {
            return RegistryKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS;
        },
        startHeartbeat: async () => {},
        stopHeartbeat: async () => {},
        handleMessage: async () => { throw new Error('unused'); },
    } as any;
    return new WorkerRunner(worker, { redisClient: new StubRedis() as any, groupName: 'g', maxConcurrency });
}

/** Reach into the runner's private health inputs without exporting them. */
function setSignals(
    runner: WorkerRunner,
    signals: { tick?: number; renewOk?: number; lagMs?: number; inFlight?: number }
): void {
    const state = runner as any;
    if (signals.tick !== undefined) state.lastConsumerTick = signals.tick;
    if (signals.renewOk !== undefined) state.lastRenewOkAt = signals.renewOk;
    if (signals.lagMs !== undefined) state.peakLoopLagMs = signals.lagMs;
    if (signals.inFlight !== undefined) {
        state.inFlight.clear();
        for (let i = 0; i < signals.inFlight; i++) state.inFlight.add(Promise.resolve());
    }
}

describe('WorkerRunner.isHealthy', () => {
    const leaseTtlMs = RegistryKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS * 1000;

    test('a worker that has not started consuming is healthy', () => {
        const runner = makeRunner();
        expect(runner.isHealthy()).toBe(true);
    });

    test('a saturated worker with long tasks stays healthy', () => {
        const runner = makeRunner(2);
        // Every slot busy, no task finished for well past the stall timeout.
        setSignals(runner, { tick: Date.now() - 60_000, inFlight: 2, renewOk: Date.now() });
        expect(runner.isHealthy()).toBe(true);
    });

    test('an idle worker whose loop stopped ticking is unhealthy', () => {
        const runner = makeRunner(2);
        setSignals(runner, { tick: Date.now() - 60_000, inFlight: 0, renewOk: Date.now() });
        expect(runner.isHealthy()).toBe(false);
    });

    test('a stale lease renewal is unhealthy even while the loop ticks', () => {
        const runner = makeRunner();
        setSignals(runner, {
            tick: Date.now(),
            inFlight: 0,
            renewOk: Date.now() - leaseTtlMs, // well past the staleness ratio
        });
        // This is the case the consumer-tick-only check missed entirely: the
        // loop looks fine because polling uses its own connection, while the
        // lease quietly expires.
        expect(runner.isHealthy()).toBe(false);
    });

    test('event-loop lag beyond the threshold is unhealthy', () => {
        const runner = makeRunner();
        setSignals(runner, { tick: Date.now(), renewOk: Date.now(), lagMs: 6_000 });
        expect(runner.isHealthy()).toBe(false);
    });

    test('a renewal that has never succeeded does not by itself mean unhealthy', () => {
        const runner = makeRunner();
        setSignals(runner, { tick: Date.now(), renewOk: 0 });
        expect(runner.isHealthy()).toBe(true);
    });
});

describe('WorkerRunner health state machine', () => {
    function evaluate(runner: WorkerRunner): boolean {
        return (runner as any).evaluateHealth();
    }

    beforeEach(() => {
        jest.spyOn(console, 'warn').mockImplementation(() => undefined);
        jest.spyOn(console, 'error').mockImplementation(() => undefined);
        jest.spyOn(console, 'log').mockImplementation(() => undefined);
    });

    afterEach(() => jest.restoreAllMocks());

    test('first unhealthy check degrades but keeps the heartbeat renewing', () => {
        const runner = makeRunner();
        setSignals(runner, { tick: Date.now() - 60_000, inFlight: 0, renewOk: Date.now() });

        const keepRenewing = evaluate(runner);

        expect(runner.health).toBe('degraded');
        // Critical: in-flight tasks still need their replies routed home.
        expect(keepRenewing).toBe(true);
    });

    test('recovery needs a streak of healthy checks', () => {
        const runner = makeRunner();
        setSignals(runner, { tick: Date.now() - 60_000, inFlight: 0, renewOk: Date.now() });
        evaluate(runner);
        expect(runner.health).toBe('degraded');

        setSignals(runner, { tick: Date.now(), renewOk: Date.now() });
        evaluate(runner);
        expect(runner.health).toBe('degraded'); // one good check is not enough
        evaluate(runner);
        expect(runner.health).toBe('degraded');
        evaluate(runner);
        expect(runner.health).toBe('normal');
    });

    test('an interrupted streak restarts the count', () => {
        const runner = makeRunner();
        setSignals(runner, { tick: Date.now() - 60_000, inFlight: 0, renewOk: Date.now() });
        evaluate(runner);

        setSignals(runner, { tick: Date.now(), renewOk: Date.now() });
        evaluate(runner);
        evaluate(runner);
        setSignals(runner, { tick: Date.now() - 60_000, renewOk: Date.now() });
        evaluate(runner); // back to unhealthy
        setSignals(runner, { tick: Date.now(), renewOk: Date.now() });
        evaluate(runner);
        evaluate(runner);
        expect(runner.health).toBe('degraded');
        evaluate(runner);
        expect(runner.health).toBe('normal');
    });

    test('staying degraded past the limit escalates to fatal and stops the heartbeat', () => {
        const runner = makeRunner();
        setSignals(runner, { tick: Date.now() - 60_000, inFlight: 0, renewOk: Date.now() });
        evaluate(runner);
        expect(runner.health).toBe('degraded');

        // Pretend the degradation started long ago.
        (runner as any).degradedSince = Date.now() - 200_000;

        const keepRenewing = evaluate(runner);

        expect(runner.health).toBe('fatal');
        expect(keepRenewing).toBe(false);
    });

    test('fatal is terminal', () => {
        const runner = makeRunner();
        (runner as any).healthState = 'fatal';
        setSignals(runner, { tick: Date.now(), renewOk: Date.now() });

        expect(evaluate(runner)).toBe(false);
        expect(runner.health).toBe('fatal');
    });

    test('health state is independent of adminLifecycle', () => {
        const runner = makeRunner();
        setSignals(runner, { tick: Date.now() - 60_000, inFlight: 0, renewOk: Date.now() });
        evaluate(runner);
        expect(runner.health).toBe('degraded');

        // An admin resume must not launder away a real health problem.
        (runner as any).adminLifecycle = 'active';
        expect(runner.health).toBe('degraded');
    });
});

describe('event-loop delay probe', () => {
    test('records delay once the loop is free again', async () => {
        const runner = makeRunner();
        (runner as any).startEventLoopProbe();
        try {
            // Block the loop synchronously for longer than the probe interval.
            const until = Date.now() + 1200;
            while (Date.now() < until) { /* busy-wait */ }

            // Let the starved timer fire now that the loop is free, plus enough
            // time for a second, on-time tick — the peak must survive it.
            await new Promise(resolve => setTimeout(resolve, 2200));

            expect((runner as any).peakLoopLagMs).toBeGreaterThan(0);
        } finally {
            (runner as any).stopEventLoopProbe();
        }
    }, 15_000);

    test('a stall is not erased by the healthy samples that follow it', () => {
        const runner = makeRunner();
        setSignals(runner, { tick: Date.now(), renewOk: Date.now(), lagMs: 8_000 });

        // Simulate the probe ticking on time afterwards: it takes a max, so the
        // recorded peak survives. Health is evaluated far less often than the
        // probe samples, and an instantaneous reading would miss the stall
        // entirely — which is exactly how this test's first version flaked.
        const probeTick = (lag: number) => {
            const state = runner as any;
            state.peakLoopLagMs = Math.max(state.peakLoopLagMs, lag);
        };
        probeTick(0);
        probeTick(2);

        expect(runner.isHealthy()).toBe(false);
    });

    test('evaluating health consumes the peak so a past stall stops failing checks', () => {
        const runner = makeRunner();
        jest.spyOn(console, 'warn').mockImplementation(() => undefined);
        setSignals(runner, { tick: Date.now(), renewOk: Date.now(), lagMs: 8_000 });

        expect(runner.isHealthy()).toBe(false);
        (runner as any).evaluateHealth();

        expect((runner as any).peakLoopLagMs).toBe(0);
        expect(runner.isHealthy()).toBe(true);
        jest.restoreAllMocks();
    });

    test('stopping the probe clears the recorded lag and the timer', () => {
        const runner = makeRunner();
        (runner as any).startEventLoopProbe();
        expect((runner as any).lagTimer).not.toBeNull();

        (runner as any).stopEventLoopProbe();

        expect((runner as any).lagTimer).toBeNull();
        expect((runner as any).peakLoopLagMs).toBe(0);
    });

    test('starting twice does not create a second timer', () => {
        const runner = makeRunner();
        (runner as any).startEventLoopProbe();
        const first = (runner as any).lagTimer;
        (runner as any).startEventLoopProbe();
        expect((runner as any).lagTimer).toBe(first);
        (runner as any).stopEventLoopProbe();
    });
});

describe('heartbeat timing validation', () => {
    const savedEnv = { ...process.env };

    afterEach(() => {
        process.env = { ...savedEnv };
    });

    function assertTiming(runner: WorkerRunner): void {
        (runner as any).assertHeartbeatTiming();
    }

    test('the shipped defaults are valid', () => {
        expect(() => assertTiming(makeRunner())).not.toThrow();
    });

    test('an interval too long for the lease is rejected at startup', () => {
        process.env.BYAI_WORKER_HEARTBEAT_INTERVAL_SECONDS = '10';
        process.env.BYAI_WORKER_LEASE_TTL_SECONDS = '15';
        // 10s x 3 renewals > 15s lease: the first slow renewal would expire it.
        expect(() => assertTiming(makeRunner())).toThrow(HeartbeatConfigError);
    });

    test('an interval that leaves room for three renewals is accepted', () => {
        process.env.BYAI_WORKER_HEARTBEAT_INTERVAL_SECONDS = '10';
        process.env.BYAI_WORKER_LEASE_TTL_SECONDS = '30';
        expect(() => assertTiming(makeRunner())).not.toThrow();
    });

    test('env overrides are read per access, not at import time', () => {
        process.env.BYAI_WORKER_HEARTBEAT_INTERVAL_SECONDS = '7';
        expect(RegistryKeys.WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS).toBe(7);
        delete process.env.BYAI_WORKER_HEARTBEAT_INTERVAL_SECONDS;
        expect(RegistryKeys.WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS).toBe(5);
    });

    test('an unusable env value falls back to the default instead of producing NaN', () => {
        process.env.BYAI_WORKER_LEASE_TTL_SECONDS = 'soon';
        expect(RegistryKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS).toBe(15);
        process.env.BYAI_WORKER_LEASE_TTL_SECONDS = '-4';
        expect(RegistryKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS).toBe(15);
    });

    test('the inline wait bound tracks an overridden lease TTL', () => {
        process.env.BYAI_WORKER_LEASE_TTL_SECONDS = '30';
        expect(RegistryKeys.WORKER_MAX_INLINE_WAIT_MS).toBe(10_000);
    });
});

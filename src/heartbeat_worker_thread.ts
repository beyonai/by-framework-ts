import { parentPort, workerData } from 'worker_threads';
import { createRedis, RedisConnectionConfig } from './redis_client';
import { WorkerRegistry } from './registry';
import { runHeartbeatTick } from './heartbeat_tick';

/**
 * Entry point run inside a worker_threads.Worker by WorkerHeartbeat. Owns its
 * own Redis connection so lease renewal keeps running on a separate OS thread
 * and survives long synchronous work on the main thread's event loop.
 */
interface HeartbeatWorkerData {
    workerId: string;
    agentTypes: string[];
    intervalMs: number;
    leaseTtlSeconds: number;
    healthCheckEnabled: boolean;
    healthStaleThresholdMs: number;
    denylistEnabled: boolean;
    redisOptions: RedisConnectionConfig;
}

interface HealthTickMessage {
    type: 'health-tick';
    healthy: boolean;
}

type InboundMessage = HealthTickMessage;

if (!parentPort) {
    throw new Error('heartbeat_worker_thread must be run inside a worker_threads.Worker');
}

const port = parentPort;
const data = workerData as HeartbeatWorkerData;
const redis = createRedis(data.redisOptions);
const registry = new WorkerRegistry(redis);

let lastHealthPingAt = Date.now();
let lastHealthPingHealthy = true;
let stopped = false;

port.on('message', (msg: InboundMessage) => {
    if (msg?.type === 'health-tick') {
        lastHealthPingAt = Date.now();
        lastHealthPingHealthy = msg.healthy !== false;
    }
});

async function tick(): Promise<void> {
    if (stopped) return;

    if (data.healthCheckEnabled) {
        const stale = Date.now() - lastHealthPingAt > data.healthStaleThresholdMs;
        if (!lastHealthPingHealthy || stale) {
            stopped = true;
            clearInterval(intervalHandle);
            port.postMessage({ type: 'unhealthy' });
            return;
        }
    }

    try {
        const result = await runHeartbeatTick({
            registry,
            workerId: data.workerId,
            agentTypes: data.agentTypes,
            leaseTtlSeconds: data.leaseTtlSeconds,
            denylistEnabled: data.denylistEnabled,
        });
        port.postMessage({ type: 'lifecycle', lifecycle: result.lifecycle });
        if (result.denied !== undefined) {
            port.postMessage({ type: 'denylist', denied: result.denied });
        }
    } catch (error) {
        port.postMessage({ type: 'error', message: String((error as Error)?.message || error) });
    }
}

const intervalHandle = setInterval(() => {
    void tick();
}, data.intervalMs);

void tick();
port.postMessage({ type: 'ready' });

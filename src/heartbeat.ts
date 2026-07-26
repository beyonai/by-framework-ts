import * as path from 'path';
import { Worker } from 'worker_threads';
import { Redis, Cluster } from 'ioredis';
import { WorkerRegistry } from './registry';
import { getRedis, RedisConnectionConfig } from './redis_client';
import { RegistryKeys } from './constants';
import { runHeartbeatTick } from './heartbeat_tick';

/** How stale a health-tick from the main thread can get before the worker thread treats it as unhealthy. */
const HEALTH_STALE_THRESHOLD_MS = 30_000;
/** How often the main thread pings the worker thread with the current healthCheck() result. */
const HEALTH_PING_INTERVAL_MS = 2_000;

function resolveHeartbeatWorkerPath(): string {
    // Production runs compiled dist/*.js: the sibling compiled worker script
    // sits right next to this file. Under ts-node/ts-jest, __filename ends in
    // .ts, so the spawned thread needs ts-node registered to load TS source.
    const ext = path.extname(__filename);
    return path.join(__dirname, `heartbeat_worker_thread${ext}`);
}

export class WorkerHeartbeat {
    private registry: WorkerRegistry;
    private leaseTtlSeconds: number;
    private lifecycleCallback?: (lifecycle: string) => void;
    private denylistRefresh?: (denied: Set<string>) => void;
    private healthCheck?: () => boolean;
    private onUnhealthy?: () => void;

    private workerThread: Worker | null = null;
    private healthPingIntervalId: NodeJS.Timeout | null = null;
    /** Only used as a fallback when the Redis connection can't be reconstructed in a worker thread (e.g. Cluster mode). */
    private fallbackIntervalId: NodeJS.Timeout | null = null;

    constructor(
        private workerId: string,
        private agentTypes: string[],
        private redis: Redis = getRedis(),
        registry?: WorkerRegistry,
        private intervalMs: number = RegistryKeys.WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS * 1000,
        leaseTtlSeconds?: number,
        lifecycleCallback?: (lifecycle: string) => void,
        denylistRefresh?: (denied: Set<string>) => void,
        healthCheck?: () => boolean,
        onUnhealthy?: () => void
    ) {
        this.registry = registry || new WorkerRegistry(this.redis);
        this.leaseTtlSeconds = leaseTtlSeconds ?? RegistryKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS;
        this.lifecycleCallback = lifecycleCallback;
        this.denylistRefresh = denylistRefresh;
        this.healthCheck = healthCheck;
        this.onUnhealthy = onUnhealthy;
    }

    private extractRedisOptions(): RedisConnectionConfig | undefined {
        if (this.redis instanceof Cluster) {
            return undefined;
        }
        const options: any = (this.redis as any).options || {};
        if (!options.host && !options.port) {
            return undefined;
        }
        return {
            mode: 'standalone',
            host: options.host,
            port: options.port,
            db: options.db,
            username: options.username,
            password: options.password,
        };
    }

    async start(): Promise<void> {
        if (this.workerThread || this.fallbackIntervalId) return;

        // Read admin lifecycle BEFORE registering membership.
        // A worker that restarts while suspended must not re-join the
        // agent_type:members sets until explicitly resumed.
        const adminState = await this.registry.getWorkerAdminState(this.workerId);
        const startupLifecycle = adminState.lifecycle || 'active';

        if (startupLifecycle === 'active') {
            await this.registry.registerWorkerMembership(this.workerId, this.agentTypes);
        } else {
            console.warn(
                `[${this.workerId}] Startup admin lifecycle is '${startupLifecycle}'; skipping member registration — worker will not consume until resumed`
            );
        }

        // Initial heartbeat
        await this.registry.heartbeatWorker(this.workerId, this.leaseTtlSeconds);

        // Propagate startup lifecycle to runner immediately (before interval fires)
        if (this.lifecycleCallback && startupLifecycle !== 'active') {
            this.lifecycleCallback(startupLifecycle);
        }

        const redisOptions = this.extractRedisOptions();
        if (!redisOptions) {
            console.warn(
                `[${this.workerId}] Heartbeat cannot run on a worker thread for this Redis connection (e.g. Cluster mode); falling back to a main-thread interval. Lease renewal may stall under long synchronous command handlers.`
            );
            this.startFallbackInterval();
            return;
        }

        this.spawnWorkerThread(redisOptions);
        console.log(`[${this.workerId}] Threaded heartbeat started`);
    }

    private spawnWorkerThread(redisOptions: RedisConnectionConfig): void {
        const workerPath = resolveHeartbeatWorkerPath();
        const execArgv = workerPath.endsWith('.ts') ? ['-r', 'ts-node/register'] : [];

        this.workerThread = new Worker(workerPath, {
            execArgv,
            workerData: {
                workerId: this.workerId,
                agentTypes: this.agentTypes,
                intervalMs: this.intervalMs,
                leaseTtlSeconds: this.leaseTtlSeconds,
                healthCheckEnabled: Boolean(this.healthCheck),
                healthStaleThresholdMs: HEALTH_STALE_THRESHOLD_MS,
                denylistEnabled: Boolean(this.denylistRefresh),
                redisOptions,
            },
        });

        this.workerThread.on('message', (msg: any) => {
            if (msg?.type === 'lifecycle') {
                this.lifecycleCallback?.(msg.lifecycle);
            } else if (msg?.type === 'denylist') {
                this.denylistRefresh?.(new Set<string>(msg.denied || []));
            } else if (msg?.type === 'unhealthy') {
                console.error(`[${this.workerId}] Heartbeat stopping: consumer loop is unhealthy`);
                void this.stop();
                this.onUnhealthy?.();
            } else if (msg?.type === 'error') {
                console.error(`[${this.workerId}] Heartbeat failed:`, msg.message);
            }
        });
        this.workerThread.on('error', (err) => {
            console.error(`[${this.workerId}] Heartbeat worker thread error:`, err);
        });

        if (this.healthCheck) {
            this.healthPingIntervalId = setInterval(() => {
                this.workerThread?.postMessage({ type: 'health-tick', healthy: this.healthCheck!() });
            }, HEALTH_PING_INTERVAL_MS);
        }
    }

    /** Pre-worker_threads behavior, kept as a fallback for connection modes (e.g. Cluster) not yet supported off-thread. */
    private startFallbackInterval(): void {
        this.fallbackIntervalId = setInterval(async () => {
            try {
                if (this.healthCheck && !this.healthCheck()) {
                    console.error(`[${this.workerId}] Heartbeat stopping: consumer loop is unhealthy`);
                    await this.stop();
                    this.onUnhealthy?.();
                    return;
                }

                const result = await runHeartbeatTick({
                    registry: this.registry,
                    workerId: this.workerId,
                    agentTypes: this.agentTypes,
                    leaseTtlSeconds: this.leaseTtlSeconds,
                    denylistEnabled: Boolean(this.denylistRefresh),
                });
                this.lifecycleCallback?.(result.lifecycle);
                if (result.denied !== undefined) {
                    this.denylistRefresh?.(new Set(result.denied));
                }
            } catch (error) {
                console.error(`[${this.workerId}] Heartbeat failed:`, error);
            }
        }, this.intervalMs);

        console.log(`[${this.workerId}] Standalone heartbeat started`);
    }

    async stop(): Promise<void> {
        if (this.healthPingIntervalId) {
            clearInterval(this.healthPingIntervalId);
            this.healthPingIntervalId = null;
        }
        if (this.workerThread) {
            const worker = this.workerThread;
            this.workerThread = null;
            await worker.terminate();
            console.log(`[${this.workerId}] Threaded heartbeat stopped`);
        }
        if (this.fallbackIntervalId) {
            clearInterval(this.fallbackIntervalId);
            this.fallbackIntervalId = null;
            console.log(`[${this.workerId}] Standalone heartbeat stopped`);
        }
    }
}

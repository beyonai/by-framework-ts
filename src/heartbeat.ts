import { Redis } from 'ioredis';
import { WorkerRegistry } from './registry';
import { getRedis } from './redis_client';
import { RegistryKeys } from './constants';

export class WorkerHeartbeat {
    private registry: WorkerRegistry;
    private intervalId: NodeJS.Timeout | null = null;
    private leaseTtlSeconds: number;
    private lifecycleCallback?: (lifecycle: string) => void;
    private denylistRefresh?: (denied: Set<string>) => void;
    private healthCheck?: () => boolean;
    private onUnhealthy?: () => void;

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

    async start(): Promise<void> {
        if (this.intervalId) return;

        // Read admin lifecycle BEFORE registering membership.
        // A worker that restarts while suspended must not re-join the
        // agent_type:members sets until explicitly resumed.
        let startupLifecycle = 'active';
        const adminState = await this.registry.getWorkerAdminState(this.workerId);
        startupLifecycle = adminState.lifecycle || 'active';

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

        this.intervalId = setInterval(async () => {
            try {
                // Health check: if the consumer loop has stalled, stop the heartbeat
                // so the lease expires and the worker is evicted from routing.
                if (this.healthCheck && !this.healthCheck()) {
                    console.error(`[${this.workerId}] Heartbeat stopping: consumer loop is unhealthy`);
                    await this.stop();
                    if (this.onUnhealthy) this.onUnhealthy();
                    return;
                }

                await this.registry.heartbeatWorker(this.workerId, this.leaseTtlSeconds);

                // Read admin state and notify runner
                const state = await this.registry.getWorkerAdminState(this.workerId);
                const currentLifecycle = state.lifecycle || 'active';
                if (this.lifecycleCallback) {
                    this.lifecycleCallback(currentLifecycle);
                }

                // Re-register membership only when active (for self-healing)
                if (currentLifecycle === 'active') {
                    await this.registry.registerWorkerMembership(this.workerId, this.agentTypes);
                }

                // Refresh denylist cache
                if (this.denylistRefresh) {
                    const denied = new Set<string>();
                    for (const agentType of this.agentTypes) {
                        if (await this.registry.isWorkerDeniedForType(agentType, this.workerId)) {
                            denied.add(agentType);
                        }
                    }
                    this.denylistRefresh(denied);
                }
            } catch (error) {
                console.error(`[${this.workerId}] Heartbeat failed:`, error);
            }
        }, this.intervalMs);

        console.log(`[${this.workerId}] Standalone heartbeat started`);
    }

    async stop(): Promise<void> {
        if (this.intervalId) {
            clearInterval(this.intervalId);
            this.intervalId = null;
            console.log(`[${this.workerId}] Standalone heartbeat stopped`);
        }
    }
}

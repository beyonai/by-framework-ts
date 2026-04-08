import { Redis } from 'ioredis';
import { WorkerRegistry } from './registry';
import { getRedis } from './redis_client';
import { RegistryKeys } from './constants';

export class WorkerHeartbeat {
    private registry: WorkerRegistry;
    private intervalId: NodeJS.Timeout | null = null;
    private leaseTtlSeconds: number;

    constructor(
        private workerId: string,
        private agentTypes: string[],
        private redis: Redis = getRedis(),
        registry?: WorkerRegistry,
        private intervalMs: number = RegistryKeys.WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS * 1000,
        leaseTtlSeconds?: number
    ) {
        this.registry = registry || new WorkerRegistry(this.redis);
        this.leaseTtlSeconds = leaseTtlSeconds ?? RegistryKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS;
    }

    async start(): Promise<void> {
        if (this.intervalId) return;

        // Register membership once
        await this.registry.registerWorkerMembership(this.workerId, this.agentTypes);

        // Initial heartbeat
        await this.registry.heartbeatWorker(this.workerId, this.leaseTtlSeconds);

        this.intervalId = setInterval(async () => {
            try {
                await this.registry.heartbeatWorker(this.workerId, this.leaseTtlSeconds);
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

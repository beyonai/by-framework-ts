import { Redis } from 'ioredis';
import { WorkerRegistry } from './registry';
import { getRedis } from './redis_client';

export class WorkerHeartbeat {
    private registry: WorkerRegistry;
    private intervalId: NodeJS.Timeout | null = null;

    constructor(
        private workerId: string,
        private agentTypes: string[],
        private redis: Redis = getRedis(),
        registry?: WorkerRegistry,
        private intervalMs: number = 10000
    ) {
        this.registry = registry || new WorkerRegistry(this.redis);
    }

    async start(): Promise<void> {
        if (this.intervalId) return;

        // Initial registration
        await this.registry.registerWorker(this.workerId, this.agentTypes);

        this.intervalId = setInterval(async () => {
            try {
                await this.registry.registerWorker(this.workerId, this.agentTypes);
            } catch (error) {
                console.error(`[${this.workerId}] Heartbeat failed:`, error);
            }
        }, this.intervalMs);

        console.log(`[${this.workerId}] Standalone heartbeat started`);
    }

    stop(): void {
        if (this.intervalId) {
            clearInterval(this.intervalId);
            this.intervalId = null;
            console.log(`[${this.workerId}] Standalone heartbeat stopped`);
        }
    }
}

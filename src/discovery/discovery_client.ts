import { Redis } from 'ioredis';
import { getRedis } from '../redis_client';
import { RegistryKeys } from '../constants';
import { ServiceInstance } from './service_instance';

/**
 * Load balancing strategies for service discovery.
 */
export type DiscoveryStrategy = 'random' | 'round-robin';

/**
 * High-efficiency service discovery client with local cache.
 *
 * Used by consumers. Reduces Redis access frequency through in-memory cache
 * and background refresh mechanism.
 */
export class DiscoveryClient {
    private redis: Redis;
    private cacheInterval: number;
    private cache: Map<string, ServiceInstance[]> = new Map();
    private lastRefresh: Map<string, number> = new Map();
    private watchedServices: Set<string> = new Set();
    private refreshTimer: ReturnType<typeof setInterval> | null = null;
    private rrCounters: Map<string, number> = new Map();

    constructor(redisClient?: Redis, cacheIntervalSeconds: number = 5) {
        this.redis = redisClient ?? getRedis();
        this.cacheInterval = cacheIntervalSeconds;
    }

    /**
     * Get service instances. Uses cache preferentially.
     *
     * @param serviceName - Service name to discover
     * @param forceRefresh - Force refresh from Redis
     * @param healthThresholdMs - Health check threshold in milliseconds
     */
    async getInstances(
        serviceName: string,
        forceRefresh: boolean = false,
        healthThresholdMs: number = RegistryKeys.SD_DEFAULT_HEALTH_THRESHOLD_MS
    ): Promise<ServiceInstance[]> {
        const now = Date.now() / 1000;
        const last = this.lastRefresh.get(serviceName) ?? 0;

        const isStale = now - last > this.cacheInterval;
        const needsRefresh = forceRefresh || isStale || !this.cache.has(serviceName);

        if (needsRefresh) {
            await this.refreshService(serviceName, healthThresholdMs);
        }

        return this.cache.get(serviceName) ?? [];
    }

    /**
     * Refresh service instances from Redis and update cache.
     */
    private async refreshService(serviceName: string, healthThresholdMs: number): Promise<void> {
        const nowMs = Date.now();
        const minScore = nowMs - healthThresholdMs;

        // 1. Get active instance IDs
        const instanceIds = await this.redis.zrangebyscore(
            RegistryKeys.sd_active_instances(serviceName),
            minScore.toString(),
            '+inf'
        );

        if (!instanceIds || instanceIds.length === 0) {
            this.cache.set(serviceName, []);
            this.lastRefresh.set(serviceName, Date.now() / 1000);
            return;
        }

        // 2. Get instance details
        const detailsRaw = await this.redis.hmget(
            RegistryKeys.sd_instance_details(serviceName),
            ...instanceIds
        );

        const instances: ServiceInstance[] = [];
        for (const raw of detailsRaw) {
            if (raw !== null && raw !== undefined) {
                const data = String(raw);
                try {
                    instances.push(ServiceInstance.fromJSON(data));
                } catch {
                    // Skip invalid JSON
                }
            }
        }

        this.cache.set(serviceName, instances);
        this.lastRefresh.set(serviceName, Date.now() / 1000);
    }

    /**
     * Add a service to the background auto-refresh list.
     */
    watch(serviceName: string): void {
        this.watchedServices.add(serviceName);
        if (!this.refreshTimer) {
            this.startRefreshLoop();
        }
    }

    /**
     * Stop watching a service.
     */
    unwatch(serviceName: string): void {
        this.watchedServices.delete(serviceName);
        if (this.watchedServices.size === 0) {
            this.stopRefreshLoop();
        }
    }

    /**
     * Start the background refresh loop.
     */
    private startRefreshLoop(): void {
        this.refreshTimer = setInterval(async () => {
            for (const serviceName of this.watchedServices) {
                try {
                    await this.refreshService(serviceName, RegistryKeys.SD_DEFAULT_HEALTH_THRESHOLD_MS);
                } catch {
                    // Retry after 1 second on error
                    setTimeout(() => this.refreshService(serviceName, RegistryKeys.SD_DEFAULT_HEALTH_THRESHOLD_MS), 1000);
                }
            }
        }, this.cacheInterval * 1000);
    }

    /**
     * Stop the background refresh loop.
     */
    private stopRefreshLoop(): void {
        if (this.refreshTimer) {
            clearInterval(this.refreshTimer);
            this.refreshTimer = null;
        }
    }

    /**
     * Perform load-balanced service discovery.
     *
     * @param serviceName - Service name to discover
     * @param strategy - Load balancing strategy ('random' or 'round-robin')
     * @returns Service instance or null if none available
     */
    async discover(
        serviceName: string,
        strategy: DiscoveryStrategy = 'random'
    ): Promise<ServiceInstance | null> {
        const instances = await this.getInstances(serviceName);
        if (!instances || instances.length === 0) {
            return null;
        }

        if (strategy === 'random') {
            const index = Math.floor(Math.random() * instances.length);
            return instances[index];
        }

        if (strategy === 'round-robin') {
            const counter = this.rrCounters.get(serviceName) ?? 0;
            const instance = instances[counter % instances.length];
            this.rrCounters.set(serviceName, counter + 1);
            return instance;
        }

        // Default to random
        const index = Math.floor(Math.random() * instances.length);
        return instances[index];
    }

    /**
     * Close the client and stop background tasks.
     */
    close(): void {
        this.stopRefreshLoop();
        this.cache.clear();
        this.lastRefresh.clear();
        this.watchedServices.clear();
        this.rrCounters.clear();
    }

    /**
     * Get cache statistics for monitoring.
     */
    getCacheStats(): {
        cachedServices: number;
        watchedServices: number;
        cacheEntries: Map<string, number>;
    } {
        return {
            cachedServices: this.cache.size,
            watchedServices: this.watchedServices.size,
            cacheEntries: new Map(this.lastRefresh),
        };
    }
}
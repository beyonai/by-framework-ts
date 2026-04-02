import { v4 as uuidv4 } from 'uuid';
import { Redis } from 'ioredis';
import { getRedis } from '../redis_client';
import { RegistryKeys } from '../constants';
import { ServiceInstance } from './service_instance';
import { getLocalIp } from './utils';

/**
 * Service Registry SDK for server-side use.
 *
 * Handles service registration, automatic heartbeat maintenance, and unregistration.
 */
export class ServiceRegistry {
    private redis: Redis;
    private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
    private currentInstance: ServiceInstance | null = null;
    private currentServiceName: string | null = null;

    constructor(redisClient?: Redis) {
        this.redis = redisClient ?? getRedis();
    }

    /**
     * Register current service instance and start background heartbeat.
     */
    async register(params: {
        serviceName: string;
        host?: string;
        port?: number;
        weight?: number;
        metadata?: Record<string, unknown>;
        heartbeatInterval?: number;
    }): Promise<void> {
        const {
            serviceName,
            host,
            port = 0,
            weight = 1,
            metadata = {},
            heartbeatInterval = RegistryKeys.SD_DEFAULT_HEARTBEAT_INTERVAL_SECONDS,
        } = params;

        const resolvedHost = host ?? this.detectHost();

        const instanceId = `${serviceName}:${uuidv4().slice(0, 8)}`;
        this.currentInstance = new ServiceInstance(
            instanceId,
            resolvedHost,
            port,
            weight,
            metadata
        );
        this.currentServiceName = serviceName;

        // 1. Write instance details
        await this.redis.hset(
            RegistryKeys.sd_instance_details(serviceName),
            instanceId,
            this.currentInstance.toJSON()
        );

        // 2. Add service name to global index
        await this.redis.sadd(RegistryKeys.SD_SERVICES, serviceName);

        // 3. Send initial heartbeat and start heartbeat loop
        await this.sendHeartbeat();
        this.startHeartbeatLoop(serviceName, instanceId, heartbeatInterval);
    }

    /**
     * Unregister current service instance and stop heartbeat.
     */
    async unregister(): Promise<void> {
        this.stopHeartbeatLoop();

        if (this.currentInstance && this.currentServiceName) {
            const pipe = this.redis.pipeline();
            pipe.hdel(
                RegistryKeys.sd_instance_details(this.currentServiceName),
                this.currentInstance.id
            );
            pipe.zrem(
                RegistryKeys.sd_active_instances(this.currentServiceName),
                this.currentInstance.id
            );
            await pipe.exec();
        }

        this.currentInstance = null;
        this.currentServiceName = null;
    }

    /**
     * Detect host from Redis connection configuration.
     */
    private detectHost(): string {
        try {
            const connectionPool = (this.redis as any).connection_pool;
            if (connectionPool) {
                const connection_kwargs = connectionPool.connection_kwargs || {};
                const redisHost = connection_kwargs.host || '8.8.8.8';
                const redisPort = connection_kwargs.port || 6379;
                return getLocalIp(redisHost, redisPort);
            }
        } catch {
            // Ignore errors and use default
        }
        return getLocalIp('8.8.8.8', 80);
    }

    /**
     * Send a single heartbeat for the current instance.
     */
    private async sendHeartbeat(): Promise<void> {
        if (this.currentInstance && this.currentServiceName) {
            const now = Date.now();
            await this.redis.zadd(
                RegistryKeys.sd_active_instances(this.currentServiceName),
                now.toString(),
                this.currentInstance.id
            );
        }
    }

    /**
     * Start the heartbeat loop.
     */
    private startHeartbeatLoop(serviceName: string, instanceId: string, intervalSeconds: number): void {
        this.heartbeatTimer = setInterval(async () => {
            try {
                await this.sendHeartbeat();
            } catch {
                // Retry after 1 second on error
                setTimeout(() => this.sendHeartbeat(), 1000);
            }
        }, intervalSeconds * 1000);
    }

    /**
     * Stop the heartbeat loop.
     */
    private stopHeartbeatLoop(): void {
        if (this.heartbeatTimer) {
            clearInterval(this.heartbeatTimer);
            this.heartbeatTimer = null;
        }
    }

    /**
     * Check if currently registered.
     */
    isRegistered(): boolean {
        return this.currentInstance !== null && this.currentServiceName !== null;
    }

    /**
     * Get current service name.
     */
    getServiceName(): string | null {
        return this.currentServiceName;
    }

    /**
     * Get current instance ID.
     */
    getInstanceId(): string | null {
        return this.currentInstance?.id ?? null;
    }
}
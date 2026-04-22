import { v4 as uuidv4 } from 'uuid';
import { Redis } from 'ioredis';
import { getRedis } from './redis_client';
import { RegistryKeys } from './constants';

interface WorkerPresence {
    readonly version: number;
    readonly token: string | null;
    readonly last_seen: number;
}

interface DecodedPresence {
    readonly token: string | null;
    readonly lastSeen: number;
    readonly isLegacy: boolean;
}

function encodeWorkerPresence(token: string | null, lastSeen: number): string {
    return JSON.stringify({
        version: 1,
        token,
        last_seen: lastSeen,
    } satisfies WorkerPresence);
}

function decodeWorkerPresence(raw: string | null): DecodedPresence {
    if (raw === null) {
        return { token: null, lastSeen: 0, isLegacy: false };
    }

    try {
        const payload = JSON.parse(raw) as unknown;
        if (payload && typeof payload === 'object' && !Array.isArray(payload)) {
            const record = payload as Record<string, unknown>;
            const token = record.token === null || record.token === undefined
                ? null
                : String(record.token);
            const lastSeen = typeof record.last_seen === 'number'
                ? record.last_seen
                : Number(record.last_seen || 0);
            return { token, lastSeen, isLegacy: false };
        }
        if (payload === 1) {
            return { token: null, lastSeen: 0, isLegacy: true };
        }
        return { token: String(payload), lastSeen: 0, isLegacy: true };
    } catch {
        return { token: raw, lastSeen: 0, isLegacy: true };
    }
}

export class WorkerRegistry {
    private redis: Redis;
    private readonly REGISTRY_KEY = 'gateway:worker_registry';
    private lockTokens: Map<string, string> = new Map();

    constructor(redisClient?: Redis) {
        this.redis = redisClient || getRedis();
    }

    async registerWorker(workerId: string, agentTypes: string[]): Promise<void> {
        await this.registerWorkerMembership(workerId, agentTypes);
        await this.heartbeatWorker(workerId);
    }

    /**
     * Register worker membership (agent types) without sending heartbeat.
     * Use this when you want to register separately from heartbeat.
     */
    async registerWorkerMembership(workerId: string, agentTypes: string[]): Promise<void> {
        await this.redis.sadd(RegistryKeys.KNOWN_WORKERS, workerId);
        for (const agentType of agentTypes) {
            await this.redis.sadd(RegistryKeys.workerDeclaredAgentTypes(workerId), agentType);
            await this.redis.sadd(RegistryKeys.agentTypeMembers(agentType), workerId);
        }
    }

    /**
     * Send heartbeat for a worker to maintain online status.
     * Updates the token-owned presence lease.
     */
    async heartbeatWorker(workerId: string, leaseTtlSeconds: number = RegistryKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS): Promise<boolean> {
        const now = Date.now();
        const key = RegistryKeys.worker_online_lease(workerId);
        const token = this.lockTokens.get(workerId) || null;
        const current = await this.redis.get(key);
        const currentPresence = decodeWorkerPresence(current);

        if (token) {
            if (current === null) {
                const ok = await this.redis.set(
                    key,
                    encodeWorkerPresence(token, now),
                    'EX',
                    leaseTtlSeconds,
                    'NX'
                );
                if (!ok) return false;
            } else if (currentPresence.token !== token) {
                return false;
            } else {
                await this.redis.set(key, encodeWorkerPresence(token, now), 'EX', leaseTtlSeconds);
            }
        } else {
            if (currentPresence.token !== null) return false;
            await this.redis.set(key, encodeWorkerPresence(null, now), 'EX', leaseTtlSeconds);
        }

        await this.redis.sadd(RegistryKeys.KNOWN_WORKERS, workerId);
        return true;
    }

    async unregisterWorker(workerId: string): Promise<void> {
        await this.markWorkerInactive(workerId);
        await this.unregisterWorkerMembership(workerId);
    }

    /**
     * Mark worker as inactive (remove lease) without removing membership.
     */
    async markWorkerInactive(workerId: string, token?: string): Promise<boolean> {
        const expected = token || this.lockTokens.get(workerId);
        const key = RegistryKeys.worker_online_lease(workerId);
        const current = await this.redis.get(key);
        const currentPresence = decodeWorkerPresence(current);
        if (expected && currentPresence.token !== expected) return false;

        await this.redis.del(key);
        return true;
    }

    /**
     * Remove worker membership (agent types) without mutating liveness.
     */
    async unregisterWorkerMembership(workerId: string): Promise<void> {
        const agentTypes = await this.redis.smembers(RegistryKeys.workerDeclaredAgentTypes(workerId));
        await this.redis.del(RegistryKeys.workerDeclaredAgentTypes(workerId));
        await this.redis.srem(RegistryKeys.KNOWN_WORKERS, workerId);
        for (const agentType of agentTypes) {
            await this.redis.srem(RegistryKeys.agentTypeMembers(agentType), workerId);
        }
    }

    /**
     * Get all online workers for a given agent type.
     */
    async getOnlineWorkers(agentType: string): Promise<string[]> {
        const [, workerIds] = await this.hasAgentType(agentType, true);
        return workerIds;
    }

    /**
     * Get a random online worker for a given agent type.
     */
    async getRandomOnlineWorker(agentType: string): Promise<string | null> {
        const workers = await this.getOnlineWorkers(agentType);
        if (workers.length === 0) {
            return null;
        }
        const randomIndex = Math.floor(Math.random() * workers.length);
        return workers[randomIndex];
    }

    /**
     * Check if a worker is online using lease key.
     */
    async isWorkerOnline(workerId: string): Promise<boolean> {
        const leaseValue = await this.redis.get(RegistryKeys.worker_online_lease(workerId));
        if (leaseValue === null) return false;
        const presence = decodeWorkerPresence(leaseValue);
        return presence.isLegacy || presence.lastSeen > 0;
    }

    /**
     * Check if an agent type has any registered and online workers.
     * Uses lease-based online check.
     */
    async hasOnlineAgentType(agentType: string): Promise<[boolean, string[]]> {
        const workers = await this.redis.smembers(RegistryKeys.agentTypeMembers(agentType));
        if (!workers || workers.length === 0) {
            return [false, []];
        }

        const onlineWorkerIds: string[] = [];
        for (const workerId of workers) {
            if (await this.isWorkerOnline(workerId)) {
                onlineWorkerIds.push(workerId);
            }
        }

        return [onlineWorkerIds.length > 0, onlineWorkerIds];
    }

    async getTargetWorker(agentType: string): Promise<string | null> {
        return this.getRandomOnlineWorker(agentType);
    }

    /**
     * Check if an agent type has any registered and online workers.
     * Uses lease-based online check (aligned with Python SDK).
     *
     * @param agentType - Agent type identifier to check
     * @param checkActive - Whether to check worker online status (default true)
     * @returns Tuple of [hasAgentType, workerIds[]]
     */
    async hasAgentType(
        agentType: string,
        checkActive: boolean = true
    ): Promise<[boolean, string[]]> {
        const workers = await this.redis.smembers(RegistryKeys.agentTypeMembers(agentType));
        if (!workers || workers.length === 0) {
            return [false, []];
        }

        let workerIds = [...workers];

        if (checkActive) {
            const onlineWorkerIds: string[] = [];
            for (const workerId of workerIds) {
                if (await this.isWorkerOnline(workerId)) {
                    onlineWorkerIds.push(workerId);
                }
            }
            workerIds = onlineWorkerIds;
        }

        return [workerIds.length > 0, workerIds];
    }

    // Maintaining these for compatibility with current TS tests, but they now use the new keys
    async getWorker(workerId: string): Promise<any | null> {
        const rawPresence = await this.redis.get(RegistryKeys.worker_online_lease(workerId));
        if (rawPresence === null) return null;
        const presence = decodeWorkerPresence(rawPresence);
        if (!presence.isLegacy && presence.lastSeen <= 0) return null;

        const agentTypes = await this.redis.smembers(RegistryKeys.workerDeclaredAgentTypes(workerId));
        return {
            agentTypes,
            last_seen: presence.isLegacy ? Date.now() : presence.lastSeen,
        };
    }

    async getAllWorkers(): Promise<Record<string, any>> {
        const workerIds = await this.redis.smembers(RegistryKeys.KNOWN_WORKERS);
        const result: Record<string, any> = {};
        for (const id of [...workerIds].sort()) {
            const data = await this.getWorker(id);
            if (data) result[id] = data;
        }
        return result;
    }

    async claimWorkerId(workerId: string, ttlSeconds: number = 60): Promise<string> {
        const token = uuidv4().replace(/-/g, '');
        const key = RegistryKeys.worker_online_lease(workerId);

        const ok = await this.redis.set(
            key,
            encodeWorkerPresence(token, 0),
            'EX',
            ttlSeconds,
            'NX'
        );
        if (!ok) {
            throw new Error(`worker_id already in use: ${workerId}`);
        }
        this.lockTokens.set(workerId, token);
        await this.redis.sadd(RegistryKeys.KNOWN_WORKERS, workerId);
        return token;
    }

    async refreshWorkerIdLock(workerId: string, ttlSeconds: number = 60): Promise<boolean> {
        const token = this.lockTokens.get(workerId);
        if (!token) return false;

        const key = RegistryKeys.worker_online_lease(workerId);
        const current = await this.redis.get(key);
        if (decodeWorkerPresence(current).token !== token) return false;

        const res = await this.redis.expire(key, ttlSeconds);
        return res === 1;
    }

    async releaseWorkerId(workerId: string, token?: string): Promise<boolean> {
        const expected = token || this.lockTokens.get(workerId);
        if (!expected) return false;

        const key = RegistryKeys.worker_online_lease(workerId);
        const current = await this.redis.get(key);
        if (decodeWorkerPresence(current).token !== expected) return false;

        await this.redis.del(key);
        this.lockTokens.delete(workerId);
        return true;
    }

    /**
     * Initialize a new execution record with timeline tracking.
     * Sets created_at, updated_at, started_at=0, finished_at=0, and initial timeline entry.
     */
    async initializeExecution(execution: Record<string, any>): Promise<void> {
        const now = Date.now();
        const executionId = String(execution.execution_id);
        const messageId = String(execution.message_id);
        const sessionId = String(execution.session_id);
        const regKey = RegistryKeys.session_registry(sessionId);

        const record = {
            ...execution,
            created_at: now,
            updated_at: now,
            started_at: 0,
            finished_at: 0,
            timeline: [{ status: execution.status || 'QUEUED', timestamp: now }],
        };

        await this.redis.pipeline()
            .hset(regKey, `exec:${executionId}`, JSON.stringify(record))
            .hset(regKey, `msg_map:${messageId}`, executionId)
            .expire(regKey, RegistryKeys.DEFAULT_SESSION_TTL)
            .exec();
    }

    /**
     * Update execution status with timeline tracking.
     */
    async updateExecutionStatus(
        executionId: string,
        sessionId: string,
        status: string,
        extra?: Record<string, any>
    ): Promise<void> {
        const current = await this.getExecution(executionId, sessionId);
        if (!current) return;

        const now = Date.now();
        current.status = status;
        current.updated_at = now;

        if (status === 'RUNNING' && (!current.started_at || current.started_at === 0)) {
            current.started_at = now;
        }

        if (extra) {
            Object.assign(current, extra);
        }

        const timeline = Array.isArray(current.timeline) ? current.timeline : [];
        timeline.push({ status, timestamp: now });
        current.timeline = timeline;

        const regKey = RegistryKeys.session_registry(sessionId);
        await this.redis.pipeline()
            .hset(regKey, `exec:${executionId}`, JSON.stringify(current))
            .expire(regKey, RegistryKeys.DEFAULT_SESSION_TTL)
            .exec();
    }

    /**
     * Update execution status by message ID.
     */
    async updateExecutionStatusByMessage(
        messageId: string,
        sessionId: string,
        status: string
    ): Promise<void> {
        const regKey = RegistryKeys.session_registry(sessionId);
        const executionId = await this.redis.hget(regKey, `msg_map:${messageId}`);
        if (!executionId) return;
        await this.updateExecutionStatus(executionId, sessionId, status);
    }

    /**
     * Get all execution records for a session.
     */
    async getAllSessionExecutions(sessionId: string): Promise<Record<string, any>[]> {
        const regKey = RegistryKeys.session_registry(sessionId);
        const allFields = await this.redis.hgetall(regKey);
        if (!allFields) return [];

        const executions: Record<string, any>[] = [];
        for (const [key, value] of Object.entries(allFields)) {
            if (key.startsWith('exec:')) {
                try {
                    executions.push(JSON.parse(value));
                } catch {
                    // Skip invalid JSON
                }
            }
        }
        return executions;
    }

    /**
     * Mark an execution as cancel_requested WITHOUT changing its status.
     * Used for terminal ancestors in cascading cancellation.
     */
    async markCancelRequested(
        executionId: string,
        sessionId: string,
        reason?: string
    ): Promise<void> {
        const current = await this.getExecution(executionId, sessionId);
        if (!current) return;

        const now = Date.now();
        current.cancel_requested = true;
        if (reason) {
            current.cancel_reason = reason;
        }
        current.updated_at = now;

        const regKey = RegistryKeys.session_registry(sessionId);
        await this.redis.pipeline()
            .hset(regKey, `exec:${executionId}`, JSON.stringify(current))
            .expire(regKey, RegistryKeys.DEFAULT_SESSION_TTL)
            .exec();
    }

    async saveExecution(execution: Record<string, any>): Promise<void> {
        const executionId = String(execution.execution_id);
        const messageId = String(execution.message_id);
        const sessionId = String(execution.session_id);
        const regKey = RegistryKeys.session_registry(sessionId);

        const now = Date.now();
        if (!execution.created_at) {
            execution.created_at = now;
        }
        if (!execution.updated_at) {
            execution.updated_at = now;
        }
        if (!execution.timeline) {
            execution.timeline = [{ status: execution.status || 'QUEUED', timestamp: now }];
        }

        const encodedData = JSON.stringify(execution);

        await this.redis.pipeline()
            .hset(regKey, `exec:${executionId}`, encodedData)
            .hset(regKey, `msg_map:${messageId}`, executionId)
            .expire(regKey, RegistryKeys.DEFAULT_SESSION_TTL)
            .exec();
    }

    async getExecution(executionId: string, sessionId?: string): Promise<Record<string, any> | null> {
        if (!sessionId) {
            return null;
        }
        const regKey = RegistryKeys.session_registry(sessionId);
        const data = await this.redis.hget(regKey, `exec:${executionId}`);
        if (!data) {
            return null;
        }
        return JSON.parse(data);
    }

    async getExecutionByMessageId(messageId: string, sessionId?: string): Promise<Record<string, any> | null> {
        if (!sessionId) return null;
        const regKey = RegistryKeys.session_registry(sessionId);
        const executionId = await this.redis.hget(regKey, `msg_map:${messageId}`);
        if (!executionId) {
            return null;
        }
        return this.getExecution(executionId, sessionId);
    }

    async markExecutionCancelling(executionId: string, sessionId: string, reason: string): Promise<void> {
        const current = await this.getExecution(executionId, sessionId);
        if (!current) {
            return;
        }

        const now = Date.now();
        current.status = 'CANCELLING';
        current.cancel_requested = true;
        current.cancel_reason = reason;
        current.updated_at = now;

        const timeline = Array.isArray(current.timeline) ? current.timeline : [];
        timeline.push({ status: 'CANCELLING', timestamp: now });
        current.timeline = timeline;

        const regKey = RegistryKeys.session_registry(sessionId);
        await this.redis.pipeline()
            .hset(regKey, `exec:${executionId}`, JSON.stringify(current))
            .expire(regKey, RegistryKeys.DEFAULT_SESSION_TTL)
            .exec();
    }

    async markExecutionFinished(executionId: string, sessionId: string, status: string): Promise<void> {
        const current = await this.getExecution(executionId, sessionId);
        if (!current) {
            return;
        }

        const now = Date.now();
        current.status = status;
        current.finished_at = now;
        current.updated_at = now;

        const timeline = Array.isArray(current.timeline) ? current.timeline : [];
        timeline.push({ status, timestamp: now });
        current.timeline = timeline;

        const regKey = RegistryKeys.session_registry(sessionId);
        await this.redis.pipeline()
            .hset(regKey, `exec:${executionId}`, JSON.stringify(current))
            .expire(regKey, RegistryKeys.DEFAULT_SESSION_TTL)
            .exec();
    }

    private encodeExecution(execution: Record<string, any>): Record<string, string> {
        const encoded: Record<string, string> = {};
        for (const [key, value] of Object.entries(execution)) {
            if (typeof value === 'boolean') {
                encoded[key] = value ? '1' : '0';
            } else {
                encoded[key] = String(value);
            }
        }
        return encoded;
    }

    private decodeExecution(execution: Record<string, string>): Record<string, any> {
        const decoded: Record<string, any> = {};
        const intFields = new Set(['created_at', 'started_at', 'finished_at', 'updated_at']);
        const boolFields = new Set(['cancel_requested']);

        for (const [key, value] of Object.entries(execution)) {
            if (boolFields.has(key)) {
                decoded[key] = value === '1' || value === 'true' || value === 'True';
            } else if (intFields.has(key) && /^\d+$/.test(value)) {
                decoded[key] = Number(value);
            } else {
                decoded[key] = value;
            }
        }
        return decoded;
    }
}

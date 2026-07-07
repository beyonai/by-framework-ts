/**
 * Lightweight observability snapshot builder.
 *
 * Scans online workers, queue depths, and per-worker execution summaries,
 * then writes compact trend points to a Redis sorted set for history.
 */
import { Redis } from 'ioredis';
import { RegistryKeys } from '../constants';

const REDIS_HISTORY_KEY = 'by_framework:obs:history';
const REDIS_HISTORY_TTL_MS = 2 * 60 * 60 * 1000; // two hours of trend data

export interface HistoryPoint {
    generated_at: number;
    workers_online: number;
    active_executions: number;
    queued_executions: number;
    failed_executions: number;
    queue_depth_total: number;
    [key: string]: number;
}

export async function saveHistoryPointToRedis(redis: Redis, point: HistoryPoint): Promise<void> {
    const score = point.generated_at;
    if (!score) return;
    try {
        await redis.zadd(REDIS_HISTORY_KEY, score, JSON.stringify(point));
        const cutoff = score - REDIS_HISTORY_TTL_MS;
        await redis.zremrangebyscore(REDIS_HISTORY_KEY, '-inf', cutoff);
    } catch {
        // best-effort
    }
}

export async function loadHistoryFromRedis(redis: Redis, limit = 20): Promise<HistoryPoint[]> {
    try {
        const raw = await redis.zrange(REDIS_HISTORY_KEY, -Math.max(limit, 1), -1);
        return raw.map(s => {
            try { return JSON.parse(s) as HistoryPoint; } catch { return null; }
        }).filter((p): p is HistoryPoint => p !== null);
    } catch {
        return [];
    }
}

async function scanOnlineWorkerIds(redis: Redis, limit = 300): Promise<string[]> {
    const pattern = RegistryKeys.worker_online_lease_scan_pattern();
    const workerIds: string[] = [];
    let cursor = '0';
    do {
        const [nextCursor, keys] = await redis.scan(cursor, 'MATCH', pattern, 'COUNT', 100);
        cursor = nextCursor;
        for (const key of keys) {
            const workerId = RegistryKeys.worker_id_from_online_lease_key(key);
            if (workerId !== null) {
                workerIds.push(workerId);
                if (limit && workerIds.length >= limit) return [...new Set(workerIds)].sort();
            }
        }
    } while (cursor !== '0');
    return [...new Set(workerIds)].sort();
}

async function getQueueDepth(redis: Redis, agentType: string): Promise<number> {
    const key = `byai_gateway:ctrl:agent_type:${agentType}`;
    try {
        return await redis.xlen(key);
    } catch {
        return 0;
    }
}

export interface ObservabilitySnapshot {
    generated_at: number;
    totals: { workers_online: number; agent_types: number; active_executions: number };
    status_counts: Record<string, number>;
    queue_depth_total: number;
}

export async function buildObservabilitySnapshot(redis: Redis): Promise<ObservabilitySnapshot> {
    const workerIds = await scanOnlineWorkerIds(redis);

    // Collect agent types from all workers
    const agentTypeSet = new Set<string>();
    for (const workerId of workerIds) {
        try {
            const types = await redis.smembers(RegistryKeys.workerDeclaredAgentTypes(workerId));
            types.forEach(t => agentTypeSet.add(t));
        } catch {
            // skip
        }
    }
    const agentTypes = [...agentTypeSet];

    // Collect queue depths
    const depths = await Promise.all(agentTypes.map(t => getQueueDepth(redis, t)));
    const queueDepthTotal = depths.reduce((a, b) => a + b, 0);

    return {
        generated_at: Date.now(),
        totals: {
            workers_online: workerIds.length,
            agent_types: agentTypes.length,
            active_executions: 0,
        },
        status_counts: {},
        queue_depth_total: queueDepthTotal,
    };
}

export function buildHistoryPoint(snapshot: ObservabilitySnapshot): HistoryPoint {
    return {
        generated_at: snapshot.generated_at,
        workers_online: snapshot.totals.workers_online,
        active_executions: snapshot.totals.active_executions,
        queued_executions: snapshot.status_counts['QUEUED'] ?? 0,
        failed_executions: snapshot.status_counts['FAILED'] ?? 0,
        queue_depth_total: snapshot.queue_depth_total,
    };
}

/**
 * Background metrics collector with distributed Redis lock.
 *
 * Only one worker process at a time writes history points; the lock is a
 * simple Redis SET NX with an expiry renewed on every successful collection
 * cycle so the lock outlives individual iteration failures.
 */
import { Redis } from 'ioredis';
import { getRedis } from '../redis_client';
import { buildObservabilitySnapshot, buildHistoryPoint, saveHistoryPointToRedis } from './snapshot';

const COLLECTOR_LOCK_KEY = 'by_framework:obs:collector_lock';
const LOCK_TTL_MULTIPLIER = 3;

function envInt(name: string, defaultVal: number): number {
    const raw = process.env[name];
    if (!raw) return defaultVal;
    const parsed = parseInt(raw, 10);
    return isNaN(parsed) ? defaultVal : parsed;
}

function envBool(name: string, defaultVal: boolean): boolean {
    const raw = (process.env[name] || '').trim().toLowerCase();
    if (['1', 'true', 'yes', 'on', 'enabled'].includes(raw)) return true;
    if (['0', 'false', 'no', 'off', 'disabled'].includes(raw)) return false;
    return defaultVal;
}

export class MetricsCollector {
    private readonly redis: Redis;
    readonly workerId: string;
    readonly intervalMs: number;
    readonly enabled: boolean;
    private readonly lockTtlSeconds: number;
    private running = false;
    private timer: NodeJS.Timeout | null = null;

    constructor(options: {
        redis?: Redis;
        workerId?: string;
        intervalSeconds?: number;
        enabled?: boolean;
    } = {}) {
        this.redis = options.redis ?? getRedis();
        this.workerId = options.workerId ?? `collector-${Math.random().toString(36).slice(2)}`;
        const intervalSeconds = options.intervalSeconds
            ?? envInt('BY_FRAMEWORK_METRICS_HISTORY_INTERVAL_SECONDS', 5);
        this.intervalMs = intervalSeconds * 1000;
        this.enabled = options.enabled
            ?? (envBool('BY_FRAMEWORK_METRICS_HISTORY_ENABLED', true)
                && envBool('BY_FRAMEWORK_OBSERVABILITY_ENABLED', true));
        this.lockTtlSeconds = Math.max(intervalSeconds * LOCK_TTL_MULTIPLIER, 15);
    }

    start(): void {
        if (!this.enabled || this.running) return;
        this.running = true;
        const tick = async () => {
            if (!this.running) return;
            try {
                await this._collectOnce();
            } catch {
                // errors are handled inside _collectOnce
            }
            if (this.running) {
                this.timer = setTimeout(tick, this.intervalMs);
            }
        };
        this.timer = setTimeout(tick, this.intervalMs);
    }

    stop(): void {
        this.running = false;
        if (this.timer) {
            clearTimeout(this.timer);
            this.timer = null;
        }
        this._releaseLock().catch(() => {});
    }

    private async _collectOnce(): Promise<void> {
        if (!this.enabled) return;
        if (!await this._acquireOrRenewLock()) return;
        try {
            const snapshot = await buildObservabilitySnapshot(this.redis);
            const point = buildHistoryPoint(snapshot);
            await saveHistoryPointToRedis(this.redis, point);
        } catch (err) {
            console.debug(`[MetricsCollector] Snapshot failed: ${err}`);
        }
    }

    private async _acquireOrRenewLock(): Promise<boolean> {
        try {
            const acquired = await this.redis.set(
                COLLECTOR_LOCK_KEY,
                this.workerId,
                'EX', this.lockTtlSeconds,
                'NX'
            );
            if (acquired === 'OK') return true;
            const current = await this.redis.get(COLLECTOR_LOCK_KEY);
            if (current === this.workerId) {
                await this.redis.expire(COLLECTOR_LOCK_KEY, this.lockTtlSeconds);
                return true;
            }
            return false;
        } catch {
            return false;
        }
    }

    private async _releaseLock(): Promise<void> {
        try {
            const current = await this.redis.get(COLLECTOR_LOCK_KEY);
            if (current === this.workerId) {
                await this.redis.del(COLLECTOR_LOCK_KEY);
            }
        } catch {
            // best-effort
        }
    }

    snapshot(): object {
        return {
            worker_id: this.workerId,
            enabled: this.enabled,
            interval_ms: this.intervalMs,
            lock_key: COLLECTOR_LOCK_KEY,
        };
    }
}

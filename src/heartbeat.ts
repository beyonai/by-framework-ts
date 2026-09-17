import { Redis } from 'ioredis';
import { WorkerRegistry } from './registry';
import { getRedis } from './redis_client';
import { RegistryKeys } from './constants';

/**
 * What one lease-renewal attempt did.
 *
 * `heartbeatWorker` collapses three very different situations into a boolean
 * plus a possible throw; naming them keeps the caller from treating "someone
 * else owns this worker id" as if it were a transient Redis error.
 */
export type RenewOutcome =
    /** The lease is ours and its TTL was extended. */
    | 'renewed'
    /** The lease is held by another token: a second process owns this worker id. */
    | 'fenced'
    /** Redis itself failed. Transient until proven otherwise. */
    | 'failed';

/** Callbacks reporting what the renewal loop observed. */
export interface HeartbeatObservers {
    /** A renewal succeeded, with the timestamp it succeeded at. */
    readonly onRenewOk?: (at: number) => void;
    /** Another process owns this worker id; this heartbeat has stopped. */
    readonly onFenced?: () => void;
}

/** Delay before the one immediate retry that follows a failed renewal. */
const RENEW_RETRY_DELAY_MS = 500;

export class WorkerHeartbeat {
    private registry: WorkerRegistry;
    private intervalId: NodeJS.Timeout | null = null;
    private retryTimer: NodeJS.Timeout | null = null;
    private leaseTtlSeconds: number;
    private lifecycleCallback?: (lifecycle: string) => void;
    private denylistRefresh?: (denied: Set<string>) => void;
    private healthCheck?: () => boolean;
    private onUnhealthy?: () => void;
    private observers: HeartbeatObservers;
    private consecutiveFailures = 0;
    private lastRenewOkAt = 0;

    /**
     * The parameter list stays positional because `WorkerHeartbeat` is part of
     * the public export surface and is constructed positionally in `examples/`.
     * New callbacks go in the trailing `observers` object rather than extending
     * the positional list further.
     */
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
        onUnhealthy?: () => void,
        observers: HeartbeatObservers = {}
    ) {
        this.registry = registry || new WorkerRegistry(this.redis);
        this.leaseTtlSeconds = leaseTtlSeconds ?? RegistryKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS;
        this.lifecycleCallback = lifecycleCallback;
        this.denylistRefresh = denylistRefresh;
        this.healthCheck = healthCheck;
        this.onUnhealthy = onUnhealthy;
        this.observers = observers;
    }

    /** Timestamp of the last successful renewal, or 0 if none has succeeded yet. */
    get lastSuccessfulRenewAt(): number {
        return this.lastRenewOkAt;
    }

    /** Consecutive failed renewal attempts; reset by any success. */
    get failureCount(): number {
        return this.consecutiveFailures;
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

        // Initial heartbeat. A fence here means a second process already owns
        // this worker id, so there is nothing to start.
        if ((await this.renewLease()) === 'fenced') {
            return;
        }

        // Propagate startup lifecycle to runner immediately (before interval fires)
        if (this.lifecycleCallback && startupLifecycle !== 'active') {
            this.lifecycleCallback(startupLifecycle);
        }

        this.intervalId = setInterval(() => {
            void this.tick();
        }, this.intervalMs);

        console.log(`[${this.workerId}] Standalone heartbeat started`);
    }

    async stop(): Promise<void> {
        if (this.retryTimer) {
            clearTimeout(this.retryTimer);
            this.retryTimer = null;
        }
        if (this.intervalId) {
            clearInterval(this.intervalId);
            this.intervalId = null;
            console.log(`[${this.workerId}] Standalone heartbeat stopped`);
        }
    }

    /**
     * One renewal attempt, classified.
     *
     * A `false` return from `heartbeatWorker` is not a failure to retry: it
     * means the lease carries someone else's token, so this process has lost
     * ownership of the worker id and must stop renewing rather than fight over
     * it. Retrying would just alternate the lease between two live workers.
     */
    private async renewLease(): Promise<RenewOutcome> {
        let renewed = false;
        try {
            renewed = await this.registry.heartbeatWorker(this.workerId, this.leaseTtlSeconds);
        } catch (error) {
            this.consecutiveFailures++;
            console.error(
                `[${this.workerId}] Heartbeat failed (attempt ${this.consecutiveFailures}):`,
                error
            );
            this.scheduleRetryAfterFailure();
            return 'failed';
        }

        if (!renewed) {
            console.error(
                `[${this.workerId}] Presence lease is held by another instance — worker id conflict; stopping heartbeat`
            );
            await this.stop();
            this.observers.onFenced?.();
            return 'fenced';
        }

        this.consecutiveFailures = 0;
        this.lastRenewOkAt = Date.now();
        this.observers.onRenewOk?.(this.lastRenewOkAt);
        return 'renewed';
    }

    /**
     * Retry once, soon, after the first failure in a run.
     *
     * The lease tolerates only two missed renewals (5s interval, 15s TTL), so
     * waiting a full interval after a transient error spends half the budget
     * doing nothing. Later failures wait for the interval — if Redis is still
     * down at that point, hammering it does not help.
     */
    private scheduleRetryAfterFailure(): void {
        if (this.consecutiveFailures !== 1 || this.retryTimer || !this.intervalId) {
            return;
        }
        this.retryTimer = setTimeout(() => {
            this.retryTimer = null;
            void this.renewLease();
        }, RENEW_RETRY_DELAY_MS);
        this.retryTimer.unref?.();
    }

    /** One full heartbeat cycle: health gate, renewal, then admin state sync. */
    private async tick(): Promise<void> {
        try {
            // Health check: if the consumer loop has stalled, stop the heartbeat
            // so the lease expires and the worker is evicted from routing.
            if (this.healthCheck && !this.healthCheck()) {
                console.error(`[${this.workerId}] Heartbeat stopping: consumer loop is unhealthy`);
                await this.stop();
                if (this.onUnhealthy) this.onUnhealthy();
                return;
            }

            // Skip the rest of the cycle unless the lease is actually ours: on a
            // fence this heartbeat is already stopped, and on a failure the
            // retry owns the next attempt.
            if ((await this.renewLease()) !== 'renewed') {
                return;
            }

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
            console.error(`[${this.workerId}] Heartbeat cycle failed:`, error);
        }
    }
}

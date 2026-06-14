/**
 * WorkerManager — admin-side API for controlling worker lifecycle and agent-type access.
 *
 * Responsibilities:
 *   - Suspend / resume / evict individual workers (lifecycle control).
 *   - Deny / allow workers from consuming a specific agent_type (admission control).
 *
 * Lifecycle commands are delivered via two channels:
 *   1. Push: XADD to byai_gateway:ctrl:worker:{worker_id} (immediate delivery).
 *   2. Pull: HSET to byai_gateway:registry:worker:admin:{worker_id} (durable fallback,
 *      read by the worker's heartbeat loop every heartbeat interval).
 */

import { v4 as uuidv4 } from 'uuid';
import { Redis } from 'ioredis';
import { getRedis } from '../redis_client';
import { WorkerRegistry } from '../registry';
import { QueueNames } from '../constants';
import {
    SuspendWorkerCommand,
    ResumeWorkerCommand,
    EvictWorkerCommand,
} from '../protocol/commands';
import { MessageHeader } from '../protocol/message_header';

function adminHeader(): MessageHeader {
    return MessageHeader.fromDict({
        session_id: 'admin',
        trace_id: uuidv4().replace(/-/g, ''),
        message_id: uuidv4().replace(/-/g, ''),
    });
}

export class WorkerManager {
    private redis: Redis;
    private registry: WorkerRegistry;

    constructor(redisClient?: Redis, registry?: WorkerRegistry) {
        this.redis = redisClient || getRedis();
        this.registry = registry || new WorkerRegistry(this.redis);
    }

    // ------------------------------------------------------------------
    // Lifecycle control
    // ------------------------------------------------------------------

    /**
     * Pause a running worker from consuming new tasks.
     *
     * Immediately removes the worker from all agent_type:members sets so that
     * routing skips it at once; the worker re-adds itself on resume.
     */
    async suspendWorker(workerId: string, reason: string = ''): Promise<void> {
        await this.registry.setWorkerAdminState(workerId, 'suspended', reason);
        await this.registry.removeWorkerFromTypeMembers(workerId);
        const command = new SuspendWorkerCommand(adminHeader(), reason);
        await this.redis.xadd(
            QueueNames.worker_ctrl_stream(workerId),
            '*',
            ...Object.entries(command.toRedisPayload()).flat()
        );
    }

    /**
     * Resume a previously suspended worker.
     *
     * Re-adds the worker to all agent_type:members sets immediately (respecting
     * the denylist), so routing can reach it again without waiting for the next
     * heartbeat cycle.
     */
    async resumeWorker(workerId: string): Promise<void> {
        await this.registry.setWorkerAdminState(workerId, 'active', '');
        await this.registry.restoreWorkerToTypeMembers(workerId);
        const command = new ResumeWorkerCommand(adminHeader());
        await this.redis.xadd(
            QueueNames.worker_ctrl_stream(workerId),
            '*',
            ...Object.entries(command.toRedisPayload()).flat()
        );
    }

    /**
     * Shut down a worker.
     *
     * Immediately removes the worker from all agent_type:members sets so
     * routing stops sending new messages before the heartbeat TTL expires.
     */
    async evictWorker(
        workerId: string,
        options: { force?: boolean; reason?: string } = {}
    ): Promise<void> {
        const { force = false, reason = '' } = options;
        await this.registry.setWorkerAdminState(workerId, 'evicted', reason);
        await this.registry.removeWorkerFromTypeMembers(workerId);
        const command = new EvictWorkerCommand(adminHeader(), reason, force);
        await this.redis.xadd(
            QueueNames.worker_ctrl_stream(workerId),
            '*',
            ...Object.entries(command.toRedisPayload()).flat()
        );
    }

    // ------------------------------------------------------------------
    // Agent-type admission control
    // ------------------------------------------------------------------

    /**
     * Prevent workerId from consuming the agent_type stream.
     */
    async denyWorkerForType(agentType: string, workerId: string): Promise<void> {
        await this.registry.denyWorkerForType(agentType, workerId);
    }

    /**
     * Remove workerId from the denylist for agent_type.
     */
    async allowWorkerForType(agentType: string, workerId: string): Promise<void> {
        await this.registry.allowWorkerForType(agentType, workerId);
    }

    /**
     * Return all worker_ids currently denied for agent_type.
     */
    async getTypeDenylist(agentType: string): Promise<string[]> {
        return this.registry.getAgentTypeDenylist(agentType);
    }

    // ------------------------------------------------------------------
    // Status queries
    // ------------------------------------------------------------------

    /**
     * Return the admin-controlled state for a worker.
     * Returns an empty object when no admin state has been set (default active).
     */
    async getWorkerAdminState(workerId: string): Promise<Record<string, string>> {
        return this.registry.getWorkerAdminState(workerId);
    }

    /**
     * Remove all admin state for a worker, restoring default-active behaviour.
     */
    async clearWorkerAdminState(workerId: string): Promise<void> {
        await this.registry.clearWorkerAdminState(workerId);
    }
}

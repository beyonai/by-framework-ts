import { WorkerRegistry } from './registry';

/**
 * The renewal sequence a heartbeat tick performs: renew the lease, read admin
 * lifecycle, re-register membership when active, and optionally refresh the
 * denylist. Shared between the main-thread fallback interval (heartbeat.ts)
 * and the worker_threads tick (heartbeat_worker_thread.ts) so the two
 * execution contexts can never drift into different renewal behavior.
 */
export interface HeartbeatTickParams {
    readonly registry: WorkerRegistry;
    readonly workerId: string;
    readonly agentTypes: ReadonlyArray<string>;
    readonly leaseTtlSeconds: number;
    readonly denylistEnabled: boolean;
}

export interface HeartbeatTickResult {
    readonly lifecycle: string;
    readonly denied?: ReadonlyArray<string>;
}

export async function runHeartbeatTick(params: HeartbeatTickParams): Promise<HeartbeatTickResult> {
    await params.registry.heartbeatWorker(params.workerId, params.leaseTtlSeconds);

    const state = await params.registry.getWorkerAdminState(params.workerId);
    const lifecycle = state.lifecycle || 'active';

    if (lifecycle === 'active') {
        await params.registry.registerWorkerMembership(params.workerId, [...params.agentTypes]);
    }

    if (!params.denylistEnabled) {
        return { lifecycle };
    }

    const denied: string[] = [];
    for (const agentType of params.agentTypes) {
        if (await params.registry.isWorkerDeniedForType(agentType, params.workerId)) {
            denied.push(agentType);
        }
    }
    return { lifecycle, denied };
}

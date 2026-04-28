import type { WorkerRegistry } from '../registry';

type RegistryWithExecution = WorkerRegistry & {
    initializeExecution?: (execution: Record<string, unknown>) => Promise<void>;
    saveExecution?: (execution: Record<string, unknown>) => Promise<void>;
};

/**
 * Persist queued execution before ctrl publish (same behavior as GatewayClient.initializeQueuedExecution).
 */
export async function initializeQueuedExecution(
    registry: RegistryWithExecution,
    execution: Record<string, unknown>
): Promise<void> {
    if (typeof registry.initializeExecution === 'function') {
        await registry.initializeExecution(execution);
        return;
    }
    if (typeof registry.saveExecution === 'function') {
        await registry.saveExecution(execution);
    }
}

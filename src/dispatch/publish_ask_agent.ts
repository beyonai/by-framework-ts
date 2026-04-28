import type { Redis } from 'ioredis';
import type { WorkerRegistry } from '../registry';
import type { AskAgentCommand } from '../protocol/commands';
import { buildExecutionRecordForAskAgentCommand, generateExecutionId } from './ask_agent_build';
import { initializeQueuedExecution } from './execution_init';
import type { CommandBus, ExecutionInitializer } from './ports';
import type { ExecutionSourceAgentFallback } from './types';

type RegistryWithExecution = WorkerRegistry & {
    initializeExecution?: (execution: Record<string, unknown>) => Promise<void>;
    saveExecution?: (execution: Record<string, unknown>) => Promise<void>;
};

/**
 * Initialize execution (best-effort) then XADD AskAgent command — shared by GatewayClient and tests.
 */
export async function publishAskAgentCommand(params: {
    readonly redis: Redis;
    readonly registry: RegistryWithExecution;
    readonly command: AskAgentCommand;
    readonly streamName: string;
    readonly executionSourceAgentFallback: ExecutionSourceAgentFallback;
}): Promise<void> {
    const executionId = generateExecutionId();
    const record = buildExecutionRecordForAskAgentCommand(
        params.command,
        params.streamName,
        executionId,
        params.executionSourceAgentFallback
    );
    await initializeQueuedExecution(params.registry, record).catch(() => undefined);
    await params.redis.xadd(params.streamName, '*', 'data', JSON.stringify(params.command.toDict()));
}

/** Variant when caller already has a materialized execution record (e.g. from dispatchAskAgent). */
export async function publishWithExecutionRecord(params: {
    readonly execution: ExecutionInitializer;
    readonly bus: CommandBus;
    readonly executionRecord: Readonly<Record<string, unknown>>;
    readonly streamName: string;
    readonly serializedCommandJson: string;
}): Promise<void> {
    await params.execution.init({ ...params.executionRecord });
    await params.bus.publish(params.streamName, params.serializedCommandJson);
}

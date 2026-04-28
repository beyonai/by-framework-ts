import type { Redis } from 'ioredis';
import { QueueNames } from '../constants';
import { AgentState } from '../protocol/agent_state';
import { buildAskAgentPublishArtifacts, resolveCallAgentPublishIds } from './ask_agent_build';
import { initializeQueuedExecution } from './execution_init';
import type { AskAgentDispatchDeps } from './ports';
import type { WorkerRegistry } from '../registry';
import { publishWithExecutionRecord } from './publish_ask_agent';
import type { CallAgentPublishInput, CallAgentPublishResult } from './types';

const AGENT_TYPE_NOT_FOUND = 'AGENT_TYPE_NOT_FOUND';

/**
 * Publish-side pipeline: optional online probe → build AskAgent → init execution → XADD ctrl stream.
 * Does not read session streams; fully event-driven at the transport layer.
 */
export async function callAgent(
    deps: AskAgentDispatchDeps,
    input: CallAgentPublishInput
): Promise<CallAgentPublishResult> {
    const probeAgentType = input.probeAgentType ?? true;
    if (probeAgentType) {
        const probe = await deps.probe.probeAgentTypeOnline(input.targetAgentType);
        if (!probe.ok) {
            return {
                status: AgentState.FAILED,
                messageId: '',
                parentMessageId: input.parentMessageId || input.defaultParentMessageId,
                targetAgentType: input.targetAgentType,
                error: probe.error ?? `No alive worker found with agent type '${input.targetAgentType}'`,
                error_code: probe.error_code ?? AGENT_TYPE_NOT_FOUND,
            };
        }
    }

    const { messageId, parentMessageId, waitForReply } = resolveCallAgentPublishIds(input);
    const artifacts = buildAskAgentPublishArtifacts(
        input,
        messageId,
        parentMessageId,
        waitForReply,
        deps.queueNames
    );

    await publishWithExecutionRecord({
        execution: deps.execution,
        bus: deps.bus,
        executionRecord: artifacts.executionRecord,
        streamName: artifacts.ctrlStreamName,
        serializedCommandJson: JSON.stringify(artifacts.command.toDict()),
    });

    const runtimeHint = waitForReply ? ('suspend' as const) : ('transfer' as const);
    return {
        status: AgentState.QUEUED,
        messageId,
        parentMessageId,
        targetAgentType: input.targetAgentType,
        runtimeHint,
    };
}

type RegistryWithExecution = WorkerRegistry & {
    initializeExecution?: (execution: Record<string, unknown>) => Promise<void>;
    saveExecution?: (execution: Record<string, unknown>) => Promise<void>;
};

/**
 * Build default deps for Redis + WorkerRegistry (aligned with GatewayClient online probe: `hasOnlineAgentType`).
 */
export function createRedisCallAgentDeps(params: {
    readonly redis: Redis;
    readonly registry: WorkerRegistry;
    readonly queueNames?: typeof QueueNames;
}): AskAgentDispatchDeps {
    const { redis, registry } = params;
    const queueNames = params.queueNames ?? QueueNames;
    return {
        probe: {
            async probeAgentTypeOnline(agentType: string) {
                const [ok] = await registry.hasOnlineAgentType(agentType);
                if (!ok) {
                    return {
                        ok: false,
                        error_code: AGENT_TYPE_NOT_FOUND,
                        error: `No alive worker found with agent type '${agentType}'`,
                    };
                }
                return { ok: true };
            },
        },
        execution: {
            async init(execution: Record<string, unknown>) {
                await initializeQueuedExecution(registry as RegistryWithExecution, execution);
            },
        },
        bus: {
            async publish(streamName: string, serializedCommandJson: string) {
                await redis.xadd(streamName, '*', 'data', serializedCommandJson);
            },
        },
        queueNames,
    };
}

/** @deprecated Use `callAgent` instead. */
export const dispatchAskAgent = callAgent;
/** @deprecated Use `createRedisCallAgentDeps` instead. */
export const createRedisAskAgentDispatchDeps = createRedisCallAgentDeps;

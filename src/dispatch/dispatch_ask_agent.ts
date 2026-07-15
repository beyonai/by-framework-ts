import type { Redis } from 'ioredis';
import { QueueNames } from '../constants';
import { AgentState } from '../protocol/agent_state';
import { buildAskAgentPublishArtifacts, resolveCallAgentPublishIds } from './ask_agent_build';
import { initializeQueuedExecution } from './execution_init';
import type { AskAgentDispatchDeps } from './ports';
import type { WorkerRegistry } from '../registry';
import { publishWithExecutionRecord } from './publish_ask_agent';
import type { CallAgentPublishInput, CallAgentPublishResult } from './types';
import { AvailabilityRouter, AvailabilityStatus, RoutePolicy } from '../availability';
import { AskAgentCommand } from '../protocol/commands';
import { MessageHeader } from '../protocol/message_header';

const AGENT_TYPE_NOT_FOUND = 'AGENT_TYPE_NOT_FOUND';

/**
 * Publish-side pipeline: optional online probe → build AskAgent → init execution → XADD ctrl stream.
 * Does not read session streams; fully event-driven at the transport layer.
 */
export async function callAgent(
    deps: AskAgentDispatchDeps,
    input: CallAgentPublishInput
): Promise<CallAgentPublishResult> {
    const policy = input.routePolicy ?? ((input.probeAgentType ?? true) ? RoutePolicy.FAIL_FAST : RoutePolicy.SEND_ANYWAY);
    if (!deps.availability) {
      const probeAgentType = policy === RoutePolicy.FAIL_FAST;
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
    }

    const { messageId, parentMessageId, waitForReply } = resolveCallAgentPublishIds(input);
    const artifacts = buildAskAgentPublishArtifacts(
        input,
        messageId,
        parentMessageId,
        waitForReply,
        deps.queueNames
    );

    const executionId = String(artifacts.executionRecord.execution_id);
    const availability = deps.availability
        ? await deps.availability.prepare({ ...input, routePolicy: policy }, artifacts.command.toDict(), executionId, messageId)
        : { status: AvailabilityStatus.DELIVER_NOW, streamName: artifacts.ctrlStreamName };
    if (availability.status === AvailabilityStatus.REJECT) {
        await deps.execution.init({
            ...artifacts.executionRecord, status: AgentState.FAILED, route_policy: policy,
            route_status: availability.status, availability_error: availability.error || '',
            availability_error_code: availability.errorCode || AGENT_TYPE_NOT_FOUND,
        });
        return {
            status: AgentState.FAILED, messageId: '', parentMessageId,
            targetAgentType: input.targetAgentType, error: availability.error,
            error_code: availability.errorCode || AGENT_TYPE_NOT_FOUND,
            routeStatus: availability.status,
        };
    }
    let command = artifacts.command;
    const executionRecord: Record<string, unknown> = { ...artifacts.executionRecord };
    if (availability.selectedAgentType) {
        const header = command.header;
        command = new AskAgentCommand(
            new MessageHeader(header.messageId, header.sessionId, header.traceId, {
                sourceAgentType: header.sourceAgentType, targetAgentType: availability.selectedAgentType,
                parentMessageId: header.parentMessageId, taskGroupId: header.taskGroupId,
                userCode: header.userCode, userName: header.userName, metadata: header.metadata,
                traceParentSpanId: header.traceParentSpanId,
                langfuseParentObservationId: header.langfuseParentObservationId,
            }), command.content, command.waitForReply, command.extraPayload
        );
        executionRecord.target_agent_type = availability.selectedAgentType;
    }
    executionRecord.stream_name = availability.streamName || artifacts.ctrlStreamName;
    executionRecord.route_policy = policy;
    executionRecord.route_status = availability.status;
    executionRecord.selected_agent_type = availability.selectedAgentType || '';

    if (availability.status === AvailabilityStatus.QUEUE_PENDING) {
        await deps.execution.init(executionRecord);
    } else {
        await publishWithExecutionRecord({
            execution: deps.execution, bus: deps.bus, executionRecord,
            streamName: availability.streamName || artifacts.ctrlStreamName,
            serializedCommandJson: JSON.stringify(command.toDict()),
        });
    }

    const runtimeHint = waitForReply ? ('suspend' as const) : ('transfer' as const);
    return {
        status: AgentState.QUEUED,
        messageId,
        parentMessageId,
        targetAgentType: availability.selectedAgentType || input.targetAgentType,
        runtimeHint,
        routeStatus: availability.status,
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
    const router = new AvailabilityRouter(redis, registry);
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
        availability: {
            async prepare(input, commandPayload, executionId, messageId) {
                return router.prepareDelivery({
                    executionId, messageId, sessionId: input.sessionId, traceId: input.traceId,
                    source: input.sourceAgentType, targetAgentType: input.targetAgentType,
                    userCode: input.userCode, region: input.region, priority: input.priority,
                    policy: input.routePolicy || RoutePolicy.FAIL_FAST,
                    timeoutMs: input.availabilityTimeoutMs, commandPayload,
                    metadata: { ...(input.metadata || {}) },
                });
            },
        },
    };
}

/** @deprecated Use `callAgent` instead. */
export const dispatchAskAgent = callAgent;
/** @deprecated Use `createRedisCallAgentDeps` instead. */
export const createRedisAskAgentDispatchDeps = createRedisCallAgentDeps;

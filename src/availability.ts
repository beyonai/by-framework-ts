import type { Redis } from 'ioredis';
import { QueueNames } from './constants';
import type { WorkerRegistry } from './registry';

export const RoutePolicy = {
    FAIL_FAST: 'FAIL_FAST',
    SEND_ANYWAY: 'SEND_ANYWAY',
    WAKE_AND_WAIT: 'WAKE_AND_WAIT',
    WAKE_AND_QUEUE: 'WAKE_AND_QUEUE',
    QUEUE_ONLY: 'QUEUE_ONLY',
} as const;
export type RoutePolicy = typeof RoutePolicy[keyof typeof RoutePolicy];

export const AvailabilityStatus = {
    DELIVER_NOW: 'DELIVER_NOW',
    WAIT_AND_DELIVER: 'WAIT_AND_DELIVER',
    QUEUE_PENDING: 'QUEUE_PENDING',
    REJECT: 'REJECT',
    FALLBACK_TO_OTHER_AGENT_TYPE: 'FALLBACK_TO_OTHER_AGENT_TYPE',
} as const;
export type AvailabilityStatus = typeof AvailabilityStatus[keyof typeof AvailabilityStatus];

export interface DeliveryIntent {
    executionId: string;
    messageId: string;
    sessionId: string;
    traceId: string;
    source: string;
    targetAgentType: string;
    userCode?: string;
    region?: string;
    priority?: number;
    policy: RoutePolicy;
    timeoutMs?: number;
    commandPayload: Record<string, unknown>;
    metadata?: Record<string, unknown>;
}

export interface AvailabilityResult {
    status: AvailabilityStatus;
    streamName?: string;
    selectedAgentType?: string;
    error?: string;
    errorCode?: string;
}

function parseJson(raw: string | null): Record<string, any> | null {
    if (!raw) return null;
    try { return JSON.parse(raw); } catch { return null; }
}

function asString(value: unknown): string | null {
    if (typeof value === 'string') return value;
    if (Buffer.isBuffer(value)) return value.toString('utf8');
    return null;
}

export class AvailabilityRouter {
    constructor(private readonly redis: Redis, private readonly registry: WorkerRegistry) {}

    async prepareDelivery(intent: DeliveryIntent): Promise<AvailabilityResult> {
        const circuit = await this.readJson(QueueNames.control_plane_agent_circuit(intent.targetAgentType));
        if (String(circuit?.state || '').toUpperCase() === 'OPEN') {
            return this.reject(String(circuit?.reason || 'agent circuit is open'), 'AGENT_CIRCUIT_OPEN');
        }
        if (intent.userCode) {
            const quota = await this.readJson(QueueNames.control_plane_user_quota(intent.userCode));
            if (quota?.available === false) {
                return this.reject(String(quota.reason || 'user quota exceeded'), 'TENANT_QUOTA_EXCEEDED');
            }
        }
        if (intent.policy === RoutePolicy.SEND_ANYWAY) return this.deliver(intent.targetAgentType);
        const [online] = await this.registry.hasOnlineAgentType(intent.targetAgentType);
        if (online) return this.deliver(intent.targetAgentType);

        const fallback = await this.readJson(QueueNames.control_plane_agent_fallback(intent.targetAgentType));
        const selected = String(fallback?.selected_agent_type || fallback?.agent_type || fallback?.target_agent_type || '');
        if (selected && (await this.registry.hasOnlineAgentType(selected))[0]) {
            return { status: AvailabilityStatus.FALLBACK_TO_OTHER_AGENT_TYPE, streamName: QueueNames.ctrl_stream(selected), selectedAgentType: selected };
        }
        switch (intent.policy) {
            case RoutePolicy.FAIL_FAST:
                return this.reject(`No alive worker found with agent type '${intent.targetAgentType}'`, 'AGENT_TYPE_UNAVAILABLE');
            case RoutePolicy.QUEUE_ONLY:
                return this.queuePending(intent);
            case RoutePolicy.WAKE_AND_QUEUE:
                await this.publishWakeup(intent);
                return this.queuePending(intent);
            case RoutePolicy.WAKE_AND_WAIT:
                return this.wakeAndWait(intent);
            default:
                return this.reject(`Unsupported offline route policy '${intent.policy}'`, 'AGENT_TYPE_UNAVAILABLE');
        }
    }

    private deliver(agentType: string): AvailabilityResult {
        return { status: AvailabilityStatus.DELIVER_NOW, streamName: QueueNames.ctrl_stream(agentType) };
    }
    private async readJson(key: string): Promise<Record<string, any> | null> {
        const get = (this.redis as any).get;
        return typeof get === 'function' ? parseJson(await get.call(this.redis, key)) : null;
    }
    private reject(error: string, errorCode: string): AvailabilityResult {
        return { status: AvailabilityStatus.REJECT, error, errorCode };
    }
    private wakeupPayload(intent: DeliveryIntent): Record<string, unknown> {
        return {
            execution_id: intent.executionId, target_agent_type: intent.targetAgentType,
            session_id: intent.sessionId, trace_id: intent.traceId, message_id: intent.messageId,
            source: intent.source, policy: intent.policy, timeout_ms: intent.timeoutMs ?? 30000,
            user_code: intent.userCode || '', region: intent.region || '', priority: intent.priority || 0,
            metadata: intent.metadata || {}, command_payload: intent.commandPayload,
        };
    }
    private async publishWakeup(intent: DeliveryIntent): Promise<void> {
        await this.redis.xadd(QueueNames.control_plane_wakeup_stream(), '*', 'data', JSON.stringify(this.wakeupPayload(intent)));
    }
    private async queuePending(intent: DeliveryIntent): Promise<AvailabilityResult> {
        const payload = {
            execution_id: intent.executionId, message_id: intent.messageId, session_id: intent.sessionId,
            trace_id: intent.traceId, target_agent_type: intent.targetAgentType,
            delivery_stream: QueueNames.ctrl_stream(intent.targetAgentType), command_payload: intent.commandPayload,
            user_code: intent.userCode || '', region: intent.region || '', priority: intent.priority || 0,
            metadata: intent.metadata || {},
        };
        const streamName = QueueNames.control_plane_delivery_pending_stream();
        await this.redis.xadd(streamName, '*', 'data', JSON.stringify(payload));
        return { status: AvailabilityStatus.QUEUE_PENDING, streamName };
    }
    private async wakeAndWait(intent: DeliveryIntent): Promise<AvailabilityResult> {
        await this.publishWakeup(intent);
        const resultStream = QueueNames.control_plane_wakeup_result_stream(intent.executionId);
        const timeout = Math.max(0, intent.timeoutMs ?? 30000);
        if (timeout === 0) return this.reject(`Timed out waiting for worker wakeup for agent_type '${intent.targetAgentType}'`, 'AGENT_TYPE_UNAVAILABLE');
        const messages = await (this.redis.xread as any)('COUNT', 1, 'BLOCK', timeout, 'STREAMS', resultStream, '0-0');
        if (!messages?.length) return this.reject(`Timed out waiting for worker wakeup for agent_type '${intent.targetAgentType}'`, 'AGENT_TYPE_UNAVAILABLE');
        const fields: unknown[] = messages[0][1][0][1];
        const dataIndex = fields.findIndex(field => asString(field) === 'data');
        const decision = parseJson(dataIndex >= 0 ? asString(fields[dataIndex + 1]) : null) || {};
        if (decision.status === 'READY' && (await this.registry.hasOnlineAgentType(intent.targetAgentType))[0]) {
            return { status: AvailabilityStatus.WAIT_AND_DELIVER, streamName: QueueNames.ctrl_stream(intent.targetAgentType) };
        }
        if (decision.status === 'FALLBACK' && decision.selected_agent_type) {
            const selectedAgentType = String(decision.selected_agent_type);
            return { status: AvailabilityStatus.FALLBACK_TO_OTHER_AGENT_TYPE, streamName: QueueNames.ctrl_stream(selectedAgentType), selectedAgentType };
        }
        return this.reject(String(decision.reason || `Wakeup rejected for agent_type '${intent.targetAgentType}'`), 'AGENT_TYPE_UNAVAILABLE');
    }
}

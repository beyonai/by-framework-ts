import type { AskAgentCommand } from '../protocol/commands';
import type { RoutePolicy } from '../availability';

/** How the host runtime may adjust local scheduling after a successful publish (not sent over Redis). */
export type AskAgentRuntimeHint = 'suspend' | 'transfer' | 'none';

export interface CallAgentPublishInput {
    readonly sessionId: string;
    readonly traceId: string;
    /** Current agent type (caller); used in header when waitForReply is true. */
    readonly sourceAgentType: string;
    /** Default parent message when `parentMessageId` is omitted (e.g. current worker message id). */
    readonly defaultParentMessageId: string;
    readonly targetAgentType: string;
    readonly content: string | ReadonlyArray<Record<string, unknown>>;
    readonly payload?: Readonly<Record<string, unknown>>;
    readonly waitForReply?: boolean;
    /** Optional user code propagated to MessageHeader.user_code. */
    readonly userCode?: string;
    /** Optional user name propagated to MessageHeader.user_name. */
    readonly userName?: string;
    /** Optional task group id propagated to MessageHeader.task_group_id. */
    readonly taskGroupId?: string;
    readonly metadata?: Readonly<Record<string, unknown>>;
    readonly messageId?: string;
    readonly parentMessageId?: string;
    readonly probeAgentType?: boolean;
    readonly routePolicy?: RoutePolicy;
    readonly availabilityTimeoutMs?: number;
    readonly region?: string;
    readonly priority?: number;
    /** Langfuse parent observation ID to nest this sub-agent under the caller's task span. */
    readonly langfuseParentObservationId?: string;
}

export interface CallAgentPublishResult {
    readonly status: string;
    readonly messageId: string;
    readonly parentMessageId?: string;
    readonly targetAgentType: string;
    readonly error?: string;
    readonly error_code?: string;
    readonly runtimeHint?: AskAgentRuntimeHint;
    readonly routeStatus?: string;
}

export interface AskAgentPublishArtifacts {
    readonly command: AskAgentCommand;
    readonly ctrlStreamName: string;
    readonly messageId: string;
    readonly parentMessageId: string;
    readonly waitForReply: boolean;
    readonly executionRecord: Readonly<Record<string, unknown>>;
}

/** When `'client'`, empty `sourceAgentType` in execution becomes `'client'` (GatewayClient). When `'none'`, keep as in header (AgentContext). */
export type ExecutionSourceAgentFallback = 'client' | 'none';

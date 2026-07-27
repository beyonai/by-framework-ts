import { AgentContext, CallAgentParams, CallAgentResult, DispatchGroupResult } from './context';
import { BaiYingMessage } from './protocol/message';

/** Typed content union accepted by Byai-aware dispatch calls. */
export type ByaiContent = string | BaiYingMessage | BaiYingMessage[];

export interface ByaiCallAgentParams extends Omit<CallAgentParams, 'content'> {
    readonly content: ByaiContent;
}

export interface ByaiAgentTask {
    readonly targetAgentType: string;
    readonly content: ByaiContent;
    readonly payload?: Readonly<Record<string, unknown>>;
    readonly metadata?: Readonly<Record<string, unknown>>;
}

export interface ByaiCallAgentsParams {
    readonly tasks: ReadonlyArray<ByaiAgentTask>;
    readonly waitForReply?: boolean;
    readonly messageId?: string;
    readonly parentMessageId?: string;
}

/**
 * AgentContext facade with Byai-specific content typing. No behavioral
 * changes over AgentContext — narrows callAgent/callAgents/dispatchGroup's
 * `content` field to ByaiContent so callers get compile-time typing for
 * decoded Byai messages, mirroring Python's ByaiAgentContext.
 */
export class ByaiAgentContext extends AgentContext {
    async callAgent(params: ByaiCallAgentParams): Promise<CallAgentResult> {
        return super.callAgent(params as CallAgentParams);
    }

    async callAgents(params: ByaiCallAgentsParams): Promise<DispatchGroupResult> {
        return super.callAgents(params as unknown as Parameters<AgentContext['callAgents']>[0]);
    }

    async dispatchGroup(params: ByaiCallAgentsParams): Promise<DispatchGroupResult> {
        return this.callAgents(params);
    }
}

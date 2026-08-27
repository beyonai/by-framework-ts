export interface AgentCapabilityProbe {
    /** Returns whether at least one worker for the agent type is online (lease-based). */
    probeAgentTypeOnline(agentType: string): Promise<{ readonly ok: boolean; readonly error_code?: string; readonly error?: string }>;
}

export interface ExecutionInitializer {
    init(execution: Record<string, unknown>): Promise<void>;
}

export interface CommandBus {
    publish(streamName: string, serializedCommandJson: string): Promise<void>;
}

export interface AskAgentQueueNames {
    ctrl_stream(agentType: string): string;
}

/**
 * Records "the caller is now suspended waiting for this sub-task's reply" in
 * the wait index. A port rather than a direct Redis call so the publish
 * pipeline stays free of I/O details; `createRedisCallAgentDeps` wires the real
 * implementation (`src/liveness/wait_registration.ts`).
 */
export interface WaitIndexRegistrar {
    register(params: {
        readonly sessionId: string;
        readonly parentMessageId: string;
        readonly childMessageId: string;
        readonly taskGroupId?: string;
        readonly timeoutMs: number;
    }): Promise<void>;
}

export interface AskAgentDispatchDeps {
    readonly probe: AgentCapabilityProbe;
    readonly execution: ExecutionInitializer;
    readonly bus: CommandBus;
    readonly queueNames: AskAgentQueueNames;
    /**
     * Optional: deps built by hand (tests, embedders) may omit it, which only
     * costs the liveness safety net — never correctness of the dispatch.
     */
    readonly waitIndex?: WaitIndexRegistrar;
    readonly availability?: {
        prepare(input: import('./types').CallAgentPublishInput, commandPayload: Record<string, unknown>, executionId: string, messageId: string): Promise<import('../availability').AvailabilityResult>;
    };
}

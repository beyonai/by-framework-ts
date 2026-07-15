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

export interface AskAgentDispatchDeps {
    readonly probe: AgentCapabilityProbe;
    readonly execution: ExecutionInitializer;
    readonly bus: CommandBus;
    readonly queueNames: AskAgentQueueNames;
    readonly availability?: {
        prepare(input: import('./types').CallAgentPublishInput, commandPayload: Record<string, unknown>, executionId: string, messageId: string): Promise<import('../availability').AvailabilityResult>;
    };
}

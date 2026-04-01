export enum AgentState {
    STARTING = "STARTING",
    CANCELLING = "CANCELLING",
    CANCELLED = "CANCELLED",
    COMPLETED = "COMPLETED",
    FAILED = "FAILED",
    RESUMED = "RESUMED",
    WAITING_AGENT = "WAITING_AGENT",
    WAITING_USER = "WAITING_USER",
    QUEUED = "QUEUED",
    CALLING_AGENT = "CALLING_AGENT",
}

/** Terminal states - states that represent a completed execution */
export const TERMINAL_STATES: ReadonlySet<string> = new Set([
    AgentState.COMPLETED,
    AgentState.FAILED,
    AgentState.CANCELLED,
]);

/**
 * Check if a given state is a terminal state.
 *
 * @param state - The state to check
 * @returns True if the state is terminal, false otherwise
 */
export function isTerminalState(state: string): boolean {
    return TERMINAL_STATES.has(state);
}

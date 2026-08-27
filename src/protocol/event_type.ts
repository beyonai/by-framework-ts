export enum EventType {
    // 兼容原生 SseResponseEvent
    ANSWER_DELTA = "answerDelta",
    REASONING_LOG_DELTA = "reasoningLogDelta",
    REASONING_LOG_START = "reasoningLogStart",
    REASONING_LOG_END = "reasoningLogEnd",
    APP_STREAM_RESPONSE = "appStreamResponse",
    FINAL_ANSWER = "finalAnswer",
    TASK_CREATE = "taskCreate",
    STEP_COMPLETE = "stepComplete",
    TASK_STOP = "taskStop",
    /**
     * A reply that arrived for an already-resolved wait and was therefore
     * dropped by the idempotency gate (see src/liveness/wait_gate.ts).
     *
     * Diagnostic only — the sub-agent did real work whose result nobody will
     * now consume, possibly with side effects, so the drop must not be silent.
     * Cross-SDK wire value, mirrored from by-framework-python EventType.
     */
    ORPHANED_REPLY = "orphanedReply",
}

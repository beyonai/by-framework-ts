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
}

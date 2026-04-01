export interface StreamDataDelta {
    content: string;
}

export interface StreamDataChoice {
    delta: StreamDataDelta;
}

export interface StreamDataPayload {
    contentType: string;
    choices: StreamDataChoice[];
}

export interface DataMessage {
    trace_id: string;
    session_id: string;
    event_type: string;
    source_agent_id?: string;
    message_id?: string;
    parent_message_id?: string;
    timestamp?: number;
    data?: Record<string, any> | StreamDataPayload;
    state_msg?: string;
    artifact_url?: string;
    metadata?: Record<string, any>;
}

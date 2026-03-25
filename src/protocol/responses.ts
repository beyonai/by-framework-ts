export interface SendMessageResponse {
    success: boolean;
    message_id: string;
    trace_id: string;
    target_worker_id: string;
    timestamp: number;
    status: string;
}

export interface CancelTaskResponse {
    success: boolean;
    message_id: string;
    execution_id: string;
    worker_id: string;
    status: string;
    timestamp: number;
    error?: string;
}

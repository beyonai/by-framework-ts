/**
 * Response type definitions for Gateway protocol.
 *
 * Contains response interfaces and TypedDict definitions for
 * send message and cancel task operations.
 */

export interface SendMessageResponse {
    success: boolean;
    message_id: string;
    trace_id: string;
    target_worker_id: string;
    timestamp: number;
    status: string;
    error?: string;
    error_code?: string;
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

/**
 * API response status values for execution operations.
 */
export class ExecutionStatus {
    // Success statuses
    static readonly SUCCESS = 'SUCCESS';
    static readonly QUEUED = 'QUEUED';
    static readonly CANCEL_REQUESTED = 'CANCEL_REQUESTED';

    // Error statuses
    static readonly NOT_FOUND = 'NOT_FOUND';
    static readonly ALREADY_FINISHED = 'ALREADY_FINISHED';
    static readonly FAILED = 'FAILED';
    static readonly SESSION_MISMATCH = 'SESSION_MISMATCH';

    // Failure error codes for SendMessageResponse
    static readonly ERR_AGENT_TYPE_NOT_FOUND = 'AGENT_TYPE_NOT_FOUND';
    static readonly ERR_WORKER_NOT_ALIVE = 'WORKER_NOT_ALIVE';
    static readonly ERR_REGISTRY_NOT_SET = 'REGISTRY_NOT_SET';
}

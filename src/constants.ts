/**
 * Gateway SDK Redis Key Constants
 *
 * All Redis Stream names, Hash keys, Set keys, and other configuration items
 * are centralized in this file. Hardcoded string literals are prohibited
 * in business code.
 */

export class QueueNames {
  /**
   * Control stream queue for dispatching tasks to workers with specific capabilities.
   */
  static ctrl_stream(capability: string): string {
    return `byai_gateway:ctrl:capability:${capability}`;
  }

  /**
   * Worker-specific control queue for directed control commands.
   */
  static worker_ctrl_stream(workerId: string): string {
    return `byai_gateway:ctrl:worker:${workerId}`;
  }

  /**
   * Session-level data stream. Workers push streaming content here.
   */
  static session_data_stream(sessionId: string): string {
    return `byai_gateway:session:${sessionId}:data_stream`;
  }

  /**
   * Task group progress tracking hash key.
   */
  static task_group(groupId: string): string {
    return `byai_gateway:task_group:${groupId}`;
  }

  /**
   * Task group results hash key.
   */
  static task_group_results(groupId: string): string {
    return `byai_gateway:task_group:${groupId}:results`;
  }
}

export class RegistryKeys {
  /** Default TTL (7 days) for session-related aggregation keys */
  static DEFAULT_SESSION_TTL = 7 * 24 * 3600;

  /** Active workers sorted set (sorted by heartbeat timestamp) */
  static ACTIVE_WORKERS = 'byai_gateway:registry:active_workers';

  /**
   * Worker capabilities set - stores all capability identifiers supported by a worker.
   */
  static worker_capabilities(workerId: string): string {
    return `byai_gateway:registry:worker:capabilities:${workerId}`;
  }

  /**
   * Capability workers set - stores all worker IDs with a specific capability.
   */
  static capability_workers(capability: string): string {
    return `byai_gateway:registry:capability:workers:${capability}`;
  }

  /**
   * Worker startup mutex lock to prevent duplicate worker_id concurrent startup.
   */
  static worker_lock(workerId: string): string {
    return `byai_gateway:registry:worker:lock:${workerId}`;
  }

  /**
   * Session-level aggregation registry (Hash).
   *
   * Internal fields:
   * - exec:{execution_id} -> Execution details JSON
   * - msg_map:{message_id} -> Message ID to execution ID mapping
   */
  static session_registry(sessionId: string): string {
    return `byai_gateway:session:${sessionId}:registry`;
  }

  /**
   * Execution details by execution ID.
   */
  static execution_detail(executionId: string): string {
    return `byai_gateway:registry:execution:detail:${executionId}`;
  }

  /**
   * Execution mapping by message ID.
   */
  static execution_by_message(messageId: string): string {
    return `byai_gateway:registry:execution:message:${messageId}`;
  }

  /**
   * Session executions set.
   */
  static session_executions(sessionId: string): string {
    return `byai_gateway:registry:session:executions:${sessionId}`;
  }
}

export class ConsumerGroups {
  /** Gateway Worker control stream consumer group */
  static AGENT_ENGINES = 'byai_gateway:consumer_group:agent_engines';
}

// --- ID Prefix Constants ---
// Used for generating unique IDs, avoiding hardcoded literals in business code
export const MESSAGE_ID_PREFIX = 'msg-';
export const EXECUTION_ID_PREFIX = 'exec-';
export const TASK_GROUP_ID_PREFIX = 'tg-';
export const CANCEL_MESSAGE_ID_PREFIX = 'msg-cancel-';

// --- Redis Hash Field Prefixes ---
// Session Registry hash field prefixes
export const EXEC_FIELD_PREFIX = 'exec:';
export const MSG_MAP_PREFIX = 'msg_map:';

// --- Task Group Hash Fields ---
export const TASK_GROUP_FIELD_TOTAL = 'total';
export const TASK_GROUP_FIELD_COMPLETED = 'completed';
export const TASK_GROUP_FIELD_SOURCE_AGENT = 'source_agent_type';

// --- Timing Constants ---
/** Control loop sleep interval (seconds) */
export const CONTROL_LOOP_SLEEP_SECONDS = 0.01;
/** Wait for tasks completion timeout (seconds) */
export const WAIT_FOR_TASKS_TIMEOUT_SECONDS = 5.0;
/** Task group key TTL (seconds), default 1 day */
export const TASK_GROUP_TTL_SECONDS = 86400;
/** First retry wait time (seconds) */
export const FIRST_RETRY_WAIT_SECONDS = 1.0;
/** Maximum retry count */
export const MAX_RETRY_COUNT = 3;

// --- Filesystem Constants ---
export const DEFAULT_WORKSPACE_DIR = 'workspace';

// --- Stream Read Markers ---
/** Redis XREAD/XREADGROUP marker for reading only new messages */
export const STREAM_READ_LAST_ID = '>';

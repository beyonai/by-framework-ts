/**
 * Gateway SDK Redis Key Constants
 *
 * All Redis Stream names, Hash keys, Set keys, and other configuration items
 * are centralized in this file. Hardcoded string literals are prohibited
 * in business code.
 */

export type KeySchemaVersion = 'v1' | 'v2';

/**
 * Controlled by REDIS_KEY_SCHEMA_VERSION, which always wins when set
 * explicitly. When it isn't set, REDIS_CLUSTER_HOST being configured
 * implies 'v2' (Cluster mode requires v2 - v1 keys have no hash tags and
 * hit CROSSSLOT errors under Cluster; see redis_client.createRedis's
 * fail-fast check); otherwise it defaults to 'v1' (the legacy unprefixed
 * key format). This mirrors redis_client.ts's REDIS_MODE/REDIS_CLUSTER_HOST
 * precedence, but deliberately does NOT infer 'v2' from an explicit
 * REDIS_MODE=cluster alone (without REDIS_CLUSTER_HOST) - that legacy
 * explicit-mode path still requires REDIS_KEY_SCHEMA_VERSION=v2 to be set
 * by hand.
 */
export function getKeySchemaVersion(): KeySchemaVersion {
  const version =
    process.env.REDIS_KEY_SCHEMA_VERSION || (process.env.REDIS_CLUSTER_HOST ? 'v2' : 'v1');
  if (version !== 'v1' && version !== 'v2') {
    throw new Error(`Invalid REDIS_KEY_SCHEMA_VERSION: '${version}' (must be 'v1' or 'v2')`);
  }
  return version;
}

const V2_PREFIX = 'byai_gateway:v2:';

/**
 * Resolve a key according to REDIS_KEY_SCHEMA_VERSION.
 *
 * v1 (default): returns v1Key unchanged, byte-for-byte.
 * v2: returns V2_PREFIX + v2Suffix, where v2Suffix already encodes any
 * Cluster hash tag needed for same-entity key groups.
 *
 * Every QueueNames/RegistryKeys factory method routes through this one
 * function so the v1/v2 decision lives in exactly one place.
 */
function versioned(v1Key: string, v2Suffix: string): string {
  if (getKeySchemaVersion() === 'v2') {
    return `${V2_PREFIX}${v2Suffix}`;
  }
  return v1Key;
}

/**
 * SCAN MATCH glob pattern matching every worker key in this family.
 *
 * Under v1 the worker_id is the last path segment (prefix + id, no suffix).
 * Under v2 it's wrapped in a Cluster hash tag in the middle of the key
 * (prefix + "{" + id + "}" + suffix) — a bare "{prefix}*" pattern would
 * never match a real v2 key, since "{"/"}" are literal characters in
 * Redis's glob matching (only *, ?, [seq] are special), not wildcards.
 */
function workerScanPattern(v1Prefix: string, v2Field: string): string {
  if (getKeySchemaVersion() === 'v2') {
    return `${V2_PREFIX}registry:worker:{*}:${v2Field}`;
  }
  return `${v1Prefix}*`;
}

/** Extract worker_id from a key returned by scanning with workerScanPattern(v1Prefix, v2Field). */
function workerIdFromScannedKey(key: string, v1Prefix: string, v2Field: string): string | null {
  if (getKeySchemaVersion() === 'v2') {
    const prefix = `${V2_PREFIX}registry:worker:{`;
    const suffix = `}:${v2Field}`;
    if (key.startsWith(prefix) && key.endsWith(suffix)) {
      return key.slice(prefix.length, key.length - suffix.length);
    }
    return null;
  }
  if (key.startsWith(v1Prefix)) {
    return key.slice(v1Prefix.length);
  }
  return null;
}

export class QueueNames {
  /**
   * Control stream queue for dispatching tasks to workers with specific agent types.
   * Single-key: agent_type is the only variable dimension, so no hash tag.
   */
  static ctrl_stream(agentType: string): string {
    return versioned(`byai_gateway:ctrl:agent_type:${agentType}`, `ctrl:agent_type:${agentType}`);
  }

  /**
   * Worker-specific control queue for directed control commands.
   * Same-entity with the other worker:{worker_id} keys in RegistryKeys.
   */
  static worker_ctrl_stream(workerId: string): string {
    return versioned(`byai_gateway:ctrl:worker:${workerId}`, `ctrl:worker:{${workerId}}`);
  }

  static control_plane_wakeup_stream(): string {
    return versioned('byai_gateway:control_plane:mgmt:wakeup', 'control_plane:mgmt:wakeup');
  }

  static control_plane_wakeup_result_stream(executionId: string): string {
    return versioned(
      `byai_gateway:control_plane:mgmt:wakeup:result:${executionId}`,
      `control_plane:mgmt:wakeup:result:${executionId}`
    );
  }

  static control_plane_delivery_pending_stream(): string {
    return versioned(
      'byai_gateway:control_plane:mgmt:delivery:pending',
      'control_plane:mgmt:delivery:pending'
    );
  }

  static control_plane_agent_circuit(agentType: string): string {
    return versioned(
      `byai_gateway:control_plane:circuit:agent_type:${agentType}`,
      `control_plane:circuit:agent_type:${agentType}`
    );
  }

  static control_plane_agent_fallback(agentType: string): string {
    return versioned(
      `byai_gateway:control_plane:fallback:agent_type:${agentType}`,
      `control_plane:fallback:agent_type:${agentType}`
    );
  }

  static control_plane_user_quota(userCode: string): string {
    return versioned(
      `byai_gateway:control_plane:quota:user:${userCode}`,
      `control_plane:quota:user:${userCode}`
    );
  }

  /**
   * Session-level data stream. Workers push streaming content here.
   * Same-entity with RegistryKeys.session_registry.
   */
  static session_data_stream(sessionId: string): string {
    return versioned(
      `byai_gateway:session:${sessionId}:data_stream`,
      `session:{${sessionId}}:data_stream`
    );
  }

  /**
   * Task group progress tracking hash key.
   */
  static task_group(groupId: string): string {
    return versioned(`byai_gateway:task_group:${groupId}`, `task_group:{${groupId}}`);
  }

  /**
   * Task group results hash key.
   */
  static task_group_results(groupId: string): string {
    return versioned(`byai_gateway:task_group:${groupId}:results`, `task_group:{${groupId}}:results`);
  }

  // --- Trace observability keys ---

  /**
   * Trace-level metadata hash (start_ts, status, session_id, …).
   *
   * v1 keeps TS's historical byai_gateway:trace:*:meta namespace. v2 unifies
   * onto the shared byai_gateway:v2:trace:{id} format used by all three
   * language SDKs (Python/Java previously shared by_framework:trace:*, TS
   * used a different byai_gateway:trace:* layout — v2 replaces both).
   */
  static trace_meta(traceId: string): string {
    return versioned(`byai_gateway:trace:${traceId}:meta`, `trace:{${traceId}}`);
  }

  /** Ordered list of serialised span JSON entries for a trace. */
  static trace_spans(traceId: string): string {
    return versioned(`byai_gateway:trace:${traceId}:spans`, `trace:spans:{${traceId}}`);
  }

  /**
   * Sorted set index: session → trace IDs (score = start_ts).
   * Cross-entity relative to the trace group (meta/spans) — deliberately untagged.
   */
  static trace_index_session(sessionId: string): string {
    return versioned(`byai_gateway:trace:idx:session:${sessionId}`, `trace:idx:session:${sessionId}`);
  }

  /** Sorted set index: worker → trace IDs (score = start_ts). Cross-entity, untagged. */
  static trace_index_worker(workerId: string): string {
    return versioned(`byai_gateway:trace:idx:worker:${workerId}`, `trace:idx:worker:${workerId}`);
  }

  /** Sorted set index: agent type → trace IDs (score = start_ts). Cross-entity, untagged. */
  static trace_index_agent(agentType: string): string {
    return versioned(`byai_gateway:trace:idx:agent:${agentType}`, `trace:idx:agent:${agentType}`);
  }
}

export class RegistryKeys {
  /** Default TTL (7 days) for session-related aggregation keys */
  static DEFAULT_SESSION_TTL = 7 * 24 * 3600;

  /** Known workers set used for registry enumeration. Global index, untagged. */
  static known_workers(): string {
    return versioned('byai_gateway:registry:workers', 'registry:workers');
  }

  /** Default health check threshold (30 seconds) in milliseconds */
  static SD_DEFAULT_HEALTH_THRESHOLD_MS = 30000;

  // --- 服务发现 (Service Discovery) ---
  /** Active service instances sorted set (sorted by heartbeat timestamp) */
  static sd_active_instances(serviceName: string): string {
    return versioned(`byai_gateway:sd:active:${serviceName}`, `sd:{${serviceName}}:active`);
  }

  /** Service instance details hash key */
  static sd_instance_details(serviceName: string): string {
    return versioned(`byai_gateway:sd:instances:${serviceName}`, `sd:{${serviceName}}:instances`);
  }

  /** All known service names set. Global index, untagged. */
  static sd_services(): string {
    return versioned('byai_gateway:sd:services', 'sd:services');
  }

  /** Default heartbeat interval (10 seconds) */
  static SD_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 10;

  /** Worker default heartbeat interval (seconds) */
  static WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 5;

  /** Worker online lease TTL (seconds) */
  static WORKER_DEFAULT_LEASE_TTL_SECONDS = 15;

  /**
   * Worker online lease key. Value stores presence token and last_seen.
   */
  static worker_online_lease(workerId: string): string {
    return versioned(`byai_gateway:registry:worker:online:${workerId}`, `registry:worker:{${workerId}}:online`);
  }

  /** SCAN MATCH glob pattern matching every worker_online_lease key. */
  static worker_online_lease_scan_pattern(): string {
    return workerScanPattern('byai_gateway:registry:worker:online:', 'online');
  }

  /** Extract worker_id from a key found via worker_online_lease_scan_pattern(). */
  static worker_id_from_online_lease_key(key: string): string | null {
    return workerIdFromScannedKey(key, 'byai_gateway:registry:worker:online:', 'online');
  }

  /**
   * Worker declared agent types set - stores all agent type identifiers supported by a worker.
   */
  static workerDeclaredAgentTypes(workerId: string): string {
    return versioned(
      `byai_gateway:registry:worker:agent_types:${workerId}`,
      `registry:worker:{${workerId}}:agent_types`
    );
  }

  /**
   * Agent type members set - stores all worker IDs with a specific agent type.
   * Mandatory shared tag with agentTypeDenied: denyWorkerForType writes both together.
   */
  static agentTypeMembers(agentType: string): string {
    return versioned(
      `byai_gateway:registry:agent_type:workers:${agentType}`,
      `registry:agent_type:{${agentType}}:workers`
    );
  }

  /**
   * Worker admin state HASH (fields: lifecycle, reason, updated_at).
   * Written by WorkerManager; read by the worker on heartbeat and startup.
   * No TTL — persists until explicitly cleared by an admin action.
   */
  static workerAdminState(workerId: string): string {
    return versioned(`byai_gateway:registry:worker:admin:${workerId}`, `registry:worker:{${workerId}}:admin`);
  }

  /**
   * SET of worker_ids explicitly denied from consuming an agent_type stream.
   * Written by WorkerManager; checked by workers before XREADGROUP.
   * Mandatory shared tag with agentTypeMembers: denyWorkerForType writes both together.
   */
  static agentTypeDenied(agentType: string): string {
    return versioned(
      `byai_gateway:registry:agent_type:denied:${agentType}`,
      `registry:agent_type:{${agentType}}:denied`
    );
  }

  /**
   * Worker startup mutex lock to prevent duplicate worker_id concurrent startup.
   */
  static worker_lock(workerId: string): string {
    return versioned(`byai_gateway:registry:worker:lock:${workerId}`, `registry:worker:{${workerId}}:lock`);
  }

  /**
   * Session-level aggregation registry (Hash).
   *
   * Internal fields:
   * - exec:{execution_id} -> Execution details JSON
   * - msg_map:{message_id} -> Message ID to execution ID mapping
   */
  static session_registry(sessionId: string): string {
    return versioned(`byai_gateway:session:${sessionId}:registry`, `session:{${sessionId}}:registry`);
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

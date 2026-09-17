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

/**
 * Read a positive number from an env var, falling back to `fallback` when it is
 * unset, unparseable, or non-positive.
 *
 * Silently ignoring a bad value here is deliberate: these feed timers, and a
 * NaN interval fails in a far more confusing way than a default one. Config
 * that is parseable but incoherent (interval too close to the TTL) is caught by
 * assertHeartbeatTiming() at startup instead.
 */
function positiveNumberFromEnv(raw: string | undefined, fallback: number): number {
  if (raw === undefined || raw === '') return fallback;
  const parsed = Number(raw);
  if (!Number.isFinite(parsed) || parsed <= 0) return fallback;
  return parsed;
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

  /**
   * Worker default heartbeat interval (seconds).
   *
   * Resolved per read rather than at module load so a deployment can set
   * `BYAI_WORKER_HEARTBEAT_INTERVAL_SECONDS` without depending on import order.
   * An unusable value falls back to the default rather than propagating NaN
   * into a timer; `assertHeartbeatTiming()` is where a misconfiguration is
   * reported.
   */
  static get WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS(): number {
    return positiveNumberFromEnv(process.env.BYAI_WORKER_HEARTBEAT_INTERVAL_SECONDS, 5);
  }

  /** Worker online lease TTL (seconds). See the interval getter above. */
  static get WORKER_DEFAULT_LEASE_TTL_SECONDS(): number {
    return positiveNumberFromEnv(process.env.BYAI_WORKER_LEASE_TTL_SECONDS, 15);
  }

  /**
   * Upper bound for an inline wait — one that blocks inside task processing and
   * holds a runner in-flight slot (today: AvailabilityRouter's wakeup wait).
   *
   * A slot held for the caller's full availability timeout is a slot not
   * consuming, and a saturated runner is what makes a busy worker look stalled.
   * Derived from the lease TTL rather than written as a literal so the two
   * cannot drift.
   */
  static get WORKER_MAX_INLINE_WAIT_MS(): number {
    return (RegistryKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS * 1000) / 3;
  }

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

  // --- Suspended-caller liveness (wait index) ---
  // Cross-SDK wire contract: the four keys below must be spelled identically
  // in Python/TS/Java, because any SDK's sweeper may resolve another SDK's
  // entry. See by-framework-python common/constants.py RedisKeys.wait_*.

  /**
   * ZSET index of suspended callers waiting for a sub-task reply.
   *
   * member = encoded wait-index member (see src/liveness/wait_index.ts),
   * score = deadline in epoch milliseconds. Sharded so sweepers can claim
   * disjoint slices without a global lock; the shard is derived from
   * session_id (see waitIndexShard()).
   *
   * Cross-entity index (spans every session), so deliberately left untagged
   * relative to the per-session keys it points at — same rule as
   * trace_index_session / known_workers.
   */
  static wait_index(shard: number): string {
    return versioned(`byai_gateway:wait:index:${shard}`, `wait:index:${shard}`);
  }

  /**
   * Short-lived claim on one wait_index() shard, held while sweeping it.
   *
   * Ownership is advisory: it only keeps two sweepers from doing the same
   * triage at the same moment. Losing it (expiry, a partitioned worker)
   * cannot corrupt anything, because every action a sweep takes is
   * idempotent — a duplicate synthesized reply is caught by the same
   * wait-index gate that catches a duplicate real one. That is why the shards
   * need no leader election.
   *
   * Cross-entity like the shard it guards, so deliberately untagged.
   */
  static wait_sweep_lock(shard: number): string {
    return versioned(`byai_gateway:wait:sweep_lock:${shard}`, `wait:sweep_lock:${shard}`);
  }

  /**
   * Short-lived marker: "this wait-index entry was already resolved".
   *
   * Written by the idempotency gate right after it wins the ZREM for a member,
   * and read when a later ZREM for the same member returns 0. It is the *only*
   * thing that distinguishes the two meanings of that 0 — "someone already
   * consumed this wait" (drop the duplicate) from "this wait was never
   * registered" (a pre-upgrade or expired entry, which must be let through).
   * Without it, every rolling upgrade would silently drop in-flight replies.
   *
   * Per-session entity, so hash-tagged with the session in v2.
   */
  static wait_consumed(sessionId: string, memberDigest: string): string {
    return versioned(
      `byai_gateway:wait:consumed:${sessionId}:${memberDigest}`,
      `wait:consumed:{${sessionId}}:${memberDigest}`
    );
  }

  /**
   * The deadline a wait's renewal budget is measured from.
   *
   * Written once (SET NX) by the first sweep that finds the entry due, so it
   * holds the wait's *original* deadline even after renewals have overwritten
   * the ZSET score. Without it a renewal budget cannot exist at all: every
   * sweep would re-measure from the score it just pushed out, and a callee
   * whose worker is alive but making no progress would be renewed forever.
   *
   * Sweeper-private: nothing on the reply path reads or writes it, so it is
   * deliberately NOT part of the wait-index member (which must stay
   * rebuildable from a reply alone — see src/liveness/wait_index.ts).
   *
   * Per-session entity, so hash-tagged with the session in v2.
   */
  static wait_renew_origin(sessionId: string, memberDigest: string): string {
    return versioned(
      `byai_gateway:wait:renew_origin:${sessionId}:${memberDigest}`,
      `wait:renew_origin:{${sessionId}}:${memberDigest}`
    );
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
/**
 * A single callAgent (non-group) dispatch stores its result in the same
 * task_group_results Hash a real group uses, under a group id derived from the
 * sub-task's own message_id — i.e. a group of size 1. Keeps one result
 * storage/recovery path instead of two.
 *
 * Cross-SDK wire contract: Python/TS/Java must use this exact prefix, since a
 * sweeper in any of them recovers a lost reply from this storage.
 */
export const TASK_GROUP_SINGLE_ID_PREFIX = 'tg-single-';
export const CANCEL_MESSAGE_ID_PREFIX = 'msg-cancel-';

/** Group id under which a single (non-group) callAgent result is stored. */
export function singleCallTaskGroupId(childMessageId: string): string {
  return `${TASK_GROUP_SINGLE_ID_PREFIX}${childMessageId}`;
}

/**
 * Sentinel GatewayClient writes as an execution record's source_agent_type for
 * a dispatch it made itself. It is NOT an agent type: nothing declares it, so
 * nothing consumes QueueNames.ctrl_stream(CLIENT_SOURCE_AGENT_TYPE).
 *
 * Load-bearing wherever a resumed execution recovers its caller from its own
 * record instead of from the resume header: a root execution's record carries
 * this, and treating it as a caller both posts the result to a stream no one
 * reads and suppresses the end-of-stream event the session data plane owes the
 * user.
 *
 * Cross-SDK: Python writes the same literal; Java writes no field at all, so a
 * missing field must be treated as "no caller" too.
 */
export const CLIENT_SOURCE_AGENT_TYPE = 'client';

// --- Redis Hash Field Prefixes ---
// Session Registry hash field prefixes
export const EXEC_FIELD_PREFIX = 'exec:';
export const MSG_MAP_PREFIX = 'msg_map:';

// --- Task Group Hash Fields ---
export const TASK_GROUP_FIELD_TOTAL = 'total';
export const TASK_GROUP_FIELD_COMPLETED = 'completed';
export const TASK_GROUP_FIELD_SOURCE_AGENT = 'source_agent_type';
/**
 * Set when a group's fan-out threw partway through, so the caller was failed
 * with siblings already dispatched. Their replies must then be discarded rather
 * than joined — the execution they would resume is gone — and a sweep must
 * clean an orphaned member up instead of compensating it.
 *
 * Cross-runtime: the task_group hash is read by Python/TS/Java alike, so this
 * field must be honoured even by an SDK whose own dispatcher never writes it.
 */
export const TASK_GROUP_FIELD_ABORTED = 'aborted';

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

// --- Suspended-caller liveness (wait index) ---
// Values below are a cross-SDK contract (see
// .trellis/tasks/08-24-suspended-parent-liveness/research/cross-sdk-wire-contract.md
// §6) and are mirrored byte-for-byte from by-framework-python
// common/constants.py. Do not retune one SDK in isolation.

/**
 * Number of RegistryKeys.wait_index() shards. Fixed: changing it re-maps every
 * session to a different shard, so in-flight entries would be swept by no one.
 * Treat as a cross-SDK protocol constant, not a tunable.
 */
export const WAIT_INDEX_SHARDS = 16;
/**
 * Default deadline for a callAgent(waitForReply=true) reply (1 hour).
 * Machine waiting on machine.
 */
export const DEFAULT_REPLY_TIMEOUT_MS = 3_600_000;
/**
 * Default deadline for an askUser reply. Machine waiting on a human, so it is
 * deliberately decoupled from DEFAULT_REPLY_TIMEOUT_MS and aligned with the
 * session TTL (which is in seconds) instead.
 */
export const DEFAULT_ASK_USER_TIMEOUT_MS = RegistryKeys.DEFAULT_SESSION_TTL * 1000;
/** How often a worker's sweeper scans the shards it owns (seconds). */
export const WAIT_SWEEP_INTERVAL_SECONDS = 30;
/**
 * TTL of a RegistryKeys.wait_sweep_lock() claim. Must comfortably exceed one
 * shard's sweep so the owner doesn't lose the shard mid-pass, and stay short
 * enough that a crashed sweeper's shards are picked up again quickly.
 */
export const WAIT_SWEEP_LOCK_TTL_SECONDS = 60;
/**
 * Most due entries one sweep resolves per shard per cycle. Bounds the work of a
 * single pass after an outage leaves a large backlog; the remainder is picked
 * up next cycle, since entries stay in the index until a reply clears them.
 */
export const WAIT_SWEEP_BATCH_LIMIT = 200;
/**
 * Fixed extension applied when a sweep finds the callee still making progress.
 * Deliberately a constant rather than the original timeout: the wait-index
 * member must stay reconstructible from a reply alone, so it cannot carry the
 * caller's original timeout.
 */
export const WAIT_RENEW_INCREMENT_MS = 300_000;
/**
 * Hard ceiling on renewals, as a multiple of the caller's own timeout: a wait
 * may be renewed until `registered_at + N * timeout`, after which the callee is
 * declared CHILD_TIMEOUT even though its worker is still alive. Without a
 * ceiling, a callee that is running but making no progress suspends its caller
 * forever — the one failure mode the deadline was supposed to bound.
 */
export const WAIT_RENEW_MAX_MULTIPLE = 3;
/**
 * TTL of RegistryKeys.wait_renew_origin(). Must comfortably exceed the largest
 * budget in use (N * timeout), or the budget silently restarts mid-wait.
 */
export const WAIT_RENEW_ORIGIN_TTL_SECONDS = TASK_GROUP_TTL_SECONDS;
/**
 * error_code for a reply whose task group tracker no longer exists.
 *
 * Distinct from the LivenessErrorCode family: those describe a sub-task that
 * went wrong, this one describes the group's bookkeeping outliving its TTL
 * while replies were still arriving.
 */
export const TASK_GROUP_EXPIRED = 'TASK_GROUP_EXPIRED';

// --- Orphaned Message Reclaim Constants ---
/**
 * Lease TTLs a message must sit unacknowledged before its owner counts as
 * dead. Well past the point where a live worker would have renewed, so the
 * liveness check that follows reads a settled state rather than a race.
 */
export const ORPHAN_RECLAIM_LEASE_MULTIPLE = 4;
/** Most pending entries inspected per stream per sweep. */
export const ORPHAN_RECLAIM_BATCH = 50;
/** Idle polls between orphan sweeps. Reclaim is rare; polling is not. */
export const ORPHAN_RECLAIM_EVERY_N_POLLS = 20;
/**
 * How long RegistryKeys.wait_consumed() remembers that a wait was already
 * resolved, i.e. how far apart two copies of the same reply may be and still be
 * recognized as duplicates.
 *
 * Sized off DEFAULT_SESSION_TTL because that is the lifetime of the session
 * registry, which is what keeps a wait entry relevant. A marker that expires
 * while entries of that session are still live leaves two holes, the second
 * being the dangerous one: a stale duplicate sub-agent reply, having lost the
 * marker that would stop it at its own candidate, falls through to the askUser
 * candidate for the same caller and claims a wait that is still live — after
 * which the real answer is dropped as "already consumed".
 */
export const WAIT_CONSUMED_TTL_SECONDS = RegistryKeys.DEFAULT_SESSION_TTL;
/**
 * How often a sweeper prunes entries that are provably beyond use. Deliberately
 * far coarser than WAIT_SWEEP_INTERVAL_SECONDS: this is garbage collection on a
 * multi-day horizon, and it is the only work a sweeper does when compensation
 * is off.
 */
export const WAIT_PRUNE_INTERVAL_SECONDS = 3600;
/**
 * How far in the past a wait entry's score must lie before pruning it.
 *
 * Every writer sets an entry's score to its own `now` plus a non-negative
 * offset, and only while the caller's execution record exists. So
 * `now - score > this` proves the session registry a sweep would interrogate
 * has expired and no triage is possible any more — pruning is therefore not a
 * decision, which is why it needs no opt-in. The margin over
 * DEFAULT_SESSION_TTL is what makes that strict rather than coincident:
 * DEFAULT_ASK_USER_TIMEOUT_MS *equals* the session TTL, so a threshold trimmed
 * to it exactly would land on the boundary of a live askUser wait and lose to
 * any clock skew between the worker that registered the entry and the one
 * sweeping it.
 */
export const WAIT_PRUNE_AFTER_SECONDS = RegistryKeys.DEFAULT_SESSION_TTL + 86400;

/**
 * error_code values carried by synthesized/recovered resume replies.
 *
 * Cross-SDK wire contract — Python/TS/Java must emit the same strings; callers
 * match on them. Append only, never rename.
 */
export enum LivenessErrorCode {
  /** The callee's worker lease expired while its execution was non-terminal. */
  CHILD_WORKER_LOST = 'CHILD_WORKER_LOST',
  /** The callee was alive but produced no reply before the deadline. */
  CHILD_TIMEOUT = 'CHILD_TIMEOUT',
  /** The dispatch was never picked up by any worker. */
  CHILD_NEVER_STARTED = 'CHILD_NEVER_STARTED',
  /**
   * The callee finished and its result was persisted, but the reply message was
   * lost; the result was recovered from storage.
   */
  REPLY_LOST_RECOVERED = 'REPLY_LOST_RECOVERED',
}

/**
 * Renewals that must fit inside one lease TTL.
 *
 * At 3, two consecutive renewals can fail before the lease expires. At 1 the
 * first blip drops the worker out of routing, which is exactly the class of
 * silent eviction this ratio exists to prevent.
 */
export const MIN_RENEWALS_PER_LEASE = 3;

// --- Worker Health Constants ---
/** How often the runner samples event-loop delay (ms). */
export const LOOP_LAG_PROBE_MS = 1000;
/**
 * Event-loop delay that counts as unhealthy (ms).
 *
 * Note the probe can only observe delay it survives: code that blocks the loop
 * outright also blocks this timer, so a truly wedged loop is detected after it
 * recovers, not during. That is still strictly better than the silence before.
 */
export const LOOP_LAG_UNHEALTHY_MS = 5000;
/**
 * Fraction of the lease TTL that may elapse without a successful renewal before
 * the worker is considered unhealthy. Below 1 so the signal fires while the
 * lease is still alive rather than after routing has already dropped us.
 */
export const RENEW_STALENESS_RATIO = 0.8;
/** Consecutive healthy checks required to leave the degraded state. */
export const DEGRADED_RECOVERY_CHECKS = 3;
/** How long a runner may stay degraded before it is treated as fatal (ms). */
export const DEGRADED_MAX_MS = 120_000;

// --- Filesystem Constants ---
export const DEFAULT_WORKSPACE_DIR = 'workspace';

// --- Stream Read Markers ---
/** Redis XREAD/XREADGROUP marker for reading only new messages */
export const STREAM_READ_LAST_ID = '>';

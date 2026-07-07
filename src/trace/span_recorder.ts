import * as crypto from 'crypto';
import { Redis } from 'ioredis';
import { getRedis } from '../redis_client';
import { QueueNames } from '../constants';

// ---------------------------------------------------------------------------
// Deterministic ID helpers (mirrors Python str_to_uint64 / str_to_uint128)
// ---------------------------------------------------------------------------

/** Convert a string to a deterministic 64-bit unsigned integer via MD5. */
export function strToUint64(s: string): bigint {
    if (s.length === 16) {
        try {
            const val = BigInt('0x' + s);
            return val !== 0n ? val : 1n;
        } catch {
            // fall through
        }
    }
    const md5 = crypto.createHash('md5').update(s, 'utf8').digest('hex');
    const val = BigInt('0x' + md5.slice(0, 16));
    return val !== 0n ? val : 1n;
}

/** Return a 16-char lowercase hex string deterministically derived from s. */
export function spanIdHex(s: string): string {
    return strToUint64(s).toString(16).padStart(16, '0');
}

// ---------------------------------------------------------------------------
// ObservabilityConfig
// ---------------------------------------------------------------------------

const TRACE_TTL_SECONDS = 15 * 60;
const DEFAULT_METADATA_VALUE_MAX_LENGTH = 256;
const DISABLED_VALUES = new Set(['0', 'false', 'no', 'off', 'disabled']);
const ENABLED_VALUES  = new Set(['1', 'true',  'yes', 'on',  'enabled']);

function cleanEnv(name: string): string {
    return (process.env[name] || '').trim().replace(/^['"""'']+|['"""'']+$/g, '');
}

function envBool(name: string, defaultVal: boolean): boolean {
    const val = cleanEnv(name).toLowerCase();
    if (!val) return defaultVal;
    if (ENABLED_VALUES.has(val))  return true;
    if (DISABLED_VALUES.has(val)) return false;
    return defaultVal;
}

function envInt(name: string, defaultVal: number): number {
    const val = parseInt(cleanEnv(name), 10);
    return isNaN(val) ? defaultVal : val;
}

function envFloat(name: string, defaultVal: number, min: number, max: number): number {
    const val = parseFloat(cleanEnv(name));
    if (isNaN(val)) return defaultVal;
    return Math.min(max, Math.max(min, val));
}

export interface ObservabilityConfig {
    readonly enabled: boolean;
    readonly redisEnabled: boolean;
    readonly otelEnabled: boolean;
    readonly langfuseEnabled: boolean;
    readonly ttlSeconds: number;
    readonly sampleRate: number;
    readonly maxSpansPerTrace: number;
    readonly metadataValueMaxLength: number;
}

export function buildObservabilityConfig(): ObservabilityConfig {
    const enabled = envBool('BY_FRAMEWORK_OBSERVABILITY_ENABLED', true);
    return {
        enabled,
        redisEnabled: enabled && envBool('BY_FRAMEWORK_TRACE_REDIS_ENABLED', true),
        otelEnabled:  enabled && envBool('BY_FRAMEWORK_OTEL_ENABLED', false),
        langfuseEnabled: enabled && envBool('BY_FRAMEWORK_LANGFUSE_ENABLED',
            envBool('BYAI_LANGFUSE_ENABLED', true)),
        ttlSeconds: envInt('BY_FRAMEWORK_TRACE_TTL_SECONDS', TRACE_TTL_SECONDS),
        sampleRate: envFloat('BY_FRAMEWORK_TRACE_SAMPLE_RATE', 1.0, 0.0, 1.0),
        maxSpansPerTrace: Math.max(1, envInt('BY_FRAMEWORK_TRACE_MAX_SPANS_PER_TRACE', 1000)),
        metadataValueMaxLength: Math.max(32, envInt(
            'BY_FRAMEWORK_TRACE_METADATA_VALUE_MAX_LENGTH', DEFAULT_METADATA_VALUE_MAX_LENGTH
        )),
    };
}

// ---------------------------------------------------------------------------
// TraceSpan
// ---------------------------------------------------------------------------

export interface TraceSpan {
    readonly traceId: string;
    readonly spanId: string;
    readonly parentSpanId: string;
    readonly operation: string;
    readonly component: string;
    readonly startTs: number;
    readonly endTs: number;
    readonly status: string;
    readonly name?: string;
    readonly kind?: string;
    readonly source?: string;
    readonly input?: unknown;
    readonly output?: unknown;
    readonly tokens?: Record<string, unknown>;
    readonly cost?: Record<string, unknown>;
    readonly sessionId?: string;
    readonly executionId?: string;
    readonly messageId?: string;
    readonly parentMessageId?: string;
    readonly workerId?: string;
    readonly sourceAgentType?: string;
    readonly targetAgentType?: string;
    readonly errorType?: string;
    readonly errorMessage?: string;
    readonly errorCode?: string;
    readonly failedStage?: string;
    readonly retryable?: boolean;
    readonly routePolicy?: string;
    readonly routeStatus?: string;
    readonly queueWaitMs?: number;
    readonly chunkCount?: number;
    readonly eventType?: string;
    readonly metadata?: Record<string, unknown>;
}

/** Convert a TraceSpan to a Redis-serialisable plain object. */
export function traceSpanToPayload(span: TraceSpan): Record<string, unknown> {
    const startTs = span.startTs || 0;
    const endTs   = Math.max(startTs, span.endTs || 0);
    const payload: Record<string, unknown> = {
        trace_id:           span.traceId,
        span_id:            span.spanId,
        parent_span_id:     span.parentSpanId,
        operation:          span.operation,
        component:          span.component,
        start_ts:           startTs,
        end_ts:             endTs,
        duration_ms:        Math.max(0, endTs - startTs),
        status:             span.status,
        name:               span.name || span.operation,
        kind:               span.kind || '',
        source:             span.source || 'redis',
        session_id:         span.sessionId || '',
        execution_id:       span.executionId || '',
        message_id:         span.messageId || '',
        parent_message_id:  span.parentMessageId || '',
        worker_id:          span.workerId || '',
        source_agent_type:  span.sourceAgentType || '',
        target_agent_type:  span.targetAgentType || '',
        error_type:         span.errorType || '',
        error_message:      span.errorMessage || '',
        error_code:         span.errorCode || '',
        failed_stage:       span.failedStage || '',
        retryable:          span.retryable || false,
        route_policy:       span.routePolicy || '',
        route_status:       span.routeStatus || '',
        queue_wait_ms:      span.queueWaitMs || 0,
        chunk_count:        span.chunkCount || 0,
        event_type:         span.eventType || '',
    };
    if (span.input  != null) payload.input  = span.input;
    if (span.output != null) payload.output = span.output;
    if (span.tokens && Object.keys(span.tokens).length > 0)   payload.tokens = span.tokens;
    if (span.cost   && Object.keys(span.cost).length > 0)     payload.cost   = span.cost;
    if (span.metadata && Object.keys(span.metadata).length > 0) payload.metadata = span.metadata;
    // Drop empty strings to keep Redis entries compact
    return Object.fromEntries(
        Object.entries(payload).filter(([, v]) => v !== '' && v != null)
    );
}

// ---------------------------------------------------------------------------
// RedisSpanExporter
// ---------------------------------------------------------------------------

export class RedisSpanExporter {
    private readonly redis: Redis;
    private readonly ttlSeconds: number;

    constructor(redis?: Redis, ttlSeconds: number = TRACE_TTL_SECONDS) {
        this.redis = redis || getRedis();
        this.ttlSeconds = Math.max(1, ttlSeconds);
    }

    async exportSpan(span: TraceSpan): Promise<void> {
        const payload  = traceSpanToPayload(span);
        const traceId  = String(payload.trace_id || '');
        const startTs  = Number(payload.start_ts || 0);
        const endTs    = Number(payload.end_ts   || startTs);

        const metaKey  = QueueNames.trace_meta(traceId);
        const spansKey = QueueNames.trace_spans(traceId);

        // Read existing meta to compute true trace start
        let existingStartTs = 0;
        try {
            const raw = await this.redis.hget(metaKey, 'start_ts');
            existingStartTs = parseInt(raw || '0', 10) || 0;
        } catch { /* best effort */ }

        const traceStartTs = (existingStartTs > 0 && startTs > 0)
            ? Math.min(existingStartTs, startTs)
            : (existingStartTs || startTs);
        const updatedAt = Math.max(existingStartTs, endTs);

        // trace_meta/trace_spans share a Cluster hash tag (trace_id) since
        // Phase 2a, so this group stays atomic. The session/worker/agent_type
        // indexes below are cross-entity relative to the trace group and are
        // written as independent, best-effort calls instead (see
        // writeTraceIndex) so a CROSSSLOT-prone shared pipeline can't cause
        // the trace_meta/trace_spans write to lose its TTL on partial failure.
        const pipeline = this.redis.pipeline();

        // Persist meta hash
        pipeline.hset(metaKey, 'trace_id', traceId);
        pipeline.hset(metaKey, 'session_id', String(payload.session_id || ''));
        pipeline.hset(metaKey, 'status', String(payload.status || ''));
        const operation = String(payload.operation || '');
        if (payload.name && operation.startsWith('client.dispatch')) {
            pipeline.hset(metaKey, 'name', String(payload.name));
        }
        if (payload.target_agent_type && operation.startsWith('client.dispatch')) {
            pipeline.hset(metaKey, 'root_agent_type', String(payload.target_agent_type));
        }
        if (payload.message_id && operation.startsWith('client.dispatch')) {
            pipeline.hset(metaKey, 'root_message_id', String(payload.message_id));
        }
        pipeline.hset(metaKey, 'start_ts',   traceStartTs);
        pipeline.hset(metaKey, 'updated_at', updatedAt);
        pipeline.expire(metaKey, this.ttlSeconds);

        // Append span JSON to list
        pipeline.rpush(spansKey, JSON.stringify(payload));
        pipeline.expire(spansKey, this.ttlSeconds);

        await pipeline.exec();

        // Maintain sorted set indexes independently — cross-entity relative
        // to the trace group, so failure here must not affect the
        // trace_meta/trace_spans write that already landed above.
        if (payload.session_id) {
            await this.writeTraceIndex(
                QueueNames.trace_index_session(String(payload.session_id)),
                traceId,
                startTs
            );
        }
        if (payload.worker_id) {
            await this.writeTraceIndex(
                QueueNames.trace_index_worker(String(payload.worker_id)),
                traceId,
                startTs
            );
        }
        if (payload.target_agent_type) {
            await this.writeTraceIndex(
                QueueNames.trace_index_agent(String(payload.target_agent_type)),
                traceId,
                startTs
            );
        }
    }

    private async writeTraceIndex(indexKey: string, traceId: string, score: number): Promise<void> {
        try {
            await this.redis.zadd(indexKey, score, traceId);
            await this.redis.expire(indexKey, this.ttlSeconds);
        } catch (err) {
            console.debug(`[RedisSpanExporter] trace index write failed for ${indexKey}:`, err);
        }
    }
}

// ---------------------------------------------------------------------------
// SpanRecorder
// ---------------------------------------------------------------------------

export interface SpanExporter {
    exportSpan(span: TraceSpan): Promise<void>;
}

export class SpanRecorder {
    readonly config: ObservabilityConfig;
    readonly exporters: SpanExporter[];
    private readonly spansByTrace = new Map<string, number>();
    private readonly trackingMaxSize: number;

    constructor(
        redis?: Redis,
        options: {
            exporters?: SpanExporter[];
            config?: ObservabilityConfig;
        } = {}
    ) {
        this.config = options.config || buildObservabilityConfig();
        this.trackingMaxSize = Math.min(10_000, this.config.maxSpansPerTrace * 20);

        if (!this.config.enabled) {
            this.exporters = [];
        } else if (options.exporters) {
            this.exporters = [...options.exporters];
        } else if (this.config.redisEnabled) {
            this.exporters = [
                new RedisSpanExporter(redis || getRedis(), this.config.ttlSeconds),
            ];
        } else {
            this.exporters = [];
        }
    }

    private shouldRecord(span: TraceSpan): { ok: boolean; reason: string } {
        if (this.exporters.length === 0) return { ok: false, reason: 'disabled' };
        if (this.config.sampleRate <= 0)  return { ok: false, reason: 'sampled' };
        if (this.config.sampleRate < 1) {
            const bucket = parseInt(
                crypto.createHash('md5').update(span.traceId).digest('hex').slice(0, 8), 16
            );
            if ((bucket / 0xFFFFFFFF) >= this.config.sampleRate) {
                return { ok: false, reason: 'sampled' };
            }
        }
        const current = this.spansByTrace.get(span.traceId) || 0;
        if (current >= this.config.maxSpansPerTrace) {
            return { ok: false, reason: 'trace_span_limit' };
        }
        // Evict oldest half when tracking map is full
        if (this.spansByTrace.size >= this.trackingMaxSize) {
            const evictCount = Math.floor(this.spansByTrace.size / 2);
            let i = 0;
            for (const key of this.spansByTrace.keys()) {
                if (i++ >= evictCount) break;
                this.spansByTrace.delete(key);
            }
        }
        this.spansByTrace.set(span.traceId, current + 1);
        return { ok: true, reason: '' };
    }

    async recordSpan(span: TraceSpan): Promise<void> {
        const { ok } = this.shouldRecord(span);
        if (!ok) return;
        for (const exporter of this.exporters) {
            try {
                await exporter.exportSpan(span);
            } catch (err) {
                // best effort — never surface to caller
                console.debug('[SpanRecorder] exporter error:', err);
            }
        }
    }
}

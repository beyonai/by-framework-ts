import { Plugin, PluginBuildContext } from './plugin';
import type { AgentConfig } from './agent_config';
import type { AgentContext } from '../context';
import type { GatewayWorker } from '../worker';
import { spanIdHex } from '../trace/span_recorder';

// ── Loose types for the optional "langfuse" npm package ──────────────────────

type ObsHandle = {
    readonly id: string;
    end(opts?: EndOpts): void;
    update(opts: UpdateOpts): void;
    span(opts: SpanOpts): ObsHandle;
};

type TraceHandle = {
    readonly id: string;
    span(opts: SpanOpts): ObsHandle;
    update(opts: UpdateOpts): void;
};

type SpanOpts = {
    id?: string;
    name: string;
    startTime?: Date;
    input?: unknown;
    metadata?: Record<string, unknown>;
    parentObservationId?: string;
};

type EndOpts = {
    output?: unknown;
    endTime?: Date;
    level?: 'DEFAULT' | 'DEBUG' | 'WARNING' | 'ERROR';
    statusMessage?: string;
};

type UpdateOpts = {
    metadata?: Record<string, unknown>;
    level?: string;
    statusMessage?: string;
    input?: unknown;
    output?: unknown;
};

type LangfuseInstance = {
    trace(opts: {
        id?: string;
        name?: string;
        sessionId?: string;
        userId?: string;
        metadata?: Record<string, unknown>;
        input?: unknown;
        tags?: string[];
    }): TraceHandle;
    span(opts: SpanOpts & { traceId: string; parentObservationId?: string }): ObsHandle;
    flushAsync(): Promise<void>;
    shutdownAsync(): Promise<void>;
};

// ── Config ────────────────────────────────────────────────────────────────────

interface LangfuseEnvConfig {
    publicKey: string;
    secretKey: string;
    baseUrl: string;
}

function readEnvConfig(): LangfuseEnvConfig | null {
    const pk = process.env.LANGFUSE_PUBLIC_KEY || '';
    const sk = process.env.LANGFUSE_SECRET_KEY || '';
    if (!pk || !sk) return null;
    return {
        publicKey: pk,
        secretKey: sk,
        baseUrl: process.env.LANGFUSE_HOST || process.env.LANGFUSE_BASE_URL || 'https://cloud.langfuse.com',
    };
}

async function loadLangfuseClass(): Promise<(new (opts: Record<string, unknown>) => LangfuseInstance) | null> {
    try {
        // eslint-disable-next-line @typescript-eslint/no-require-imports
        const mod = require('langfuse') as Record<string, unknown>;
        return (mod.default ?? mod.Langfuse ?? null) as any;
    } catch {
        return null;
    }
}

function safeSerialize(value: unknown): unknown {
    if (value === null || value === undefined) return null;
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') return value;
    try {
        return JSON.parse(JSON.stringify(value));
    } catch {
        return String(value);
    }
}

// ── Stored state per execution ────────────────────────────────────────────────

interface ExecState {
    workerSpan: ObsHandle;
    taskSpan:   ObsHandle;
    workflowKey: string;
}

// ── Plugin ────────────────────────────────────────────────────────────────────

/**
 * LangfusePlugin — reports framework task lifecycle to Langfuse as a nested span tree.
 *
 * Prerequisites:
 *   npm install langfuse
 *
 * Required env vars:
 *   LANGFUSE_PUBLIC_KEY   — project public key
 *   LANGFUSE_SECRET_KEY   — project secret key
 *   LANGFUSE_HOST         — optional, defaults to https://cloud.langfuse.com
 *
 * Span hierarchy per task execution:
 *   trace (traceId)
 *     └── agent.workflow:{agentType}   (one per original message; persists across callAgent resume)
 *           └── worker.execute         (one per execution segment)
 *                 └── {agentType}      (agent task span; its ID propagated to sub-agents)
 *
 * Multi-agent nesting:
 *   The parent agent's task span ID is forwarded via MessageHeader.langfuseParentObservationId,
 *   so sub-agent workflow spans are automatically nested under the correct parent observation.
 */
export class LangfusePlugin extends Plugin {
    private _client: LangfuseInstance | null = null;

    // Key: "sessionId:messageId" → workflow span (kept alive across suspend/resume)
    private _workflowSpans = new Map<string, ObsHandle>();

    // Key: executionId → active execution state (cleared on complete/error/cancel)
    private _execState = new Map<string, ExecState>();

    constructor() {
        super({ plugin_id: 'langfuse', priority: 100 });
    }

    async registerAgentConfigs(_ctx: PluginBuildContext): Promise<AgentConfig[] | null> {
        return null;
    }

    // ── Lifecycle hooks ───────────────────────────────────────────────────────

    async onWorkerStartup(_worker: GatewayWorker): Promise<void> {
        const cfg = readEnvConfig();
        if (!cfg) {
            console.debug('[LangfusePlugin] LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY not set — plugin disabled');
            return;
        }
        const Cls = await loadLangfuseClass();
        if (!Cls) {
            console.warn('[LangfusePlugin] "langfuse" npm package not installed — plugin disabled. Run: npm install langfuse');
            return;
        }
        this._client = new Cls({ publicKey: cfg.publicKey, secretKey: cfg.secretKey, baseUrl: cfg.baseUrl, enabled: true });
        console.log(`[LangfusePlugin] Initialized — reporting to ${cfg.baseUrl}`);
    }

    async onWorkerShutdown(_worker: GatewayWorker): Promise<void> {
        try { await this._client?.flushAsync?.(); } catch { /* ignore */ }
        this._client = null;
    }

    async onTaskStart(context: AgentContext): Promise<void> {
        const client = this._client;
        if (!client) return;

        const header      = (context.currentCommand as any)?.header;
        const agentType   = (context as any).currentAgentType || header?.targetAgentType || 'unknown';
        const executionId = context.executionId || header?.messageId || '';
        const messageId   = header?.messageId || '';
        const sessionId   = context.sessionId;
        const traceId     = context.traceId;

        // Parent observation ID — set by the parent agent's LangfusePlugin via context.traceParentObservationId
        const parentObsId = header?.langfuseParentObservationId || '';

        // ── Workflow span: one per original message, survives suspend/resume ──
        const workflowKey = `${sessionId}:${messageId}`;
        let workflowSpan = this._workflowSpans.get(workflowKey);

        if (!workflowSpan) {
            client.trace({
                id:        traceId,
                name:      `agent.workflow:${agentType}`,
                sessionId,
                userId:    header?.userCode || header?.userName || undefined,
                input:     safeSerialize((context.currentCommand as any)?.content),
                metadata:  { agentType, sessionId, messageId },
                tags:      ['by-framework'],
            });

            workflowSpan = client.span({
                id:                  spanIdHex(executionId + ':workflow'),
                traceId,
                parentObservationId: parentObsId || undefined,
                name:                `agent.workflow:${agentType}`,
                startTime:           new Date(),
                input:               safeSerialize((context.currentCommand as any)?.content),
                metadata:            { messageId, agentType, sessionId },
            });
            this._workflowSpans.set(workflowKey, workflowSpan);
        }

        // ── worker.execute span: one per execution segment ────────────────────
        const workerSpan = client.span({
            id:                  spanIdHex(executionId + ':worker.execute'),
            traceId,
            parentObservationId: workflowSpan.id,
            name:                'worker.execute',
            startTime:           new Date(),
            input:               safeSerialize((context.currentCommand as any)?.content),
            metadata:            { executionId, messageId },
        });

        // ── agent.task span: the actual agent processing ──────────────────────
        const taskSpan = client.span({
            id:                  spanIdHex(executionId + ':agent.task'),
            traceId,
            parentObservationId: workerSpan.id,
            name:                agentType,
            startTime:           new Date(),
            input:               safeSerialize((context.currentCommand as any)?.content),
            metadata:            { executionId, agentType, sessionId },
        });

        this._execState.set(executionId, { workerSpan, taskSpan, workflowKey });

        // Propagate observation ID so downstream callAgent passes it to sub-agents
        context.traceParentObservationId = taskSpan.id;
    }

    async onTaskComplete(context: AgentContext, result: any): Promise<void> {
        const executionId = context.executionId || (context.currentCommand as any)?.header?.messageId || '';
        const state = this._execState.get(executionId);
        if (!state) return;
        this._execState.delete(executionId);

        const output = safeSerialize(result);
        state.taskSpan.end({ output, endTime: new Date() });
        state.workerSpan.end({ output, endTime: new Date() });

        // End workflow span only when fully done (not suspended waiting for a sub-agent)
        if (!context.isSuspended()) {
            this._workflowSpans.get(state.workflowKey)?.end({ output, endTime: new Date() });
            this._workflowSpans.delete(state.workflowKey);
        }
    }

    async onTaskError(context: AgentContext, error: Error): Promise<void> {
        const executionId = context.executionId || (context.currentCommand as any)?.header?.messageId || '';
        const state = this._execState.get(executionId);
        if (!state) return;
        this._execState.delete(executionId);

        const errOut = { error: error.message, errorType: error.constructor?.name };
        state.taskSpan.end({ output: errOut, endTime: new Date(), level: 'ERROR', statusMessage: error.message });
        state.workerSpan.end({ output: errOut, endTime: new Date(), level: 'ERROR', statusMessage: error.message });

        this._workflowSpans.get(state.workflowKey)?.end({ output: errOut, endTime: new Date(), level: 'ERROR', statusMessage: error.message });
        this._workflowSpans.delete(state.workflowKey);
    }

    async onTaskCancel(context: AgentContext, _cancelCommand: any): Promise<void> {
        const executionId = context.executionId || (context.currentCommand as any)?.header?.messageId || '';
        const state = this._execState.get(executionId);
        if (!state) return;
        this._execState.delete(executionId);

        const cancelOut = { status: 'CANCELLED' };
        state.taskSpan.end({ output: cancelOut, endTime: new Date(), level: 'WARNING', statusMessage: 'task cancelled' });
        state.workerSpan.end({ output: cancelOut, endTime: new Date(), level: 'WARNING', statusMessage: 'task cancelled' });

        this._workflowSpans.get(state.workflowKey)?.end({ output: cancelOut, endTime: new Date(), level: 'WARNING', statusMessage: 'task cancelled' });
        this._workflowSpans.delete(state.workflowKey);
    }
}

/**
 * Register LangfusePlugin automatically if LANGFUSE_PUBLIC_KEY and LANGFUSE_SECRET_KEY are set.
 * Call once before WorkerRunner.start() as an alternative to manual plugin registration.
 *
 * @returns true if registered, false if env vars are missing
 */
export function autoRegisterLangfusePlugin(): boolean {
    if (!readEnvConfig()) return false;
    Plugin.registerPluginClass(LangfusePlugin);
    return true;
}

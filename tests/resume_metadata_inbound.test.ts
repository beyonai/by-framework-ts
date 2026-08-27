import { WorkerRunner } from '../src/runner';
import { AnonymousWorker } from '../src/worker';
import { WorkerRegistry } from '../src/registry';
import { AgentContext } from '../src/context';
import { GatewayProcessor } from '../src/processor';
import { PluginRegistry } from '../src/extensions/registry';
import { AgentState } from '../src/protocol/agent_state';
import { AskAgentCommand, GatewayCommand, ResumeCommand, commandFromDict } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { CLIENT_SOURCE_AGENT_TYPE, QueueNames } from '../src/constants';
import { FRAMEWORK_HOP_METADATA_KEYS, mergeResumeMetadata } from '../src/resume_metadata';
import { MockRedis, bringAgentTypeOnline } from './helpers/mock_redis';

/**
 * The INBOUND half of resume metadata: what a resumed handler reads.
 *
 * `resume_metadata.test.ts` covers the outbound half — the header a resumed
 * execution SENDS, where the stored dispatch metadata replaces the waking
 * message's wholesale. This file covers the opposite direction and the
 * opposite rule: a resumed handler is the addressee of the message that woke
 * it, so that message's metadata is payload rather than plumbing and layers on
 * top of what the execution was originally dispatched with.
 *
 * Without this, everything an agent was dispatched with disappears the first
 * time it suspends.
 */

// ---------------------------------------------------------------------------
// The rule itself
// ---------------------------------------------------------------------------

describe('mergeResumeMetadata', () => {
    it('keeps the original dispatch metadata as the base', () => {
        expect(mergeResumeMetadata({ tenant: 'acme', req: 'r-1' }, {})).toEqual({
            tenant: 'acme',
            req: 'r-1',
        });
    });

    it('layers the waking message on top', () => {
        expect(mergeResumeMetadata({ tenant: 'acme' }, { answer: 'Pink' })).toEqual({
            tenant: 'acme',
            answer: 'Pink',
        });
    });

    it('lets the waking message win a collision', () => {
        // The newer, more specific hop — never the other way round.
        expect(mergeResumeMetadata({ tag: 'dispatch' }, { tag: 'waking' }).tag).toBe('waking');
    });

    it('never restores the framework hop keys from the snapshot', () => {
        const merged = mergeResumeMetadata(
            {
                tenant: 'acme',
                trace_parent_span_id: 'stale-trace',
                framework_parent_span_id: 'stale-framework',
                langfuse_parent_observation_id: 'stale-langfuse',
            },
            {}
        );
        expect(merged).toEqual({ tenant: 'acme' });
        for (const key of FRAMEWORK_HOP_METADATA_KEYS) {
            expect(merged).not.toHaveProperty(key);
        }
    });

    it('keeps the framework hop keys the CURRENT message supplies', () => {
        // Only the stored copy is stale; this hop's own values describe now.
        expect(
            mergeResumeMetadata({ trace_parent_span_id: 'stale' }, { trace_parent_span_id: 'current' })
        ).toEqual({ trace_parent_span_id: 'current' });
    });

    it('degrades to the waking message when the snapshot has no metadata', () => {
        // Records written before this field existed, or by another SDK.
        expect(mergeResumeMetadata(null, { client_tag: 't' })).toEqual({ client_tag: 't' });
        expect(mergeResumeMetadata({}, { client_tag: 't' })).toEqual({ client_tag: 't' });
        expect(mergeResumeMetadata(undefined, undefined)).toEqual({});
    });

    it('mutates neither input', () => {
        const stored = { tenant: 'acme' };
        const incoming = { answer: 'Pink' };
        mergeResumeMetadata(stored, incoming);
        expect(stored).toEqual({ tenant: 'acme' });
        expect(incoming).toEqual({ answer: 'Pink' });
    });
});

// ---------------------------------------------------------------------------
// GatewayWorker
// ---------------------------------------------------------------------------

type Handler = (command: GatewayCommand, context: AgentContext) => Promise<any>;

function node(redis: MockRedis, agentType: string, onTask: Handler) {
    const registry = new WorkerRegistry(redis as any);
    const worker = new AnonymousWorker({
        workerId: `worker-${agentType}`,
        agentTypes: [agentType],
        registry,
        redisClient: redis as any,
        pluginRegistry: new PluginRegistry(),
        onTask,
    });
    const runner = new WorkerRunner(worker, {
        redisClient: redis as any,
        groupName: `g-${agentType}`,
    });
    return { worker, runner, registry };
}

describe('a resumed handler reads its own dispatch metadata again', () => {
    async function resumeAgentB(params: {
        storedMetadata?: Record<string, unknown>;
        wakingMetadata: Record<string, unknown>;
    }) {
        const redis = new MockRedis();
        const seen: Record<string, unknown>[] = [];
        const { runner, registry } = node(redis, 'agent-b', async (command) => {
            seen.push({ ...command.header.metadata });
            return { status: AgentState.COMPLETED, reply_data: { ok: true } };
        });
        await bringAgentTypeOnline(redis, 'agent-b', 'worker-agent-b');

        // What A's dispatch wrote for B, then B suspended on askUser.
        await registry.initializeExecution({
            execution_id: 'exec-b',
            message_id: 'msg-b',
            session_id: 'sess-inbound',
            parent_message_id: 'msg-a',
            source_agent_type: 'agent-a',
            target_agent_type: 'agent-b',
            task_group_id: '',
            ...(params.storedMetadata === undefined ? {} : { metadata: params.storedMetadata }),
            status: AgentState.WAITING_USER,
        });

        const answer = new ResumeCommand(
            new MessageHeader('msg-b', 'sess-inbound', 'trace-inbound', {
                targetAgentType: 'agent-b',
                sourceAgentType: '',
                parentMessageId: '',
                metadata: params.wakingMetadata,
            }),
            'Pink',
            AgentState.COMPLETED,
            null
        );
        await runner.processAndAck(QueueNames.ctrl_stream('agent-b'), '1-0', answer);
        return { seen, redis };
    }

    it('merges the stored dispatch metadata under the waking message', async () => {
        const { seen } = await resumeAgentB({
            storedMetadata: { tenant: 'acme', tag: 'from-dispatch' },
            wakingMetadata: { answer: 'Pink', tag: 'from-waking' },
        });
        expect(seen).toHaveLength(1);
        expect(seen[0].tenant).toBe('acme');
        expect(seen[0].answer).toBe('Pink');
        // The newer hop wins the collision.
        expect(seen[0].tag).toBe('from-waking');
    });

    it('drops the snapshot\'s stale framework hop keys', async () => {
        const { seen } = await resumeAgentB({
            storedMetadata: {
                tenant: 'acme',
                trace_parent_span_id: 'stale-trace',
                framework_parent_span_id: 'stale-framework',
                langfuse_parent_observation_id: 'stale-langfuse',
            },
            wakingMetadata: {},
        });
        expect(seen[0]).toEqual({ tenant: 'acme' });
    });

    it('degrades to the waking message when the record predates the field', async () => {
        const { seen } = await resumeAgentB({
            storedMetadata: undefined,
            wakingMetadata: { client_tag: 't' },
        });
        expect(seen[0]).toEqual({ client_tag: 't' });
    });

    it('does not leak the inbound merge into the reply that goes out', async () => {
        // The two directions share a record, not a rule. Building the reply off
        // the restored command instead of the raw one is the mistake this pins.
        const { redis } = await resumeAgentB({
            storedMetadata: { caller: 'original' },
            wakingMetadata: { from_waking: 'should-not-leak' },
        });
        const replies = redis
            .getStreamPayloads(QueueNames.ctrl_stream('agent-a'))
            .map((p) => commandFromDict(p));
        expect(replies).toHaveLength(1);
        expect(replies[0].header.metadata).toEqual({ caller: 'original' });
        expect(replies[0].header.metadata).not.toHaveProperty('from_waking');
    });

    it('leaves a first dispatch alone — there is nothing to restore from', async () => {
        const redis = new MockRedis();
        const seen: Record<string, unknown>[] = [];
        const { runner } = node(redis, 'agent-b', async (command) => {
            seen.push({ ...command.header.metadata });
            return { status: AgentState.COMPLETED, reply_data: { ok: true } };
        });
        await bringAgentTypeOnline(redis, 'agent-b', 'worker-agent-b');

        await runner.processAndAck(
            QueueNames.ctrl_stream('agent-b'),
            '1-0',
            new AskAgentCommand(
                new MessageHeader('msg-fresh', 'sess-inbound', 'trace-inbound', {
                    targetAgentType: 'agent-b',
                    sourceAgentType: '',
                    parentMessageId: '',
                    metadata: { tenant: 'acme' },
                }),
                'hello'
            )
        );
        expect(seen[0]).toEqual({ tenant: 'acme' });
    });
});

// ---------------------------------------------------------------------------
// The write side: whoever executes a message records its dispatch metadata
// ---------------------------------------------------------------------------

describe('the executing worker records the dispatch metadata itself', () => {
    it('writes header.metadata onto the record on a first pickup', async () => {
        const redis = new MockRedis();
        const { runner, registry } = node(redis, 'agent-a', async () => ({
            status: AgentState.COMPLETED,
            reply_data: { ok: true },
        }));
        await bringAgentTypeOnline(redis, 'agent-a', 'worker-agent-a');

        // A client root dispatch: the record exists but carries no metadata,
        // because no dispatcher writes it for a root. The worker must.
        await registry.initializeExecution({
            execution_id: 'exec-root',
            message_id: 'msg-root',
            session_id: 'sess-write',
            parent_message_id: '',
            source_agent_type: CLIENT_SOURCE_AGENT_TYPE,
            target_agent_type: 'agent-a',
            task_group_id: '',
            status: 'QUEUED',
        });

        await runner.processAndAck(
            QueueNames.ctrl_stream('agent-a'),
            '1-0',
            new AskAgentCommand(
                new MessageHeader('msg-root', 'sess-write', 'trace-write', {
                    targetAgentType: 'agent-a',
                    sourceAgentType: '',
                    parentMessageId: '',
                    metadata: { tenant: 'acme', req: 'r-1' },
                }),
                'hello'
            )
        );

        const record = await registry.getExecutionByMessageId('msg-root', 'sess-write');
        expect(record?.metadata).toEqual({ tenant: 'acme', req: 'r-1' });
    });

    it('a resume must NOT overwrite the stored original', async () => {
        // The record's metadata is the only copy of what this execution was
        // originally dispatched with. Writing the waking message's over it on
        // the way in destroys exactly what the restore exists to recover.
        const redis = new MockRedis();
        const { runner, registry } = node(redis, 'agent-b', async () => ({
            status: AgentState.COMPLETED,
            reply_data: { ok: true },
        }));
        await bringAgentTypeOnline(redis, 'agent-b', 'worker-agent-b');

        await registry.initializeExecution({
            execution_id: 'exec-b',
            message_id: 'msg-b',
            session_id: 'sess-write',
            parent_message_id: 'msg-a',
            source_agent_type: 'agent-a',
            target_agent_type: 'agent-b',
            task_group_id: '',
            metadata: { tenant: 'acme' },
            status: AgentState.WAITING_USER,
        });

        await runner.processAndAck(
            QueueNames.ctrl_stream('agent-b'),
            '1-0',
            new ResumeCommand(
                new MessageHeader('msg-b', 'sess-write', 'trace-write', {
                    targetAgentType: 'agent-b',
                    sourceAgentType: '',
                    parentMessageId: '',
                    metadata: { client_tag: 'this-hop-only' },
                }),
                'Pink',
                AgentState.COMPLETED,
                null
            )
        );

        const record = await registry.getExecutionByMessageId('msg-b', 'sess-write');
        expect(record?.metadata).toEqual({ tenant: 'acme' });
    });
});

// ---------------------------------------------------------------------------
// GatewayProcessor: the second entry point, same rules
// ---------------------------------------------------------------------------

describe('GatewayProcessor restores the inbound metadata too', () => {
    async function processResume(snapshot: Record<string, unknown> | null, wakingMetadata: Record<string, unknown>) {
        const redis = new MockRedis();
        const registry = new WorkerRegistry(redis as any);
        if (snapshot) {
            await registry.initializeExecution({
                execution_id: 'exec-root',
                message_id: 'msg-root',
                session_id: 'sess-proc',
                target_agent_type: 'agent-a',
                status: AgentState.WAITING_USER,
                ...snapshot,
            });
        }
        const processor = new GatewayProcessor('worker-1', redis as any);
        let seen: Record<string, unknown> = {};
        await processor.process(
            new ResumeCommand(
                new MessageHeader('msg-root', 'sess-proc', 'trace-proc', {
                    targetAgentType: 'agent-a',
                    sourceAgentType: 'agent-b',
                    parentMessageId: 'msg-sub',
                    metadata: wakingMetadata,
                }),
                'answer',
                AgentState.COMPLETED,
                null
            ),
            async (command) => {
                seen = { ...command.header.metadata };
                return { status: AgentState.COMPLETED, reply_data: { done: true } };
            }
        );
        return seen;
    }

    it('merges for a client-dispatched root, which is owed no reply at all', async () => {
        // resolveReplyHeader short-circuits to null on the client sentinel. The
        // inbound restore must not share that short-circuit: a root has no
        // caller but still has its own metadata to get back. This is the case
        // that motivated the whole change.
        const seen = await processResume(
            {
                source_agent_type: CLIENT_SOURCE_AGENT_TYPE,
                parent_message_id: '',
                metadata: { tenant: 'acme', tag: 'from-dispatch' },
            },
            { answer: 'Pink', tag: 'from-waking' }
        );
        expect(seen.tenant).toBe('acme');
        expect(seen.answer).toBe('Pink');
        expect(seen.tag).toBe('from-waking');
    });

    it('drops stale framework hop keys', async () => {
        const seen = await processResume(
            {
                source_agent_type: CLIENT_SOURCE_AGENT_TYPE,
                metadata: {
                    tenant: 'acme',
                    trace_parent_span_id: 'stale',
                    framework_parent_span_id: 'stale',
                    langfuse_parent_observation_id: 'stale',
                },
            },
            {}
        );
        expect(seen).toEqual({ tenant: 'acme' });
    });

    it('degrades to the waking message without a stored value', async () => {
        const seen = await processResume(
            { source_agent_type: CLIENT_SOURCE_AGENT_TYPE },
            { client_tag: 't' }
        );
        expect(seen).toEqual({ client_tag: 't' });
    });
});

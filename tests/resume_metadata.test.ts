import { WorkerRunner } from '../src/runner';
import { AnonymousWorker } from '../src/worker';
import { WorkerRegistry } from '../src/registry';
import { AgentContext } from '../src/context';
import { PluginRegistry } from '../src/extensions/registry';
import { RoutePolicy } from '../src/availability';
import { AgentState } from '../src/protocol/agent_state';
import { AskAgentCommand, GatewayCommand, ResumeCommand, commandFromDict } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { AgentTaskResult } from '../src/protocol/results';
import { CLIENT_SOURCE_AGENT_TYPE, QueueNames } from '../src/constants';
import { MockRedis, bringAgentTypeOnline } from './helpers/mock_redis';

/**
 * The caller's dispatch metadata has to survive however many times the callee
 * suspends before it finally replies.
 *
 * The bug: a reply built by a resumed execution rebuilt its header from the
 * message that WOKE it — an askUser answer, or a nested sub-call's reply —
 * rather than from the dispatch it is actually answering. Routing fields were
 * already restored from the execution snapshot; `metadata` was not, so the
 * caller silently got a transient hop's metadata instead of its own. The
 * non-suspending reply path was never affected, which is exactly why every test
 * here has to reach the reply through a suspension.
 *
 * Two halves, and they are separate production changes:
 *  - the dispatch has to PERSIST the caller's metadata on the callee's
 *    execution record (two independent writers in TS: the single-call pipeline
 *    in dispatch/, and dispatchGroup's inline record in context.ts);
 *  - the resume has to RESTORE it as a full replacement, never merged with the
 *    waking message's own metadata.
 *
 * Ported from by-framework-python's fix/py-resume-metadata-loss (559c945):
 * context.py's initialize_execution payload, worker.py's
 * _resolve_reply_command, wait_sweeper.py's _synthesize_failure.
 */

const SESSION = 'sess-meta';

interface Node {
    readonly worker: AnonymousWorker;
    readonly runner: WorkerRunner;
}

type Handler = (command: GatewayCommand, context: AgentContext) => Promise<any>;

function node(redis: MockRedis, agentType: string, onTask: Handler): Node {
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
    return { worker, runner };
}

/** Hand-cranked control-plane bus, so each hop can be asserted on in between. */
class Bus {
    private readonly offsets = new Map<string, number>();
    private seq = 0;

    constructor(private readonly redis: MockRedis, private readonly nodes: Record<string, Node>) { }

    pending(agentType: string): GatewayCommand[] {
        const stream = QueueNames.ctrl_stream(agentType);
        const payloads = this.redis.getStreamPayloads(stream);
        return payloads.slice(this.offsets.get(stream) ?? 0).map((p) => commandFromDict(p));
    }

    async deliver(agentType: string): Promise<void> {
        const stream = QueueNames.ctrl_stream(agentType);
        const payloads = this.redis.getStreamPayloads(stream);
        const start = this.offsets.get(stream) ?? 0;
        this.offsets.set(stream, payloads.length);
        for (let i = start; i < payloads.length; i++) {
            this.seq += 1;
            await this.nodes[agentType].runner.processAndAck(
                stream,
                `${this.seq}-0`,
                commandFromDict(payloads[i])
            );
        }
    }
}

/** What GatewayClient writes for a root dispatch: the caller is the sentinel. */
async function seedRootExecution(
    registry: WorkerRegistry,
    params: { executionId: string; messageId: string; sessionId: string; targetAgentType: string }
): Promise<void> {
    await registry.initializeExecution({
        execution_id: params.executionId,
        message_id: params.messageId,
        session_id: params.sessionId,
        parent_message_id: '',
        source_agent_type: CLIENT_SOURCE_AGENT_TYPE,
        target_agent_type: params.targetAgentType,
        task_group_id: '',
        metadata: {},
        status: 'QUEUED',
    });
}

function rootAsk(messageId: string, sessionId: string, targetAgentType: string): AskAgentCommand {
    return new AskAgentCommand(
        new MessageHeader(messageId, sessionId, 'trace-meta', {
            targetAgentType,
            sourceAgentType: '',
            parentMessageId: '',
        }),
        'root question'
    );
}

// ---------------------------------------------------------------------------
// Half one: the dispatch persists it
// ---------------------------------------------------------------------------

describe('a dispatch records the caller\'s own metadata on the callee\'s execution', () => {
    test('the single-call pipeline (callAgent) persists it', async () => {
        // A resumed sub-task rebuilds its reply header's metadata from this
        // snapshot, not from whatever message wakes it — so the caller's
        // original metadata has to be persisted here, same as the routing
        // fields beside it. Mirrors Python
        // test_context.py::test_dispatch_records_caller_metadata_on_the_execution.
        const redis = new MockRedis();
        const registry = new WorkerRegistry(redis as any);
        await bringAgentTypeOnline(redis, 'agent-b');
        const ctx = new AgentContext(
            SESSION, 'trace-meta', redis as any, 'agent-a', 'msg-a'
        );

        const result = await ctx.callAgent({
            targetAgentType: 'agent-b',
            content: 'one',
            metadata: { caller: 'agent-a', request_id: 'req-1' },
        });

        const record = await registry.getExecutionByMessageId(result.messageId!, SESSION);
        expect(record).not.toBeNull();
        expect(record!.metadata.caller).toBe('agent-a');
        expect(record!.metadata.request_id).toBe('req-1');
    });

    test('dispatchGroup persists it on every member', async () => {
        // TS has a SECOND execution-record writer that Python does not: Python's
        // dispatch_group delegates to _dispatch_single_task, so it inherits the
        // field for free, while context.ts builds its own record inline. Miss it
        // and a group member that suspends before replying still loses the
        // caller's metadata while a non-suspending sibling keeps it.
        const redis = new MockRedis();
        const registry = new WorkerRegistry(redis as any);
        await bringAgentTypeOnline(redis, 'agent-b');
        await bringAgentTypeOnline(redis, 'agent-c');
        const ctx = new AgentContext(
            SESSION, 'trace-meta', redis as any, 'agent-a', 'msg-a'
        );

        const group = await ctx.dispatchGroup({
            tasks: [
                { targetAgentType: 'agent-b', content: 'one', metadata: { caller: 'agent-a', leg: 'b' } },
                { targetAgentType: 'agent-c', content: 'two', metadata: { caller: 'agent-a', leg: 'c' } },
            ],
        });

        expect(group.dispatchedTasks).toHaveLength(2);
        const records = await Promise.all(
            group.dispatchedTasks.map((t) => registry.getExecutionByMessageId(t.message_id, SESSION))
        );
        expect(records.map((r) => r!.metadata.caller)).toEqual(['agent-a', 'agent-a']);
        expect(records.map((r) => r!.metadata.leg)).toEqual(['b', 'c']);
    });

    test('a member rejected at dispatch time records it too', async () => {
        // This record is what the sweep reads to synthesize a failure for a
        // member that provably cannot answer; the stand-in has to carry the
        // caller's metadata like every other reply shape reaching it.
        const redis = new MockRedis();
        const registry = new WorkerRegistry(redis as any);
        const ctx = new AgentContext(
            SESSION, 'trace-meta', redis as any, 'agent-a', 'msg-a'
        );

        const group = await ctx.dispatchGroup({
            tasks: [{ targetAgentType: 'offline-agent', content: 'x', metadata: { caller: 'agent-a' } }],
        });

        const record = await registry.getExecutionByMessageId(
            group.dispatchedTasks[0].message_id,
            SESSION
        );
        expect(record!.status).toBe(AgentState.FAILED);
        expect(record!.metadata.caller).toBe('agent-a');
    });
});

// ---------------------------------------------------------------------------
// Half two: the resume restores it — through a real suspension, every time
// ---------------------------------------------------------------------------

describe('a group member that suspends still replies with the caller\'s metadata', () => {
    /**
     * A dispatches a one-member group to B; B calls askUser and unwinds; the
     * human answers; B resumes and replies. The reply is the group's single
     * accounting event, so it must carry both the group id AND A's metadata.
     */
    async function harness() {
        const redis = new MockRedis();
        const registry = new WorkerRegistry(redis as any);
        await bringAgentTypeOnline(redis, 'agent-b');

        const nodes: Record<string, Node> = {
            'agent-a': node(redis, 'agent-a', async (command, context) => {
                if (command instanceof ResumeCommand) {
                    return new AgentTaskResult({ status: AgentState.COMPLETED });
                }
                await context.dispatchGroup({
                    tasks: [{
                        targetAgentType: 'agent-b',
                        content: 'group leg',
                        // A's own dispatch metadata for this member.
                        metadata: { caller: 'agent-a', tag: 'keep' },
                    }],
                });
                return { status: AgentState.QUEUED };
            }),
            'agent-b': node(redis, 'agent-b', async (command, context) => {
                if (command instanceof ResumeCommand) {
                    return new AgentTaskResult({
                        status: AgentState.COMPLETED,
                        replyData: { userSaid: command.replyData as any },
                        metadata: { agent: 'agent-b', tag: 'from-agent-b' },
                    });
                }
                await context.askUser('which one?');
                return { status: AgentState.QUEUED };
            }),
        };
        const bus = new Bus(redis, nodes);

        await seedRootExecution(registry, {
            executionId: 'exec-a', messageId: 'msg-a', sessionId: SESSION,
            targetAgentType: 'agent-a',
        });
        await nodes['agent-a'].runner.processAndAck(
            QueueNames.ctrl_stream('agent-a'), '0-0',
            rootAsk('msg-a', SESSION, 'agent-a')
        );
        const memberMessageId = String(
            redis.getStreamPayloads(QueueNames.ctrl_stream('agent-b'))[0].header.message_id
        );
        await bus.deliver('agent-b'); // B asks the human and suspends
        return { redis, bus, memberMessageId };
    }

    test('B suspended on askUser does not reply to A', async () => {
        // Guards against the whole suite silently degrading into the
        // never-suspended path, where the reply header was always correct.
        const { bus } = await harness();
        expect(bus.pending('agent-a')).toEqual([]);
    });

    test('the reply carries the group id AND A\'s dispatch metadata', async () => {
        const { redis, bus, memberMessageId } = await harness();

        await redis.xadd(
            QueueNames.ctrl_stream('agent-b'), '*', 'data',
            JSON.stringify(new ResumeCommand(
                new MessageHeader(memberMessageId, SESSION, 'trace-meta', {
                    targetAgentType: 'agent-b',
                    sourceAgentType: CLIENT_SOURCE_AGENT_TYPE,
                    // The answering client's metadata for this hop only.
                    metadata: { caller: 'should-not-leak', client_tag: 'should-not-leak' },
                }),
                '', AgentState.COMPLETED, 'the blue one'
            ).toDict())
        );
        await bus.deliver('agent-b');

        const [reply] = bus.pending('agent-a');
        expect(reply).toBeDefined();
        // Still routed as a group reply — restoring metadata must not have
        // disturbed the fields the join keys on.
        expect(reply.header.taskGroupId).not.toBe('');
        expect(reply.header.parentMessageId).toBe(memberMessageId);
        expect(reply.header.metadata.caller).toBe('agent-a');
        expect(reply.header.metadata.tag).toBe('from-agent-b');
        expect(reply.header.metadata.agent).toBe('agent-b');
        expect(reply.header.metadata).not.toHaveProperty('client_tag');
    });
});

// ---------------------------------------------------------------------------
// Degradation: a record written before the field existed
// ---------------------------------------------------------------------------

describe('a snapshot with no metadata degrades to empty, never to the waking hop\'s', () => {
    /**
     * Rolling upgrade: executions dispatched by the previous version have no
     * `metadata` on their record. Falling back to the waking command's metadata
     * would be the original bug wearing a default value — the caller would get
     * an askUser answer's metadata attributed to itself. Empty is the only safe
     * degradation.
     */
    async function replyFromLegacyRecord(): Promise<ResumeCommand> {
        const redis = new MockRedis();
        const registry = new WorkerRegistry(redis as any);
        const b = node(redis, 'agent-b', async (command) => {
            if (command instanceof ResumeCommand) {
                return new AgentTaskResult({
                    status: AgentState.COMPLETED,
                    replyData: { from: 'b' },
                });
            }
            return new AgentTaskResult({ status: AgentState.QUEUED });
        });

        // Exactly what the pre-fix dispatch pipeline wrote: routing fields, no
        // `metadata` key at all.
        await registry.initializeExecution({
            execution_id: 'exec-legacy',
            message_id: 'msg-b',
            session_id: SESSION,
            trace_id: 'trace-meta',
            parent_message_id: 'msg-a',
            source_agent_type: 'agent-a',
            target_agent_type: 'agent-b',
            task_group_id: '',
            status: AgentState.WAITING_USER,
            cancel_requested: false,
            cancel_reason: '',
        });

        await b.runner.processAndAck(
            QueueNames.ctrl_stream('agent-b'), '1-0',
            new ResumeCommand(
                new MessageHeader('msg-b', SESSION, 'trace-meta', {
                    targetAgentType: 'agent-b',
                    sourceAgentType: CLIENT_SOURCE_AGENT_TYPE,
                    metadata: { caller: 'should-not-leak', client_tag: 'should-not-leak' },
                }),
                '', AgentState.COMPLETED, 'answer'
            )
        );

        const replies = redis.getStreamPayloads(QueueNames.ctrl_stream('agent-a'))
            .map((p) => commandFromDict(p) as ResumeCommand);
        expect(replies).toHaveLength(1);
        return replies[0];
    }

    test('the caller is still resolved and still gets its reply', async () => {
        const reply = await replyFromLegacyRecord();
        expect(reply.header.messageId).toBe('msg-a');
        expect(reply.header.targetAgentType).toBe('agent-a');
        expect(reply.replyData).toEqual({ from: 'b' });
    });

    test('none of the waking message\'s metadata is attributed to the caller', async () => {
        const reply = await replyFromLegacyRecord();
        expect(reply.header.metadata).not.toHaveProperty('caller');
        expect(reply.header.metadata).not.toHaveProperty('client_tag');
        expect(reply.header.metadata).toEqual({});
    });
});

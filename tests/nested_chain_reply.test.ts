import { WorkerRunner } from '../src/runner';
import { AnonymousWorker } from '../src/worker';
import { GatewayProcessor } from '../src/processor';
import { WorkerRegistry } from '../src/registry';
import { AgentContext, TaskCancelledError } from '../src/context';
import { PluginRegistry } from '../src/extensions/registry';
import { RoutePolicy } from '../src/availability';
import { AgentState } from '../src/protocol/agent_state';
import { EventType } from '../src/protocol/event_type';
import { AskAgentCommand, GatewayCommand, ResumeCommand, commandFromDict } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { AgentTaskResult } from '../src/protocol/results';
import { CLIENT_SOURCE_AGENT_TYPE, QueueNames } from '../src/constants';
import { MockRedis } from './helpers/mock_redis';

/**
 * D3: a resumed execution replies to ITS OWN caller, and a suspended one does
 * not reply at all.
 *
 * Both halves used to be broken by the same predicate,
 * `hasSourceAgent = !!header.sourceAgentType && !isResume`:
 *
 *  - "&& !isResume" denied every resumed execution a reply, so an A -> B -> C
 *    chain silently dropped B's result on the floor (AC-TS-5), as did a
 *    sub-agent that called askUser and then finished (AC-TS-6);
 *  - and had it not, the header it replied against belongs to the hop that just
 *    FINISHED — its sourceAgentType is our sub-agent — so the result would have
 *    been posted back down to the callee we just called.
 *
 * Meanwhile the reply itself was ungated on suspension, so a middle agent that
 * suspended on callAgent forwarded the placeholder its handler returned only so
 * it could unwind — waking its caller early AND burning the one reply that
 * caller was waiting for.
 *
 * Everything below runs the REAL WorkerRunner / GatewayWorker / WorkerRegistry /
 * AgentContext dispatch pipeline over the shared in-memory Redis, so the causal
 * chain (dispatch record -> resume -> rebuilt caller) is exercised rather than
 * restated.
 */

interface Node {
    readonly worker: AnonymousWorker;
    readonly runner: WorkerRunner;
}

type Handler = (command: GatewayCommand, context: AgentContext) => Promise<any>;

/** A worker + runner pair for one agent type, all sharing one Redis. */
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

/**
 * Hand-cranked control-plane bus: delivers whatever has newly landed on an
 * agent's ctrl stream to that agent's runner. Deliberately manual so each test
 * can assert on the stream contents BETWEEN hops.
 */
class Bus {
    private readonly offsets = new Map<string, number>();
    private seq = 0;

    constructor(private readonly redis: MockRedis, private readonly nodes: Record<string, Node>) { }

    /** Commands that have landed on an agent's ctrl stream but not been delivered. */
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
        status: 'QUEUED',
    });
}

function rootAsk(messageId: string, sessionId: string, targetAgentType: string): AskAgentCommand {
    return new AskAgentCommand(
        new MessageHeader(messageId, sessionId, 'trace-chain', {
            targetAgentType,
            sourceAgentType: '',
            parentMessageId: '',
        }),
        'root question'
    );
}

function events(redis: MockRedis, sessionId: string): any[] {
    return redis.getStreamPayloads(QueueNames.session_data_stream(sessionId));
}

// ---------------------------------------------------------------------------
// AC-TS-5: A -> B -> C
// ---------------------------------------------------------------------------

describe('AC-TS-5: A -> B -> C, B replies to A with its OWN result', () => {
    async function runChain(): Promise<{
        redis: MockRedis;
        bus: Bus;
        registry: WorkerRegistry;
    }> {
        const redis = new MockRedis();
        const registry = new WorkerRegistry(redis as any);

        const nodes: Record<string, Node> = {
            'agent-a': node(redis, 'agent-a', async (command, context) => {
                if (command instanceof ResumeCommand) {
                    return new AgentTaskResult({
                        status: AgentState.COMPLETED,
                        replyData: { finalFrom: 'a', heard: command.replyData as any },
                    });
                }
                await context.callAgent({
                    targetAgentType: 'agent-b',
                    content: 'do the thing',
                    routePolicy: RoutePolicy.SEND_ANYWAY,
                });
                return { status: AgentState.QUEUED };
            }),
            'agent-b': node(redis, 'agent-b', async (command, context) => {
                if (command instanceof ResumeCommand) {
                    // B's REAL answer, computed only now that C has replied.
                    return new AgentTaskResult({
                        status: AgentState.COMPLETED,
                        replyData: { from: 'b', refined: (command.replyData as any)?.from },
                    });
                }
                await context.callAgent({
                    targetAgentType: 'agent-c',
                    content: 'sub-thing',
                    routePolicy: RoutePolicy.SEND_ANYWAY,
                });
                return { status: AgentState.QUEUED };
            }),
            'agent-c': node(redis, 'agent-c', async () => new AgentTaskResult({
                status: AgentState.COMPLETED,
                replyData: { from: 'c' },
            })),
        };
        const bus = new Bus(redis, nodes);

        await seedRootExecution(registry, {
            executionId: 'exec-a', messageId: 'msg-a', sessionId: 'sess-chain',
            targetAgentType: 'agent-a',
        });
        await nodes['agent-a'].runner.processAndAck(
            QueueNames.ctrl_stream('agent-a'), '0-0',
            rootAsk('msg-a', 'sess-chain', 'agent-a')
        );

        return { redis, bus, registry };
    }

    test('B suspended on callAgent does NOT wake A early', async () => {
        const { bus } = await runChain();

        await bus.deliver('agent-b'); // B dispatches to C and suspends

        // The value B's handler returned was a placeholder so it could unwind.
        // Forwarding it would both hand A a fake answer and burn the one reply
        // A is waiting for — after which C's real result has nothing to wake.
        expect(bus.pending('agent-a')).toEqual([]);
        expect(bus.pending('agent-c')).toHaveLength(1);
    });

    test('B resumed by C replies to A, and the payload is B\'s result not C\'s', async () => {
        const { bus } = await runChain();

        await bus.deliver('agent-b');
        await bus.deliver('agent-c'); // C answers -> reply lands on B's stream
        expect(bus.pending('agent-b')).toHaveLength(1);

        await bus.deliver('agent-b'); // B resumes and finishes

        const toA = bus.pending('agent-a');
        expect(toA).toHaveLength(1);
        const reply = toA[0] as ResumeCommand;
        // Addressed to A's own execution (what runner reattaches by)...
        expect(reply.header.messageId).toBe('msg-a');
        expect(reply.header.targetAgentType).toBe('agent-a');
        // ...and sent BY B, not by C.
        expect(reply.header.sourceAgentType).toBe('agent-b');
        expect(reply.header.parentMessageId).not.toBe('');
        expect(reply.replyData).toEqual({ from: 'b', refined: 'c' });
    });

    test('nothing is ever posted back down to C, the callee B just called', async () => {
        const { bus } = await runChain();

        await bus.deliver('agent-b');
        await bus.deliver('agent-c');
        const beforeResume = bus.pending('agent-c');
        await bus.deliver('agent-b');

        // Replying against the resume's own header would address agent-c —
        // its sourceAgentType is the sub-agent that just finished.
        expect(bus.pending('agent-c')).toEqual(beforeResume);
    });

    test('A resumed by B reaches a terminal state and closes the stream', async () => {
        const { redis, bus, registry } = await runChain();

        await bus.deliver('agent-b');
        await bus.deliver('agent-c');
        await bus.deliver('agent-b');
        await bus.deliver('agent-a');

        const record = await registry.getExecution('exec-a', 'sess-chain');
        expect(record!.status).toBe(AgentState.COMPLETED);
        const eventTypes = events(redis, 'sess-chain').map((e) => e.event_type);
        expect(eventTypes).toContain(EventType.APP_STREAM_RESPONSE);
    });
});

// ---------------------------------------------------------------------------
// AC-TS-6: askUser inside a sub-agent
// ---------------------------------------------------------------------------

describe('AC-TS-6: a sub-agent that calls askUser replies to its caller afterwards', () => {
    async function harness() {
        const redis = new MockRedis();
        const registry = new WorkerRegistry(redis as any);

        const nodes: Record<string, Node> = {
            'agent-a': node(redis, 'agent-a', async (command, context) => {
                if (command instanceof ResumeCommand) {
                    return new AgentTaskResult({ status: AgentState.COMPLETED });
                }
                await context.callAgent({
                    targetAgentType: 'agent-b',
                    content: 'ask the human something',
                    routePolicy: RoutePolicy.SEND_ANYWAY,
                });
                return { status: AgentState.QUEUED };
            }),
            'agent-b': node(redis, 'agent-b', async (command, context) => {
                if (command instanceof ResumeCommand) {
                    return new AgentTaskResult({
                        status: AgentState.COMPLETED,
                        replyData: { userSaid: command.replyData as any },
                    });
                }
                await context.askUser('which one?');
                return { status: AgentState.QUEUED };
            }),
        };
        const bus = new Bus(redis, nodes);

        await seedRootExecution(registry, {
            executionId: 'exec-a', messageId: 'msg-a', sessionId: 'sess-ask',
            targetAgentType: 'agent-a',
        });
        await nodes['agent-a'].runner.processAndAck(
            QueueNames.ctrl_stream('agent-a'), '0-0',
            rootAsk('msg-a', 'sess-ask', 'agent-a')
        );
        // B's own dispatch id, read off the ctrl message before it is consumed;
        // it is what the front end's answer must address.
        const bMessageId = String(
            redis.getStreamPayloads(QueueNames.ctrl_stream('agent-b'))[0].header.message_id
        );
        await bus.deliver('agent-b'); // B asks the user and suspends
        return { redis, bus, registry, bMessageId };
    }

    test('B suspended on askUser does not reply to A', async () => {
        const { bus } = await harness();
        expect(bus.pending('agent-a')).toEqual([]);
    });

    test('the human answer resumes B, which then replies to A', async () => {
        const { redis, bus, bMessageId } = await harness();

        // What the front end sends back. Its header names B's execution; its
        // sourceAgentType is whatever the client put there — never the caller.
        await redis.xadd(
            QueueNames.ctrl_stream('agent-b'), '*', 'data',
            JSON.stringify(new ResumeCommand(
                new MessageHeader(bMessageId, 'sess-ask', 'trace-chain', {
                    targetAgentType: 'agent-b',
                    sourceAgentType: CLIENT_SOURCE_AGENT_TYPE,
                    parentMessageId: '',
                }),
                '', AgentState.COMPLETED, 'the blue one'
            ).toDict())
        );

        await bus.deliver('agent-b');

        const toA = bus.pending('agent-a');
        expect(toA).toHaveLength(1);
        expect((toA[0] as ResumeCommand).replyData).toEqual({ userSaid: 'the blue one' });
        expect(toA[0].header.messageId).toBe('msg-a');
        expect(toA[0].header.sourceAgentType).toBe('agent-b');
    });
});

// ---------------------------------------------------------------------------
// The client sentinel
// ---------------------------------------------------------------------------

describe('CLIENT_SOURCE_AGENT_TYPE on a root record is a marker, not a caller', () => {
    async function rootAskUserRound(): Promise<MockRedis> {
        const redis = new MockRedis();
        const registry = new WorkerRegistry(redis as any);
        const root = node(redis, 'agent-root', async (command, context) => {
            if (command instanceof ResumeCommand) {
                return new AgentTaskResult({ status: AgentState.COMPLETED, replyData: 'done' });
            }
            await context.askUser('colour?');
            return { status: AgentState.QUEUED };
        });

        await seedRootExecution(registry, {
            executionId: 'exec-root', messageId: 'msg-root', sessionId: 'sess-root',
            targetAgentType: 'agent-root',
        });
        await root.runner.processAndAck(
            QueueNames.ctrl_stream('agent-root'), '1-0',
            rootAsk('msg-root', 'sess-root', 'agent-root')
        );
        await root.runner.processAndAck(
            QueueNames.ctrl_stream('agent-root'), '2-0',
            new ResumeCommand(
                new MessageHeader('msg-root', 'sess-root', 'trace-chain', {
                    targetAgentType: 'agent-root',
                    sourceAgentType: CLIENT_SOURCE_AGENT_TYPE,
                }),
                '', AgentState.COMPLETED, 'blue'
            )
        );
        return redis;
    }

    test('no reply is posted to the control stream nobody consumes', async () => {
        const redis = await rootAskUserRound();

        // ctrl:agent_type:client has no consumer group and no reader: every
        // message written there is an unbounded leak AND a lost reply.
        expect(await redis.xlen(QueueNames.ctrl_stream(CLIENT_SOURCE_AGENT_TYPE))).toBe(0);
    });

    test('the end-of-stream event the front end waits on is still emitted', async () => {
        const redis = await rootAskUserRound();

        // Treating the sentinel as a caller flips hasSourceAgent, which
        // suppresses APP_STREAM_RESPONSE — the front end never sees the stream
        // close even though the answer arrived. Python regressed here once.
        const eventTypes = events(redis, 'sess-root').map((e) => e.event_type);
        expect(eventTypes).toContain(EventType.APP_STREAM_RESPONSE);
    });
});

// ---------------------------------------------------------------------------
// Gate exceptions
// ---------------------------------------------------------------------------

describe('the suspension gate has three exceptions, all load-bearing', () => {
    function subAgentAsk(): AskAgentCommand {
        return new AskAgentCommand(
            new MessageHeader('msg-sub', 'sess-gate', 'trace-gate', {
                sourceAgentType: 'agent-caller',
                targetAgentType: 'agent-sub',
                parentMessageId: 'msg-caller',
            }),
            'work'
        );
    }

    function subWorker(redis: MockRedis, onTask: Handler): AnonymousWorker {
        return new AnonymousWorker({
            workerId: 'worker-gate',
            agentTypes: ['agent-sub'],
            registry: new WorkerRegistry(redis as any),
            redisClient: redis as any,
            pluginRegistry: new PluginRegistry(),
            onTask,
        });
    }

    test('baseline: suspending on callAgent suppresses the reply entirely', async () => {
        const redis = new MockRedis();
        const worker = subWorker(redis, async (_c, context) => {
            await context.callAgent({
                targetAgentType: 'agent-leaf', content: 'x',
                routePolicy: RoutePolicy.SEND_ANYWAY,
            });
            return { status: AgentState.QUEUED };
        });

        const result = await worker.handleMessage(subAgentAsk());

        expect(result.status).toBe(AgentState.WAITING_AGENT);
        expect(await redis.xlen(QueueNames.ctrl_stream('agent-caller'))).toBe(0);
    });

    test('a terminal handler status after dispatching still replies', async () => {
        // It dispatched, but it finished anyway — it will never be resumed to
        // reply later, so it owes its caller a reply NOW.
        const redis = new MockRedis();
        const worker = subWorker(redis, async (_c, context) => {
            await context.callAgent({
                targetAgentType: 'agent-leaf', content: 'x', waitForReply: true,
                routePolicy: RoutePolicy.SEND_ANYWAY,
            });
            return new AgentTaskResult({ status: AgentState.COMPLETED, replyData: { done: true } });
        });

        await worker.handleMessage(subAgentAsk());

        const replies = redis.getStreamPayloads(QueueNames.ctrl_stream('agent-caller'));
        expect(replies).toHaveLength(1);
        expect(replies[0].body.status).toBe(AgentState.COMPLETED);
        expect(replies[0].body.reply_data).toEqual({ done: true });
    });

    test('a cancelled suspended execution still replies CANCELLED', async () => {
        // The execution is dead: no later resume will produce the reply, so
        // this is the caller's last chance to hear anything at all.
        const redis = new MockRedis();
        const worker = subWorker(redis, async (_c, context) => {
            await context.askUser('anything?');
            throw new TaskCancelledError('user aborted');
        });

        await worker.handleMessage(subAgentAsk());

        const replies = redis.getStreamPayloads(QueueNames.ctrl_stream('agent-caller'));
        expect(replies).toHaveLength(1);
        expect(replies[0].body.status).toBe(AgentState.CANCELLED);
    });

    test('a crashed suspended execution still replies FAILED', async () => {
        const redis = new MockRedis();
        const worker = subWorker(redis, async (_c, context) => {
            await context.askUser('anything?');
            throw new Error('boom');
        });

        await worker.handleMessage(subAgentAsk());

        const replies = redis.getStreamPayloads(QueueNames.ctrl_stream('agent-caller'));
        expect(replies).toHaveLength(1);
        expect(replies[0].body.status).toBe(AgentState.FAILED);
    });

    /**
     * The cancel and failure paths must be addressed the same way the success
     * path is. On a RESUMED execution the raw header names agent-leaf — the sub
     * we just called — so routing a CANCELLED/FAILED reply by it sends the bad
     * news down to the callee and leaves the real caller suspended forever.
     */
    describe('on a resumed execution the cancel/failure replies go UP, not down', () => {
        const resumedSnapshot = {
            isResumed: true,
            parentMessageId: 'msg-caller',
            existingData: {
                source_agent_type: 'agent-caller',
                parent_message_id: 'msg-caller',
                task_group_id: '',
            },
        };

        function resumeIntoSub(): ResumeCommand {
            return new ResumeCommand(
                new MessageHeader('msg-sub', 'sess-gate', 'trace-gate', {
                    targetAgentType: 'agent-sub',
                    sourceAgentType: 'agent-leaf',
                    parentMessageId: 'msg-leaf',
                }),
                '', AgentState.COMPLETED, { from: 'leaf' }
            );
        }

        test('cancel', async () => {
            const redis = new MockRedis();
            const worker = subWorker(redis, async () => { throw new TaskCancelledError('user aborted'); });

            await worker.handleMessage(resumeIntoSub(), { execution: resumedSnapshot });

            const replies = redis.getStreamPayloads(QueueNames.ctrl_stream('agent-caller'));
            expect(replies).toHaveLength(1);
            expect(replies[0].body.status).toBe(AgentState.CANCELLED);
            expect(replies[0].header.message_id).toBe('msg-caller');
            expect(await redis.xlen(QueueNames.ctrl_stream('agent-leaf'))).toBe(0);
        });

        test('failure', async () => {
            const redis = new MockRedis();
            const worker = subWorker(redis, async () => { throw new Error('boom'); });

            await worker.handleMessage(resumeIntoSub(), { execution: resumedSnapshot });

            const replies = redis.getStreamPayloads(QueueNames.ctrl_stream('agent-caller'));
            expect(replies).toHaveLength(1);
            expect(replies[0].body.status).toBe(AgentState.FAILED);
            expect(replies[0].header.message_id).toBe('msg-caller');
            expect(await redis.xlen(QueueNames.ctrl_stream('agent-leaf'))).toBe(0);
        });

        test('a cascade cancel is detected on the CALLER\'s record, not the sub-task\'s', async () => {
            // The lookup id also has to come from the rebuilt header: checking
            // the raw header's parentMessageId inspects msg-leaf, the sub we
            // just called, whose cancel flag says nothing about our caller.
            const redis = new MockRedis();
            const registry = new WorkerRegistry(redis as any);
            await registry.initializeExecution({
                execution_id: 'exec-caller', message_id: 'msg-caller', session_id: 'sess-gate',
                status: 'WAITING_AGENT', cancel_requested: true, cancel_reason: 'user aborted',
            });
            const worker = new AnonymousWorker({
                workerId: 'worker-gate', agentTypes: ['agent-sub'], registry,
                redisClient: redis as any, pluginRegistry: new PluginRegistry(),
                onTask: async () => { throw new TaskCancelledError('user aborted'); },
            });

            await worker.handleMessage(resumeIntoSub(), { execution: resumedSnapshot });

            // The caller is being cancelled too: no point telling it.
            expect(await redis.xlen(QueueNames.ctrl_stream('agent-caller'))).toBe(0);
        });
    });
});

// ---------------------------------------------------------------------------
// GatewayProcessor: the second, independent entry point
// ---------------------------------------------------------------------------

describe('GatewayProcessor resolves the caller the same way GatewayWorker does', () => {
    async function seedSubExecution(redis: MockRedis, overrides: { sourceAgentType: string }): Promise<void> {
        await new WorkerRegistry(redis as any).initializeExecution({
            execution_id: 'exec-proc',
            message_id: 'msg-proc',
            session_id: 'sess-proc',
            parent_message_id: 'msg-proc-caller',
            source_agent_type: overrides.sourceAgentType,
            target_agent_type: 'agent-proc',
            task_group_id: 'tg-proc',
            status: 'QUEUED',
        });
    }

    /** The reply a sub-agent of agent-proc sends back: header names agent-proc's execution. */
    function resumeIntoProcessor(): ResumeCommand {
        return new ResumeCommand(
            new MessageHeader('msg-proc', 'sess-proc', 'trace-proc', {
                targetAgentType: 'agent-proc',
                // The hop that just FINISHED, i.e. our sub-agent — never the caller.
                sourceAgentType: 'agent-leaf',
                parentMessageId: 'msg-leaf',
                taskGroupId: 'tg-leaf',
            }),
            '', AgentState.COMPLETED, { from: 'leaf' }
        );
    }

    test('a resumed execution replies to the caller from its execution record', async () => {
        const redis = new MockRedis();
        await seedSubExecution(redis, { sourceAgentType: 'agent-proc-caller' });

        await new GatewayProcessor('worker-proc', redis as any).process(
            resumeIntoProcessor(),
            async () => new AgentTaskResult({ status: AgentState.COMPLETED, replyData: { from: 'proc' } })
        );

        const replies = redis.getStreamPayloads(QueueNames.ctrl_stream('agent-proc-caller'));
        expect(replies).toHaveLength(1);
        // Reattachable by the caller, carrying OUR result and OUR group.
        expect(replies[0].header.message_id).toBe('msg-proc-caller');
        expect(replies[0].header.parent_message_id).toBe('msg-proc');
        expect(replies[0].header.task_group_id).toBe('tg-proc');
        expect(replies[0].header.source_agent_type).toBe('agent-proc');
        expect(replies[0].body.reply_data).toEqual({ from: 'proc' });
        // And nothing went back down to the sub-agent that just replied.
        expect(await redis.xlen(QueueNames.ctrl_stream('agent-leaf'))).toBe(0);
    });

    test('the client sentinel is not a caller here either', async () => {
        const redis = new MockRedis();
        await seedSubExecution(redis, { sourceAgentType: CLIENT_SOURCE_AGENT_TYPE });

        await new GatewayProcessor('worker-proc', redis as any).process(
            resumeIntoProcessor(),
            async () => new AgentTaskResult({ status: AgentState.COMPLETED })
        );

        expect(await redis.xlen(QueueNames.ctrl_stream(CLIENT_SOURCE_AGENT_TYPE))).toBe(0);
    });

    test('a suspended execution does not reply', async () => {
        const redis = new MockRedis();
        await seedSubExecution(redis, { sourceAgentType: 'agent-proc-caller' });

        await new GatewayProcessor('worker-proc', redis as any).process(
            resumeIntoProcessor(),
            async (_command, context) => {
                await context.askUser('one more thing?');
                return { status: AgentState.QUEUED };
            }
        );

        expect(await redis.xlen(QueueNames.ctrl_stream('agent-proc-caller'))).toBe(0);
    });

    test('a suspended execution that crashes still replies FAILED', async () => {
        const redis = new MockRedis();
        await seedSubExecution(redis, { sourceAgentType: 'agent-proc-caller' });

        await expect(
            new GatewayProcessor('worker-proc', redis as any).process(
                resumeIntoProcessor(),
                async (_command, context) => {
                    await context.askUser('one more thing?');
                    throw new Error('boom');
                }
            )
        ).rejects.toThrow('boom');

        const replies = redis.getStreamPayloads(QueueNames.ctrl_stream('agent-proc-caller'));
        expect(replies).toHaveLength(1);
        expect(replies[0].body.status).toBe(AgentState.FAILED);
        expect(replies[0].header.message_id).toBe('msg-proc-caller');
    });

    test('a fresh dispatch still replies against its own header', async () => {
        const redis = new MockRedis();

        await new GatewayProcessor('worker-proc', redis as any).process(
            new AskAgentCommand(
                new MessageHeader('msg-fresh', 'sess-proc', 'trace-proc', {
                    sourceAgentType: 'agent-proc-caller',
                    targetAgentType: 'agent-proc',
                    parentMessageId: 'msg-proc-caller',
                    taskGroupId: 'tg-fresh',
                }),
                'work'
            ),
            async () => new AgentTaskResult({ status: AgentState.COMPLETED })
        );

        const replies = redis.getStreamPayloads(QueueNames.ctrl_stream('agent-proc-caller'));
        expect(replies).toHaveLength(1);
        expect(replies[0].header.message_id).toBe('msg-proc-caller');
        expect(replies[0].header.task_group_id).toBe('tg-fresh');
    });
});

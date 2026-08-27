import { GatewayProcessor } from '../src/processor';
import { AgentContext } from '../src/context';
import { WorkerRegistry } from '../src/registry';
import { AskAgentCommand, GatewayCommand, ResumeCommand, commandFromDict } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { ActionType } from '../src/protocol/action_type';
import { AgentState } from '../src/protocol/agent_state';
import { AgentTaskResult } from '../src/protocol/results';
import { CLIENT_SOURCE_AGENT_TYPE, QueueNames } from '../src/constants';
import { MockRedis, bringAgentTypeOnline } from './helpers/mock_redis';

class StreamCaptureRedis {
    calls: Array<{ name: string; payload: string }> = [];

    async xadd(name: string, _id: string, field: string, payload: string): Promise<string> {
        this.calls.push({ name, payload });
        return '1-0';
    }

    pipeline() {
        const self = this;
        const pipe = {
            xadd: (name: string, id: string, field: string, payload: string) => {
                self.xadd(name, id, field, payload);
                return pipe;
            },
            expire: (key: string, seconds: number) => {
                return pipe;
            },
            exec: async () => {
                return [];
            }
        };
        return pipe;
    }
}

/**
 * GatewayProcessor 测试，对标 Python test_processor.py
 */
describe('GatewayProcessor', () => {
    test('enqueue callback emits ResumeCommand to source agent stream', async () => {
        const redis = new StreamCaptureRedis();
        const processor = new GatewayProcessor('worker-1', redis as any);

        const command = new AskAgentCommand(
            new MessageHeader('msg-1', 'sess-1', 'trace-1', {
                sourceAgentType: 'agent-a',
                targetAgentType: 'agent-b',
                parentMessageId: 'caller-msg-1',
            }),
            'do something'
        );

        await processor.process(command, async (_cmd, _ctx) => {
            return new AgentTaskResult({
                status: AgentState.COMPLETED,
                content: 'done',
                replyData: 'task result',
                metadata: { tokens: 123 },
                extraPayload: { debug_id: 'abc' },
            });
        });

        // Should have emitted: state events + callback to source agent
        const callbackCalls = redis.calls.filter((c) => c.name.includes('agent-a'));
        expect(callbackCalls.length).toBe(1);

        const callbackData = JSON.parse(callbackCalls[0].payload);
        expect(callbackData.action_type).toBe(ActionType.RESUME);
        expect(callbackData.body.status).toBe(AgentState.COMPLETED);
        expect(callbackData.body.content).toBe('done');
        expect(callbackData.body.reply_data).toBe('task result');
        expect(callbackData.body.extra_payload).toEqual({ debug_id: 'abc' });
        expect(callbackData.header.metadata).toEqual({ tokens: 123 });
        expect(callbackData.header.message_id).toBe('caller-msg-1');
        expect(callbackData.header.target_agent_type).toBe('agent-a');
        expect(callbackData.header.source_agent_type).toBe('agent-b');
        expect(callbackData.header.parent_message_id).toBe('msg-1');
    });

    test('process injects decoded command into context', async () => {
        const redis = new StreamCaptureRedis();
        const processor = new GatewayProcessor('worker-1', redis as any);

        const command = new AskAgentCommand(
            new MessageHeader('msg-2', 'sess-2', 'trace-2', {
                targetAgentType: 'target-agent',
            }),
            'test content'
        );

        let capturedCommand: any = null;
        await processor.process(command, async (cmd, ctx) => {
            capturedCommand = cmd;
            return null;
        });

        expect(capturedCommand).toBe(command);
        expect(capturedCommand.content).toBe('test content');
    });

    test('on failure enqueues FAILED callback to source agent', async () => {
        const redis = new StreamCaptureRedis();
        const processor = new GatewayProcessor('worker-1', redis as any);

        const command = new AskAgentCommand(
            new MessageHeader('msg-3', 'sess-3', 'trace-3', {
                sourceAgentType: 'caller-agent',
                targetAgentType: 'failing-agent',
            }),
            'fail task'
        );

        await expect(
            processor.process(command, async () => {
                throw new Error('handler crashed');
            })
        ).rejects.toThrow('handler crashed');

        const callbackCalls = redis.calls.filter((c) => c.name.includes('caller-agent'));
        expect(callbackCalls.length).toBe(1);

        const callbackData = JSON.parse(callbackCalls[0].payload);
        expect(callbackData.body.status).toBe('FAILED');
    });

    test('emits COMPLETED state when no source agent', async () => {
        const redis = new StreamCaptureRedis();
        const processor = new GatewayProcessor('worker-1', redis as any);

        const command = new AskAgentCommand(
            new MessageHeader('msg-4', 'sess-4', 'trace-4'),
            'standalone task'
        );

        await processor.process(command, async () => 'done');

        // State emission goes to session data stream
        const dataCalls = redis.calls.filter((c) => c.name.includes('sess-4'));
        const statePayloads = dataCalls.map((c) => JSON.parse(c.payload));
        const completedState = statePayloads.find(
            (p: any) => p.data?.choices?.[0]?.delta?.content?.includes(AgentState.COMPLETED)
        );
        expect(completedState).toBeTruthy();
    });

    test('emits RESUMED state for ResumeCommand', async () => {
        const redis = new StreamCaptureRedis();
        const processor = new GatewayProcessor('worker-1', redis as any);

        const command = new ResumeCommand(
            new MessageHeader('msg-5', 'sess-5', 'trace-5', {
                targetAgentType: 'resuming-agent',
            }),
            'resume content',
            'SUCCESS',
            { answer: 'data' }
        );

        await processor.process(command, async () => 'resumed result');

        const dataCalls = redis.calls.filter((c) => c.name.includes('sess-5'));
        const statePayloads = dataCalls.map((c) => JSON.parse(c.payload));
        const resumedState = statePayloads.find(
            (p: any) => p.data?.choices?.[0]?.delta?.content?.includes(AgentState.RESUMED)
        );
        expect(resumedState).toBeTruthy();
    });
});

/**
 * GatewayProcessor is the SECOND reply path (callers that drive their own
 * consume loop instead of subclassing GatewayWorker), and resolveReplyHeader is
 * its copy of GatewayWorker.resolveReplyCommand. The worker copy restores the
 * caller's dispatch metadata from the execution record; this one restored only
 * the three routing fields and left `metadata` as whatever woke the execution
 * up — the exact bug the worker path was fixed for, on the door nobody looked
 * at. Mirrors Python tests/worker/test_processor.py's
 * test_processor_resumed_reply_* pair.
 *
 * Every test here reaches the reply THROUGH A SUSPENSION on purpose: the
 * never-suspended path builds its reply header straight from the incoming
 * command, where the metadata was always right, so a test that skips the
 * suspension proves nothing.
 */
describe('a resumed execution on the processor path replies with the caller\'s metadata', () => {
    const SESSION = 'sess-proc-meta';

    /**
     * A (an AgentContext) dispatches to B with its own metadata; B runs on a
     * GatewayProcessor, calls askUser and unwinds; the human answers with
     * competing metadata; B resumes and replies to A.
     */
    async function suspendOnAskUser(): Promise<{
        redis: MockRedis;
        processor: GatewayProcessor;
        dispatched: AskAgentCommand;
        repliesToA: () => ResumeCommand[];
    }> {
        const redis = new MockRedis();
        await bringAgentTypeOnline(redis, 'agent-b');
        const processor = new GatewayProcessor('worker-proc', redis as any);

        // A's dispatch: this is what persists A's metadata on B's execution
        // record, which is the only durable record of what it was.
        const callerContext = new AgentContext(
            SESSION, 'trace-proc', redis as any, 'agent-a', 'msg-a'
        );
        await callerContext.callAgent({
            targetAgentType: 'agent-b',
            content: 'delegate',
            metadata: { caller: 'agent-a', tag: 'keep' },
        });

        const dispatched = commandFromDict(
            redis.getStreamPayloads(QueueNames.ctrl_stream('agent-b'))[0]
        ) as AskAgentCommand;

        // B suspends on askUser and unwinds without a result.
        await processor.process(dispatched, async (command: GatewayCommand, context: AgentContext) => {
            await context.askUser('which colour?');
            return { status: AgentState.QUEUED };
        });

        const repliesToA = () => redis
            .getStreamPayloads(QueueNames.ctrl_stream('agent-a'))
            .map((p) => commandFromDict(p) as ResumeCommand);
        return { redis, processor, dispatched, repliesToA };
    }

    /** What the client sends when the person answers; its metadata is this
     *  hop's plumbing and belongs to nobody downstream. */
    function theHumanAnswers(messageId: string): ResumeCommand {
        return new ResumeCommand(
            new MessageHeader(messageId, SESSION, 'trace-proc', {
                targetAgentType: 'agent-b',
                sourceAgentType: CLIENT_SOURCE_AGENT_TYPE,
                metadata: { caller: 'should-not-leak', client_tag: 'should-not-leak' },
            }),
            'Pink',
            AgentState.COMPLETED,
            { answer: 'Pink' }
        );
    }

    test('B suspended on askUser does not reply to A at all', async () => {
        // Guards the two tests below from degrading into the never-suspended
        // path, where the reply header was always correct.
        const { repliesToA } = await suspendOnAskUser();
        expect(repliesToA()).toEqual([]);
    });

    test('the reply carries A\'s metadata, not the answering client\'s', async () => {
        const { processor, dispatched, repliesToA } = await suspendOnAskUser();

        await processor.process(
            theHumanAnswers(dispatched.header.messageId),
            async () => new AgentTaskResult({
                status: AgentState.COMPLETED,
                replyData: { done: true },
                // B's own contribution: overrides same-named keys from A's
                // metadata, leaves the rest alone.
                metadata: { caller: 'overridden', tokens: 123 },
            })
        );

        const replies = repliesToA();
        expect(replies).toHaveLength(1);
        const [reply] = replies;
        // Proves this went through the resume rebuild: the waking command's own
        // parentMessageId is empty, so only the execution record can name A.
        expect(reply.header.messageId).toBe('msg-a');
        expect(reply.header.targetAgentType).toBe('agent-a');
        expect(reply.header.parentMessageId).toBe(dispatched.header.messageId);

        expect(reply.header.metadata.tag).toBe('keep');
        expect(reply.header.metadata.caller).toBe('overridden');
        expect(reply.header.metadata.tokens).toBe(123);
        expect(reply.header.metadata).not.toHaveProperty('client_tag');
    });

    test('a record predating the field degrades to an empty object', async () => {
        // Rolling upgrade: executions dispatched by the previous version carry
        // no `metadata` on their record. Falling back to the waking command's
        // metadata would be the original bug wearing a default value, so the
        // assertion is on the WHOLE object, not on the absence of leaked keys.
        const redis = new MockRedis();
        const registry = new WorkerRegistry(redis as any);
        const processor = new GatewayProcessor('worker-proc', redis as any);

        // Exactly what the pre-fix dispatch pipeline wrote: routing fields, no
        // `metadata` key at all.
        await registry.initializeExecution({
            execution_id: 'exec-legacy',
            message_id: 'msg-b',
            session_id: SESSION,
            trace_id: 'trace-proc',
            parent_message_id: 'msg-a',
            source_agent_type: 'agent-a',
            target_agent_type: 'agent-b',
            task_group_id: '',
            status: AgentState.WAITING_USER,
        });

        await processor.process(
            theHumanAnswers('msg-b'),
            async () => new AgentTaskResult({
                status: AgentState.COMPLETED,
                replyData: { done: true },
            })
        );

        const replies = redis
            .getStreamPayloads(QueueNames.ctrl_stream('agent-a'))
            .map((p) => commandFromDict(p) as ResumeCommand);
        expect(replies).toHaveLength(1);
        expect(replies[0].header.messageId).toBe('msg-a');
        expect(replies[0].header.metadata).toEqual({});
    });
});

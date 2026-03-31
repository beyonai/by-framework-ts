import { GatewayProcessor } from '../src/processor';
import { AskAgentCommand, ResumeCommand, commandFromDict } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { ActionType } from '../src/protocol/action_type';
import { AgentState } from '../src/protocol/agent_state';

class MockRedis {
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
        const redis = new MockRedis();
        const processor = new GatewayProcessor('worker-1', redis as any);

        const command = new AskAgentCommand(
            new MessageHeader('msg-1', 'sess-1', 'trace-1', {
                sourceAgentType: 'agent-a',
                targetAgentType: 'agent-b',
            }),
            'do something'
        );

        await processor.process(command, async (_cmd, _ctx) => {
            return 'task result';
        });

        // Should have emitted: state events + callback to source agent
        const callbackCalls = redis.calls.filter((c) => c.name.includes('agent-a'));
        expect(callbackCalls.length).toBe(1);

        const callbackData = JSON.parse(callbackCalls[0].payload);
        expect(callbackData.action_type).toBe(ActionType.RESUME);
        expect(callbackData.body.status).toBe('SUCCESS');
        expect(callbackData.body.reply_data).toBe('task result');
        expect(callbackData.header.target_agent_type).toBe('agent-a');
        expect(callbackData.header.source_agent_id).toBe('agent-b');
        expect(callbackData.header.parent_message_id).toBe('msg-1');
    });

    test('process injects decoded command into context', async () => {
        const redis = new MockRedis();
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
        const redis = new MockRedis();
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
        const redis = new MockRedis();
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
        const redis = new MockRedis();
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

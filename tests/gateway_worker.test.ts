import { GatewayWorker, AnonymousWorker } from '../src/worker';
import { AskAgentCommand, ResumeCommand, GatewayCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { AgentState } from '../src/protocol/agent_state';
import { AgentContext, TaskCancelledError } from '../src/context';
import { ActionType } from '../src/protocol/action_type';
import { WorkerRegistry } from '../src/registry';
import { PluginRegistry } from '../src/extensions/registry';

class MockRedis {
    calls: Array<{ name: string; payload: string }> = [];

    async xadd(name: string, _id: string, field: string, payload: string): Promise<string> {
        this.calls.push({ name, payload });
        return '1-0';
    }

    async hset(_key: string, _values: Record<string, string>): Promise<number> {
        return 1;
    }

    async hget(_key: string, _field: string): Promise<string | null> {
        return null;
    }

    async hgetall(_key: string): Promise<Record<string, string>> {
        return {};
    }

    async hincrby(_key: string, _field: string, _increment: number): Promise<number> {
        return 1;
    }

    async expire(_key: string, _seconds: number): Promise<number> {
        return 1;
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
 * GatewayWorker 测试，对标 Python test_gateway_worker.py
 */
describe('GatewayWorker', () => {
    function createWorker(onTask: (cmd: GatewayCommand, ctx: AgentContext) => Promise<any>, redis?: MockRedis): AnonymousWorker {
        const r = redis || new MockRedis();
        return new AnonymousWorker({
            workerId: 'test-worker',
            agentTypes: ['test-agent'],
            onTask,
            redisClient: r as any,
            pluginRegistry: new PluginRegistry(),
        });
    }

    test('handleMessage returns COMPLETED on success', async () => {
        const worker = createWorker(async () => 'done');

        const command = new AskAgentCommand(
            new MessageHeader('msg-1', 'sess-1', 'trace-1', {
                targetAgentType: 'test-agent',
            }),
            'test content'
        );

        const result = await worker.handleMessage(command);
        expect(result).toBe(AgentState.COMPLETED);
    });

    test('handleMessage returns CANCELLED on TaskCancelledError', async () => {
        const worker = createWorker(async () => {
            throw new TaskCancelledError('user cancelled');
        });

        const command = new AskAgentCommand(
            new MessageHeader('msg-2', 'sess-2', 'trace-2', {
                targetAgentType: 'test-agent',
            }),
            'cancel test'
        );

        const result = await worker.handleMessage(command);
        expect(result).toBe(AgentState.CANCELLED);
    });

    test('cancelled worker returns CANCELLED without emitting state events', async () => {
        const redis = new MockRedis();
        const worker = createWorker(async () => {
            throw new TaskCancelledError('timeout');
        }, redis);

        const command = new AskAgentCommand(
            new MessageHeader('msg-3', 'sess-3', 'trace-3', {
                targetAgentType: 'test-agent',
            }),
            'cancel state test'
        );

        const result = await worker.handleMessage(command);
        expect(result).toBe(AgentState.CANCELLED);

        // After removing emitState calls, no CANCELLING/CANCELLED state events should be emitted
        const statePayloads = redis.calls
            .filter((c) => c.name.includes('sess-3'))
            .map((c) => JSON.parse(c.payload));

        const states = statePayloads
            .map((p) => p.data?.choices?.[0]?.delta?.content)
            .filter(Boolean);

        expect(states.some((s: string) => s.includes(AgentState.CANCELLING))).toBe(false);
    });

    test('handleMessage returns FAILED on regular error', async () => {
        const worker = createWorker(async () => {
            throw new Error('something broke');
        });

        const command = new AskAgentCommand(
            new MessageHeader('msg-4', 'sess-4', 'trace-4', {
                targetAgentType: 'test-agent',
            }),
            'fail test'
        );

        const result = await worker.handleMessage(command);
        expect(result).toBe(AgentState.FAILED);
    });

    test('handleMessage enqueues callback to source agent', async () => {
        const redis = new MockRedis();
        const worker = createWorker(async () => 'result data', redis);

        const command = new AskAgentCommand(
            new MessageHeader('msg-5', 'sess-5', 'trace-5', {
                sourceAgentType: 'caller-agent',
                targetAgentType: 'test-agent',
            }),
            'callback test'
        );

        await worker.handleMessage(command);

        const callbackCalls = redis.calls.filter((c) => c.name.includes('caller-agent'));
        expect(callbackCalls.length).toBe(1);

        const callbackData = JSON.parse(callbackCalls[0].payload);
        expect(callbackData.action_type).toBe(ActionType.RESUME);
        expect(callbackData.body.status).toBe('COMPLETED');
        expect(callbackData.body.reply_data).toBe('result data');
        expect(callbackData.header.target_agent_type).toBe('caller-agent');
        expect(callbackData.header.parent_message_id).toBe('msg-5');
    });

    test('handleMessage processes ResumeCommand and emits RESUMED state', async () => {
        const redis = new MockRedis();
        const worker = createWorker(async (cmd) => {
            expect(cmd).toBeInstanceOf(ResumeCommand);
            return 'resumed';
        }, redis);

        const command = new ResumeCommand(
            new MessageHeader('msg-6', 'sess-6', 'trace-6', {
                targetAgentType: 'test-agent',
            }),
            'resume content',
            'SUCCESS',
            { answer: 42 }
        );

        const result = await worker.handleMessage(command);
        expect(result).toBe(AgentState.COMPLETED);

        const payloads = redis.calls
            .filter((c) => c.name.includes('sess-6'))
            .map((c) => JSON.parse(c.payload));
        const states = payloads
            .map((p) => p.data?.choices?.[0]?.delta?.content)
            .filter(Boolean);
        expect(states.some((s: string) => s.includes(AgentState.RESUMED))).toBe(true);
    });

    test('cancel callback enqueues CANCELLED status to source agent', async () => {
        const redis = new MockRedis();
        const worker = createWorker(async () => {
            throw new TaskCancelledError('cancel reason');
        }, redis);

        const command = new AskAgentCommand(
            new MessageHeader('msg-7', 'sess-7', 'trace-7', {
                sourceAgentType: 'parent-agent',
                targetAgentType: 'test-agent',
            }),
            'cancellable task'
        );

        await worker.handleMessage(command);

        const callbackCalls = redis.calls.filter((c) => c.name.includes('parent-agent'));
        expect(callbackCalls.length).toBe(1);

        const callbackData = JSON.parse(callbackCalls[0].payload);
        expect(callbackData.body.status).toBe(AgentState.CANCELLED);
    });

    test('AnonymousWorker returns agentTypes', () => {
        const worker = createWorker(async () => null);
        expect(worker.getAgentTypes()).toEqual(['test-agent']);
    });

    test('handleMessage passes cancel signal to context', async () => {
        const redis = new MockRedis();
        let capturedCancelState = false;

        const worker = createWorker(async (_cmd, ctx) => {
            capturedCancelState = ctx.isCancelRequested();
            return null;
        }, redis);

        const command = new AskAgentCommand(
            new MessageHeader('msg-8', 'sess-8', 'trace-8', {
                targetAgentType: 'test-agent',
            }),
            'signal test'
        );

        // Pass cancel signal that is already set
        await worker.handleMessage(command, {
            cancelSignal: { is_set: false } as any,
        });

        expect(capturedCancelState).toBe(false);
    });
});

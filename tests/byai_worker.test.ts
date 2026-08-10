import { GatewayWorker, AnonymousWorker } from '../src/worker';
import { ByaiWorker } from '../src/byai_worker';
import { ByaiAgentContext } from '../src/byai_context';
import { AskAgentCommand, GatewayCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { AgentState } from '../src/protocol/agent_state';
import { AgentContext } from '../src/context';
import { PluginRegistry } from '../src/extensions/registry';

class MockRedis {
    calls: Array<{ name: string; payload: string }> = [];

    async xadd(name: string, _id: string, field: string, payload: string): Promise<string> {
        this.calls.push({ name, payload });
        return '1-0';
    }

    async hset(): Promise<number> { return 1; }
    async hget(): Promise<string | null> { return null; }
    async hgetall(): Promise<Record<string, string>> { return {}; }
    async hincrby(): Promise<number> { return 1; }
    async expire(): Promise<number> { return 1; }

    pipeline() {
        const self = this;
        const pipe = {
            xadd: (name: string, id: string, field: string, payload: string) => {
                self.xadd(name, id, field, payload);
                return pipe;
            },
            hset: () => pipe,
            expire: () => pipe,
            exec: async () => [],
        };
        return pipe;
    }
}

class RecordingByaiWorker extends ByaiWorker {
    public receivedContent: unknown;
    public receivedContext: AgentContext | null = null;

    getAgentTypes(): ReadonlyArray<string> {
        return ['byai-agent'];
    }

    async processCommand(command: GatewayCommand, context: AgentContext): Promise<any> {
        this.receivedContent = (command as AskAgentCommand).content;
        this.receivedContext = context;
        return 'done';
    }
}

describe('ByaiWorker content auto-decode', () => {
    function wireMessageCommand(): AskAgentCommand {
        return new AskAgentCommand(
            new MessageHeader('msg-byai-1', 'sess-byai-1', 'trace-byai-1', {
                targetAgentType: 'byai-agent',
            }),
            [{ role: 'user', content: { text: 'hello from wire' } }]
        );
    }

    test('processCommand receives an already-decoded BaiYingMessage, not raw wire content', async () => {
        const redis = new MockRedis();
        const worker = new RecordingByaiWorker('worker-byai', undefined, redis as any, new PluginRegistry());

        const result = await worker.handleMessage(wireMessageCommand());

        expect(result.status).toBe(AgentState.COMPLETED);
        expect(worker.receivedContent).toEqual({
            role: 'user',
            content: { text: 'hello from wire', files: [], resources: [] },
        });
    });

    test('processCommand receives a ByaiAgentContext instance', async () => {
        const redis = new MockRedis();
        const worker = new RecordingByaiWorker('worker-byai', undefined, redis as any, new PluginRegistry());

        await worker.handleMessage(wireMessageCommand());

        expect(worker.receivedContext).toBeInstanceOf(ByaiAgentContext);
    });

    test('a plain string wire content passes through unchanged', async () => {
        const redis = new MockRedis();
        const worker = new RecordingByaiWorker('worker-byai', undefined, redis as any, new PluginRegistry());

        const command = new AskAgentCommand(
            new MessageHeader('msg-byai-2', 'sess-byai-2', 'trace-byai-2', {
                targetAgentType: 'byai-agent',
            }),
            'plain string content'
        );
        await worker.handleMessage(command);

        expect(worker.receivedContent).toBe('plain string content');
    });
});

describe('GatewayWorker default hooks (regression: existing subclasses unaffected)', () => {
    test('plain GatewayWorker subclasses receive raw content, undecoded, via the default no-op hook', async () => {
        const redis = new MockRedis();
        let receivedContent: unknown;
        const worker = new AnonymousWorker({
            workerId: 'plain-worker',
            agentTypes: ['plain-agent'],
            redisClient: redis as any,
            pluginRegistry: new PluginRegistry(),
            onTask: async (command) => {
                receivedContent = (command as AskAgentCommand).content;
                return 'ok';
            },
        });

        const wireContent = [{ role: 'user', content: { text: 'unchanged' } }];
        const command = new AskAgentCommand(
            new MessageHeader('msg-plain-1', 'sess-plain-1', 'trace-plain-1', {
                targetAgentType: 'plain-agent',
            }),
            wireContent
        );
        const result = await worker.handleMessage(command);

        expect(result.status).toBe(AgentState.COMPLETED);
        expect(receivedContent).toBe(wireContent);
    });

    test('GatewayWorker.getContextClass defaults to AgentContext (not ByaiAgentContext)', async () => {
        const redis = new MockRedis();
        let receivedContext: AgentContext | null = null;
        const worker = new AnonymousWorker({
            workerId: 'plain-worker-2',
            agentTypes: ['plain-agent'],
            redisClient: redis as any,
            pluginRegistry: new PluginRegistry(),
            onTask: async (_command, context) => {
                receivedContext = context;
                return 'ok';
            },
        });

        const command = new AskAgentCommand(
            new MessageHeader('msg-plain-2', 'sess-plain-2', 'trace-plain-2', {
                targetAgentType: 'plain-agent',
            }),
            'hi'
        );
        await worker.handleMessage(command);

        expect(receivedContext).not.toBeNull();
        expect(receivedContext).not.toBeInstanceOf(ByaiAgentContext);
        expect(receivedContext).toBeInstanceOf(AgentContext);
    });
});

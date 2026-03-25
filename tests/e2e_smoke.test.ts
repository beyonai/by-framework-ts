import { GatewayClient } from '../src/client';
import { AnonymousWorker } from '../src/worker';
import { WorkerRunner } from '../src/runner';
import { Plugin, PluginBuildContext } from '../src/extensions/plugin';
import { PluginRegistry } from '../src/extensions/registry';
import { AgentConfig } from '../src/extensions/agent_config';
import { InMemoryHistoryStorage, HistoryProvider } from '../src/history';

class MockRedisBus {
    private streams = new Map<string, Array<{ id: string; data: string }>>();
    private seq = 1;
    public ackCalls: Array<[string, string, string]> = [];

    async xadd(stream: string, _id: string, field: string, payload: string): Promise<string> {
        if (field !== 'data') {
            throw new Error(`unsupported field: ${field}`);
        }
        const id = `${this.seq++}-0`;
        if (!this.streams.has(stream)) {
            this.streams.set(stream, []);
        }
        this.streams.get(stream)!.push({ id, data: payload });
        return id;
    }

    async xgroup(..._args: any[]): Promise<'OK'> {
        return 'OK';
    }

    async xack(stream: string, group: string, msgId: string): Promise<number> {
        this.ackCalls.push([stream, group, msgId]);
        return 1;
    }

    async xreadgroup(...args: any[]): Promise<any> {
        const streamsIndex = args.findIndex((x: any) => x === 'STREAMS');
        const streamNames = args.slice(streamsIndex + 1, args.length / 2 + streamsIndex + 1);

        const result: any[] = [];
        for (const streamName of streamNames) {
            const queue = this.streams.get(streamName) || [];
            if (queue.length === 0) {
                continue;
            }
            const item = queue.shift()!;
            result.push([streamName, [[item.id, ['data', item.data]]]]);
        }
        return result.length > 0 ? result : null;
    }

    getStreamPayloads(stream: string): any[] {
        return (this.streams.get(stream) || []).map((i) => JSON.parse(i.data));
    }

    pipeline() {
        const self = this;
        const pipe = {
            xadd: (stream: string, id: string, field: string, payload: string) => {
                self.xadd(stream, id, field, payload);
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

class MockRegistry {
    private executionByMessage = new Map<string, any>();

    async getTargetWorker(_agentId: string): Promise<string | null> {
        return 'worker-e2e';
    }

    async saveExecution(execution: any): Promise<void> {
        this.executionByMessage.set(execution.message_id, { ...execution });
    }

    async getExecutionByMessageId(messageId: string): Promise<any | null> {
        return this.executionByMessage.get(messageId) || null;
    }

    async markExecutionFinished(executionId: string, status: string): Promise<void> {
        for (const [messageId, item] of this.executionByMessage.entries()) {
            if (item.execution_id === executionId) {
                this.executionByMessage.set(messageId, {
                    ...item,
                    status,
                });
                break;
            }
        }
    }

    async markExecutionCancelling(_executionId: string, _reason: string): Promise<void> {}
    async claimWorkerId(_workerId: string): Promise<string> { return 'lock-token'; }
    async refreshWorkerIdLock(_workerId: string): Promise<boolean> { return true; }
    async releaseWorkerId(_workerId: string, _token?: string): Promise<boolean> { return true; }
    async registerWorker(_workerId: string, _capabilities: string[]): Promise<void> {}
}

class E2EPlugin extends Plugin {
    constructor() {
        super({ plugin_id: 'e2e_plugin', enabled: true, priority: 1 });
    }

    async registerAgentConfigs(buildContext: PluginBuildContext): Promise<AgentConfig[]> {
        return [
            ...buildContext.listAgentConfigs(),
            {
                agent_id: 'e2e_agent',
                tools: {
                    echo_tool: ({ text }: { text: string }) => `tool:${text}`,
                },
                on_conflict: 'overwrite',
            },
        ];
    }
}

describe('E2E smoke: client -> runner -> worker -> plugin -> history', () => {
    test('processes one message end-to-end and persists history', async () => {
        const redis = new MockRedisBus();
        const registry = new MockRegistry();
        const pluginRegistry = new PluginRegistry();
        pluginRegistry.registerBundle(new E2EPlugin());
        await pluginRegistry.initializePlugins();

        const historyStorage = new InMemoryHistoryStorage();
        HistoryProvider.setStorage(historyStorage);
        await HistoryProvider.saveMessage('sess-e2e', 'system', 'preloaded');

        let receivedHistoryCount = -1;
        const worker = new AnonymousWorker({
            workerId: 'worker-e2e',
            capabilities: ['e2e-agent'],
            registry: registry as any,
            redisClient: redis as any,
            pluginRegistry,
            onTask: async (command: any, context) => {
                receivedHistoryCount = (command.extraPayload?.history || []).length;
                const config = context.getAgentConfig('e2e_agent');
                expect(config).toBeDefined();
                const toolResult = await context.callTool('echo_tool', { text: 'ping' });
                expect(toolResult).toBe('tool:ping');
                await context.emitChunk(`reply:${String(command.content)}`);
                return { ok: true };
            },
        });

        const runner = new WorkerRunner(worker, {
            redisClient: redis as any,
            groupName: 'group-e2e',
            fetchCount: 5,
            maxConcurrency: 2,
        });
        await runner.setupStreams();
        await runner.setupControlStreams();

        const client = new GatewayClient(registry as any, redis as any);
        const sendResult = await client.sendMessage({
            targetAgentType: 'e2e-agent',
            sessionId: 'sess-e2e',
            content: 'hello-e2e',
        });
        expect(sendResult.success).toBe(true);

        const messages = await runner.poll({ count: 5, block: 1 });
        expect(messages.length).toBe(1);
        await runner.processAndAck(messages[0].streamName, messages[0].msgId, messages[0].data);

        expect(receivedHistoryCount).toBeGreaterThanOrEqual(1);

        const dataMessages = redis.getStreamPayloads('byai_gateway:session:sess-e2e:data_stream');
        expect(dataMessages.some((msg) => msg.event_type === 'answerDelta')).toBe(true);

        const sessionHistory = await HistoryProvider.getSessionHistory('sess-e2e', 20);
        const roles = sessionHistory.map((m) => m.role);
        expect(roles).toContain('user');
        expect(roles).toContain('assistant');
    });
});

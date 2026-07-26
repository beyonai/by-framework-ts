import { GatewayClient } from '../src/client';
import { AnonymousWorker } from '../src/worker';
import { WorkerRunner } from '../src/runner';
import { Plugin, PluginBuildContext, PluginReloadContext } from '../src/extensions/plugin';
import { PluginRegistry } from '../src/extensions/registry';
import { AgentConfig } from '../src/extensions/agent_config';
import { QueueNames } from '../src/constants';

class MockRedisBus {
    private streams = new Map<string, Array<{ id: string; data: string }>>();
    private seq = 1;

    async xadd(stream: string, _id: string, field: string, payload: string): Promise<string> {
        if (field !== 'data') throw new Error(`unsupported field: ${field}`);
        const id = `${this.seq++}-0`;
        if (!this.streams.has(stream)) this.streams.set(stream, []);
        this.streams.get(stream)!.push({ id, data: payload });
        return id;
    }

    async xgroup(..._args: any[]): Promise<'OK'> { return 'OK'; }
    async xack(): Promise<number> { return 1; }

    async xreadgroup(...args: any[]): Promise<any> {
        const streamsIndex = args.findIndex((x: any) => x === 'STREAMS');
        const streamName = args[streamsIndex + 1];
        const queue = this.streams.get(streamName) || [];
        if (queue.length === 0) return null;
        const item = queue.shift()!;
        return [[streamName, [[item.id, ['data', item.data]]]]];
    }

    async xread(...args: unknown[]): Promise<any> {
        const streamsIndex = args.findIndex((x) => x === 'STREAMS');
        const streamName = args[streamsIndex + 1] as string;
        const queue = this.streams.get(streamName) || [];
        if (queue.length === 0) return null;
        return [[streamName, queue.map((item) => [item.id, ['data', item.data]])]];
    }

    pipeline() {
        const self = this;
        const pipe = {
            xadd: (stream: string, id: string, field: string, payload: string) => { self.xadd(stream, id, field, payload); return pipe; },
            expire: () => pipe,
            exec: async () => [],
        };
        return pipe;
    }
}

class MockRegistry {
    async hasOnlineAgentType(_agentType: string): Promise<[boolean, string[]]> {
        return [true, ['worker-reload']];
    }
    async getTargetWorker(): Promise<string | null> { return 'worker-reload'; }
    async isWorkerOnline(): Promise<boolean> { return true; }
    async saveExecution(): Promise<void> {}
    async getExecutionByMessageId(): Promise<any | null> { return null; }
    async markExecutionFinished(): Promise<void> {}
    async claimWorkerId(): Promise<string> { return 'lock-token'; }
    async refreshWorkerIdLock(): Promise<boolean> { return true; }
    async releaseWorkerId(): Promise<boolean> { return true; }
}

class ReloadDemoPlugin extends Plugin {
    constructor(private nextDescription: string) {
        super({ plugin_id: 'demo-agent', enabled: true });
    }

    async registerAgentConfigs(buildContext: PluginBuildContext): Promise<AgentConfig[]> {
        return [...buildContext.listAgentConfigs(), { agent_id: 'demo-agent', description: 'v1' } as AgentConfig];
    }

    async reload(context: PluginReloadContext): Promise<AgentConfig[]> {
        return context.currentAgentConfigs.map((c) =>
            c.agent_id === 'demo-agent' ? { ...c, description: this.nextDescription } : c
        );
    }
}

describe('Plugin hot-reload end-to-end: client trigger -> worker execution -> ack collection', () => {
    test('reloads plugin config on the worker without a restart, and the client observes the ack', async () => {
        const redis = new MockRedisBus();
        const registry = new MockRegistry();
        const pluginRegistry = new PluginRegistry();
        pluginRegistry.registerBundle(new ReloadDemoPlugin('v2'));
        await pluginRegistry.initializePlugins();
        expect(pluginRegistry.agentConfigs.find((c) => c.agent_id === 'demo-agent')?.description).toBe('v1');

        const worker = new AnonymousWorker({
            workerId: 'worker-reload',
            agentTypes: ['reload-agent'],
            registry: registry as any,
            redisClient: redis as any,
            pluginRegistry,
            onTask: async () => 'ok',
        });

        const runner = new WorkerRunner(worker, { redisClient: redis as any, groupName: 'group-reload' });
        await runner.setupStreams();
        await runner.setupControlStreams();

        const client = new GatewayClient(registry as any, redis as any);
        const dispatch = await client.reloadPluginsForAgentType({ agentType: 'reload-agent', reason: 'ship v2' });

        expect(dispatch.dispatchedCount).toBe(1);
        expect(dispatch.workerIds).toEqual(['worker-reload']);

        await runner.runControlOnce(1);

        // The worker's own plugin registry now reflects the reloaded config.
        expect(pluginRegistry.agentConfigs.find((c) => c.agent_id === 'demo-agent')?.description).toBe('v2');

        const acks = await client.collectReloadAcks(dispatch.reloadId);
        expect(acks).toHaveLength(1);
        expect(acks[0].status).toBe('success');
        expect(acks[0].worker_id).toBe('worker-reload');
        expect(acks[0].reload_id).toBe(dispatch.reloadId);
    });

    test('no online workers for the agent type: dispatch is a no-op with dispatchedCount 0', async () => {
        const redis = new MockRedisBus();
        class OfflineRegistry extends MockRegistry {
            async hasOnlineAgentType(): Promise<[boolean, string[]]> { return [false, []]; }
        }
        const client = new GatewayClient(new OfflineRegistry() as any, redis as any);
        const dispatch = await client.reloadPluginsForAgentType({ agentType: 'ghost-agent' });
        expect(dispatch.dispatchedCount).toBe(0);
        expect(dispatch.workerIds).toEqual([]);
    });

    test('a failing plugin reload publishes a failure ack, not a thrown error into the control loop', async () => {
        const redis = new MockRedisBus();
        const registry = new MockRegistry();
        const pluginRegistry = new PluginRegistry();
        class BrokenPlugin extends Plugin {
            constructor() { super({ plugin_id: 'broken-agent', enabled: true }); }
            async registerAgentConfigs(): Promise<AgentConfig[]> {
                return [{ agent_id: 'broken-agent' } as AgentConfig];
            }
            async reload(): Promise<never> { throw new Error('reload boom'); }
        }
        pluginRegistry.registerBundle(new BrokenPlugin());
        await pluginRegistry.initializePlugins();

        const worker = new AnonymousWorker({
            workerId: 'worker-reload',
            agentTypes: ['reload-agent-2'],
            registry: registry as any,
            redisClient: redis as any,
            pluginRegistry,
            onTask: async () => 'ok',
        });
        const runner = new WorkerRunner(worker, { redisClient: redis as any, groupName: 'group-reload-2' });
        await runner.setupStreams();
        await runner.setupControlStreams();

        const client = new GatewayClient(registry as any, redis as any);
        const dispatch = await client.reloadPluginsForAgentType({ agentType: 'reload-agent-2' });

        await expect(runner.runControlOnce(1)).resolves.not.toThrow();

        const acks = await client.collectReloadAcks(dispatch.reloadId);
        expect(acks).toHaveLength(1);
        expect(acks[0].status).toBe('failure');
        expect(acks[0].error).toContain('reload boom');
    });
});

import { PluginRegistry } from '../src/extensions/registry';
import { Plugin, PluginBuildContext, PluginReloadContext } from '../src/extensions/plugin';
import { AgentConfig } from '../src/extensions/agent_config';

class BasePlugin extends Plugin {
    constructor(private readonly config: AgentConfig) {
        super({ plugin_id: config.agent_id, enabled: true });
    }

    async registerAgentConfigs(buildContext: PluginBuildContext): Promise<AgentConfig[]> {
        return [...buildContext.listAgentConfigs(), this.config];
    }
}

class ReloadablePlugin extends BasePlugin {
    public reloadCalls: PluginReloadContext[] = [];
    constructor(config: AgentConfig, private readonly nextDescription: string) {
        super(config);
    }

    async reload(context: PluginReloadContext): Promise<AgentConfig[]> {
        this.reloadCalls.push(context);
        return context.currentAgentConfigs.map((c) =>
            c.agent_id === this.pluginId ? { ...c, description: this.nextDescription } : c
        );
    }
}

class ThrowingReloadPlugin extends BasePlugin {
    async reload(): Promise<never> {
        throw new Error('reload boom');
    }
}

describe('Plugin.reload default behavior', () => {
    test('defaults to a no-op passthrough of currentAgentConfigs', async () => {
        const plugin = new BasePlugin({ agent_id: 'noop-agent', description: 'v1' } as AgentConfig);
        const context: PluginReloadContext = {
            pluginId: 'noop-agent',
            reloadId: 'r-1',
            reason: '',
            currentAgentConfigs: [{ agent_id: 'noop-agent', description: 'v1' } as AgentConfig],
            previousStableAgentConfigs: [],
            currentVersion: 0,
        };
        const result = await plugin.reload(context);
        expect(result).toEqual(context.currentAgentConfigs);
    });
});

describe('PluginRegistry.reloadPlugins', () => {
    async function buildRegistry(plugin: Plugin): Promise<PluginRegistry> {
        const registry = new PluginRegistry();
        registry.registerBundle(plugin);
        await registry.initializePlugins();
        return registry;
    }

    test('replays each active plugin reload() and commits only after the whole chain succeeds', async () => {
        const plugin = new ReloadablePlugin({ agent_id: 'a', description: 'v1' } as AgentConfig, 'v2');
        const registry = await buildRegistry(plugin);
        const versionBefore = registry.agentConfigsVersion;

        const snapshot = await registry.reloadPlugins('reload-1', 'ship v2 prompt');

        expect(plugin.reloadCalls).toHaveLength(1);
        expect(plugin.reloadCalls[0].reloadId).toBe('reload-1');
        expect(plugin.reloadCalls[0].reason).toBe('ship v2 prompt');
        expect(snapshot.version).toBe(versionBefore + 1);
        expect(snapshot.configs.find((c) => c.agent_id === 'a')?.description).toBe('v2');
        expect(registry.agentConfigsVersion).toBe(versionBefore + 1);
        expect(registry.agentConfigs.find((c) => c.agent_id === 'a')?.description).toBe('v2');
    });

    test('is idempotent per reloadId — replaying the same id is a no-op', async () => {
        const plugin = new ReloadablePlugin({ agent_id: 'a', description: 'v1' } as AgentConfig, 'v2');
        const registry = await buildRegistry(plugin);

        await registry.reloadPlugins('reload-2', 'first');
        const versionAfterFirst = registry.agentConfigsVersion;
        expect(plugin.reloadCalls).toHaveLength(1);

        const secondSnapshot = await registry.reloadPlugins('reload-2', 'first');

        expect(plugin.reloadCalls).toHaveLength(1); // not replayed again
        expect(registry.agentConfigsVersion).toBe(versionAfterFirst);
        expect(secondSnapshot.version).toBe(versionAfterFirst);
    });

    test('a failing plugin reload leaves configs and version unchanged', async () => {
        const plugin = new ThrowingReloadPlugin({ agent_id: 'b', description: 'v1' } as AgentConfig);
        const registry = await buildRegistry(plugin);
        const versionBefore = registry.agentConfigsVersion;
        const configsBefore = registry.agentConfigs;

        await expect(registry.reloadPlugins('reload-3', 'bad change')).rejects.toThrow('reload boom');

        expect(registry.agentConfigsVersion).toBe(versionBefore);
        expect(registry.agentConfigs).toEqual(configsBefore);
        expect(registry.getReloadStatus('reload-3')?.status).toBe('failure');
        expect(registry.getReloadStatus('reload-3')?.error).toContain('reload boom');
    });

    test('records a success status with version_before/version_after', async () => {
        const plugin = new ReloadablePlugin({ agent_id: 'c', description: 'v1' } as AgentConfig, 'v2');
        const registry = await buildRegistry(plugin);
        const versionBefore = registry.agentConfigsVersion;

        await registry.reloadPlugins('reload-4', 'ok');

        const status = registry.getReloadStatus('reload-4');
        expect(status?.status).toBe('success');
        expect(status?.versionBefore).toBe(versionBefore);
        expect(status?.versionAfter).toBe(versionBefore + 1);
    });
});

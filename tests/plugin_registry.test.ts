import { PluginRegistry } from '../src/extensions/registry';
import { Plugin, PluginBuildContext, PromptTemplate } from '../src/extensions/plugin';
import { AgentConfig } from '../src/extensions/agent_config';

class CountingPlugin extends Plugin {
    shutdownCount = 0;
    constructor(id: string, enabled: boolean = true, priority: number = 0) {
        super({ plugin_id: id, enabled, priority });
    }

    async registerAgentConfigs(_buildContext: PluginBuildContext): Promise<AgentConfig[] | null> {
        return null;
    }

    async onWorkerShutdown(): Promise<void> {
        this.shutdownCount += 1;
    }

    async onTaskStart(context: any): Promise<void> {
        context.order.push(this.name);
    }
}

describe('PluginRegistry', () => {
    test('initializePlugins is idempotent and skips disabled plugins', async () => {
        const registry = new PluginRegistry();
        registry.registerBundle(new CountingPlugin('enabled', true));
        registry.registerBundle(new CountingPlugin('disabled', false));

        await registry.initializePlugins();
        await registry.initializePlugins();

        const stats = registry.getHookStats();
        expect(stats.enabled?.registerAgentConfigs?.success).toBe(1);
        expect(stats.disabled).toBeUndefined();
    });

    test('lifecycle order follows priority then name', async () => {
        const registry = new PluginRegistry();
        registry.registerBundle(new CountingPlugin('b', true, 10));
        registry.registerBundle(new CountingPlugin('a', true, 10));
        registry.registerBundle(new CountingPlugin('c', true, 1));

        const context = { order: [] as string[] };
        await registry.onTaskStart(context as any);
        expect(context.order).toEqual(['c', 'a', 'b']);
    });

    test('conflict strategy supports error/skip/overwrite', async () => {
        class FirstPlugin extends Plugin {
            constructor() { super({ plugin_id: 'first' }); }
            async registerAgentConfigs(): Promise<AgentConfig[]> {
                return [{ agent_id: 'dup' }];
            }
        }
        class ErrorPlugin extends Plugin {
            constructor() { super({ plugin_id: 'error' }); }
            async registerAgentConfigs(): Promise<AgentConfig[]> {
                return [{ agent_id: 'dup', on_conflict: 'error' }];
            }
        }
        class SkipPlugin extends Plugin {
            constructor() { super({ plugin_id: 'skip' }); }
            async registerAgentConfigs(): Promise<AgentConfig[]> {
                return [{ agent_id: 'dup', on_conflict: 'skip' }];
            }
        }
        class OverwritePlugin extends Plugin {
            constructor() { super({ plugin_id: 'overwrite' }); }
            async registerAgentConfigs(): Promise<AgentConfig[]> {
                return [{ agent_id: 'dup', name: 'new', on_conflict: 'overwrite' }];
            }
        }

        const registry = new PluginRegistry();
        registry.registerBundle(new FirstPlugin());
        await registry.initializePlugins();
        expect(registry.agentConfigs.find((c) => c.agent_id === 'dup')).toBeDefined();

        registry.registerBundle(new ErrorPlugin());
        await registry.initializePlugins();
        // error 插件不应标记初始化成功，因此多次初始化都会再次尝试并失败
        await registry.initializePlugins();

        registry.registerBundle(new SkipPlugin());
        await registry.initializePlugins();

        registry.registerBundle(new OverwritePlugin());
        await registry.initializePlugins();
        expect(registry.agentConfigs.find((c) => c.agent_id === 'dup')?.name).toBe('new');

        const stats = registry.getHookStats();
        expect(stats.error.registerAgentConfigs.failure).toBeGreaterThanOrEqual(2);
    });

    test('records timeout stats and supports resetHookStats', async () => {
        class SlowPlugin extends Plugin {
            constructor() { super({ plugin_id: 'slow' }, 0.01); }
            async registerAgentConfigs(): Promise<AgentConfig[] | null> { return null; }
            async onTaskStart(): Promise<void> {
                await new Promise((resolve) => setTimeout(resolve, 50));
            }
        }

        const registry = new PluginRegistry();
        registry.registerBundle(new SlowPlugin());
        await registry.onTaskStart({} as any);

        const stats = registry.getHookStats();
        expect(stats.slow.onTaskStart.failure).toBe(1);
        expect(stats.slow.onTaskStart.timeout).toBe(1);
        expect(stats.slow.onTaskStart.totalRuns).toBe(1);

        registry.resetHookStats('slow', 'onTaskStart');
        expect(registry.getHookStats().slow).toBeUndefined();
    });

    test('PromptTemplate renders and reports missing variables', () => {
        const prompt = new PromptTemplate('Hello {name}, from {city}');
        expect(() => prompt.render({ name: 'A' })).toThrow('Prompt missing variables');
        expect(prompt.render({ name: 'A', city: 'B' })).toBe('Hello A, from B');
    });

    test('registerAgentConfigs can read previous snapshot from build context', async () => {
        class SeedPlugin extends Plugin {
            constructor() { super({ plugin_id: 'seed', priority: 0 }); }
            async registerAgentConfigs(): Promise<AgentConfig[]> {
                return [{ agent_id: 'seed_agent', name: 'seed' }];
            }
        }

        class AppendPlugin extends Plugin {
            sawPrevIds: string[] = [];
            constructor() { super({ plugin_id: 'append', priority: 1 }); }
            async registerAgentConfigs(buildContext: PluginBuildContext): Promise<AgentConfig[]> {
                this.sawPrevIds = buildContext.getPrevAgentConfigs().map((c) => c.agent_id);
                return [
                    ...buildContext.listAgentConfigs().map((c) => ({ ...c, on_conflict: 'overwrite' as const })),
                    { agent_id: 'append_agent', name: 'append', on_conflict: 'overwrite' },
                ];
            }
        }

        const registry = new PluginRegistry();
        const append = new AppendPlugin();
        registry.registerBundles([new SeedPlugin(), append]);
        await registry.initializePlugins();

        expect(append.sawPrevIds).toEqual(['seed_agent']);
        expect(registry.agentConfigs.map((c) => c.agent_id)).toEqual(['seed_agent', 'append_agent']);
    });
});

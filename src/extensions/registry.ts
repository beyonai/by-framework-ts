import { AgentConfig, normalizeAgentConfig } from './agent_config';
import { Plugin, PluginBuildContext } from './plugin';
import type { AgentContext } from '../context';
import type { GatewayWorker } from '../worker';
import * as fs from 'fs/promises';
import * as path from 'path';
import { pathToFileURL } from 'url';

type HookStats = {
    success: number;
    failure: number;
    timeout: number;
    totalMs: number;
    lastError: string;
};

type HookStatsSnapshotItem = HookStats & { avgMs: number; totalRuns: number };

export class PluginRegistry {
    plugins: Plugin[] = [];
    logHookStatsOnShutdown = true;
    private agentConfigsInternal: AgentConfig[] = [];
    private initializedPlugins = new Set<string>();
    private hookStats = new Map<string, Map<string, HookStats>>();

    get agentConfigs(): AgentConfig[] {
        return [...this.agentConfigsInternal];
    }

    private findAgentConfig(agentId: string): AgentConfig | undefined {
        return this.agentConfigsInternal.find((c) => c.agent_id === agentId);
    }

    registerBundle(plugin: Plugin): void {
        if (!this.plugins.includes(plugin)) {
            if (this.plugins.some((p) => p.name === plugin.name)) {
                console.warn(`Duplicate plugin name detected: ${plugin.name}`);
            }
            this.plugins.push(plugin);
        }
    }

    registerBundles(plugins: Plugin[]): void {
        for (const plugin of plugins) {
            this.registerBundle(plugin);
        }
    }

    getPlugin(pluginId: string): Plugin | undefined {
        return this.plugins.find((p) => p.pluginId === pluginId);
    }

    getActivePlugins(): Plugin[] {
        return [...this.plugins]
            .filter((p) => p.manifest.enabled)
            .sort((a, b) => {
                const priorityGap = a.manifest.priority - b.manifest.priority;
                if (priorityGap !== 0) {
                    return priorityGap;
                }
                return a.name.localeCompare(b.name);
            });
    }

    private ensureHookStats(pluginName: string, hookName: string): HookStats {
        if (!this.hookStats.has(pluginName)) {
            this.hookStats.set(pluginName, new Map());
        }
        const perPlugin = this.hookStats.get(pluginName)!;
        if (!perPlugin.has(hookName)) {
            perPlugin.set(hookName, {
                success: 0,
                failure: 0,
                timeout: 0,
                totalMs: 0,
                lastError: '',
            });
        }
        return perPlugin.get(hookName)!;
    }

    private async executeHook(plugin: Plugin, hookName: string, run: () => Promise<void>): Promise<boolean> {
        const stat = this.ensureHookStats(plugin.name, hookName);
        const start = Date.now();
        try {
            const timeoutSeconds = plugin.hookTimeoutSeconds;
            if (timeoutSeconds && timeoutSeconds > 0) {
                await Promise.race([
                    run(),
                    new Promise<void>((_, reject) =>
                        setTimeout(
                            () => reject(new Error(`hook timeout (${plugin.hookTimeoutSeconds}s)`)),
                            timeoutSeconds * 1000
                        )
                    ),
                ]);
            } else {
                await run();
            }
            stat.success += 1;
            return true;
        } catch (error: any) {
            stat.failure += 1;
            stat.lastError = String(error?.message || error);
            if (String(error?.message || '').includes('timeout')) {
                stat.timeout += 1;
            }
            console.error(`Plugin ${plugin.name} ${hookName} failed:`, error);
            return false;
        } finally {
            stat.totalMs += Date.now() - start;
        }
    }

    private validateAgentConfig(config: AgentConfig): void {
        if (!config.agent_id) {
            throw new Error('AgentConfig.agent_id must not be empty');
        }
    }

    private registerAgentConfig(config: AgentConfig): void {
        const normalized = normalizeAgentConfig(config);
        this.validateAgentConfig(normalized);
        const existing = this.findAgentConfig(normalized.agent_id);
        if (existing) {
            if (existing === normalized) {
                return;
            }
            const conflict = normalized.on_conflict ?? 'error';
            if (conflict === 'error') {
                throw new Error(`agent_config '${normalized.agent_id}' is already registered`);
            }
            if (conflict === 'skip') {
                console.warn(`Skip duplicate agent_config registration: ${normalized.agent_id}`);
                return;
            }
            this.agentConfigsInternal = this.agentConfigsInternal.filter((c) => c.agent_id !== normalized.agent_id);
        }
        this.agentConfigsInternal.push(normalized);
    }

    async discoverPlugins(): Promise<void> {
        for (const Klass of Plugin.getRegisteredPlugins()) {
            const alreadyRegistered = this.plugins.some((p) => p instanceof Klass);
            if (alreadyRegistered) {
                continue;
            }
            try {
                const plugin = new Klass();
                this.registerBundle(plugin);
            } catch (error) {
                console.error(`Failed to instantiate plugin class ${Klass.name}:`, error);
            }
        }
    }

    async loadPluginsFromDir(directory: string): Promise<void> {
        let entries: string[] = [];
        try {
            entries = await fs.readdir(directory);
        } catch {
            console.warn(`Plugin directory not found or not a directory: ${directory}`);
            return;
        }

        for (const filename of entries) {
            if (filename.startsWith('__')) {
                continue;
            }
            if (!filename.endsWith('.js') && !filename.endsWith('.ts') && !filename.endsWith('.mjs') && !filename.endsWith('.cjs')) {
                continue;
            }
            const filePath = path.resolve(directory, filename);
            try {
                let mod: Record<string, any> = {};
                try {
                    // 优先使用 require，兼容 Jest/CJS 运行时下的本地文件加载
                    // eslint-disable-next-line @typescript-eslint/no-var-requires
                    mod = require(filePath);
                } catch {
                    mod = await import(pathToFileURL(filePath).href);
                }
                for (const value of Object.values(mod)) {
                    if (typeof value !== 'function') {
                        continue;
                    }
                    if (!(value.prototype instanceof Plugin)) {
                        continue;
                    }
                    const plugin = new (value as new () => Plugin)();
                    this.registerBundle(plugin);
                }
            } catch (error) {
                console.error(`Failed to load plugin module from ${filePath}:`, error);
            }
        }
    }

    async initializePlugins(buildContext?: PluginBuildContext): Promise<void> {
        const context = buildContext ?? new PluginBuildContext(this.agentConfigs);
        for (const plugin of this.getActivePlugins()) {
            if (this.initializedPlugins.has(plugin.pluginId)) {
                continue;
            }
            const ok = await this.executeHook(plugin, 'registerAgentConfigs', async () => {
                context.freezePrevAgentConfigs();
                const newConfigs = await plugin.registerAgentConfigs(context);
                if (newConfigs) {
                    context.setAgentConfigs(newConfigs);
                }
                for (const config of context.listAgentConfigs()) {
                    this.registerAgentConfig(config);
                }
            });
            if (ok) {
                this.initializedPlugins.add(plugin.pluginId);
            }
        }
    }

    applyDefaultHookTimeout(timeoutSeconds: number): void {
        if (timeoutSeconds <= 0) {
            return;
        }
        for (const plugin of this.plugins) {
            if (!plugin.hookTimeoutSeconds) {
                plugin.hookTimeoutSeconds = timeoutSeconds;
            }
        }
    }

    async onWorkerStartup(worker: GatewayWorker): Promise<void> {
        await this.discoverPlugins();
        await this.initializePlugins();
        for (const plugin of this.getActivePlugins()) {
            await this.executeHook(plugin, 'onWorkerStartup', () => plugin.onWorkerStartup(worker));
        }
    }

    async onWorkerShutdown(worker: GatewayWorker): Promise<void> {
        for (const plugin of this.getActivePlugins()) {
            await this.executeHook(plugin, 'onWorkerShutdown', () => plugin.onWorkerShutdown(worker));
        }
    }

    async onTaskStart(context: AgentContext): Promise<void> {
        for (const plugin of this.getActivePlugins()) {
            await this.executeHook(plugin, 'onTaskStart', () => plugin.onTaskStart(context));
        }
    }

    async onTaskComplete(context: AgentContext, result: any): Promise<void> {
        for (const plugin of this.getActivePlugins()) {
            await this.executeHook(plugin, 'onTaskComplete', () => plugin.onTaskComplete(context, result));
        }
    }

    async onTaskError(context: AgentContext, error: Error): Promise<void> {
        for (const plugin of this.getActivePlugins()) {
            await this.executeHook(plugin, 'onTaskError', () => plugin.onTaskError(context, error));
        }
    }

    async onTaskCancel(context: AgentContext, command: any): Promise<void> {
        for (const plugin of this.getActivePlugins()) {
            await this.executeHook(plugin, 'onTaskCancel', () => plugin.onTaskCancel(context, command));
        }
    }

    async onCallAgentStart(context: AgentContext, command: any): Promise<void> {
        for (const plugin of this.getActivePlugins()) {
            await this.executeHook(plugin, 'onCallAgentStart', () => plugin.onCallAgentStart(context, command));
        }
    }

    async onCallAgentComplete(context: AgentContext, command: any, result: any): Promise<void> {
        for (const plugin of this.getActivePlugins()) {
            await this.executeHook(plugin, 'onCallAgentComplete', () => plugin.onCallAgentComplete(context, command, result));
        }
    }

    async onCallAgentError(context: AgentContext, command: any, error: Error): Promise<void> {
        for (const plugin of this.getActivePlugins()) {
            await this.executeHook(plugin, 'onCallAgentError', () => plugin.onCallAgentError(context, command, error));
        }
    }

    async onAgentReturnStart(context: AgentContext, command: any, callbackCommand: any): Promise<void> {
        for (const plugin of this.getActivePlugins()) {
            await this.executeHook(plugin, 'onAgentReturnStart', () => plugin.onAgentReturnStart(context, command, callbackCommand));
        }
    }

    async onAgentReturnComplete(context: AgentContext, command: any, callbackCommand: any): Promise<void> {
        for (const plugin of this.getActivePlugins()) {
            await this.executeHook(plugin, 'onAgentReturnComplete', () => plugin.onAgentReturnComplete(context, command, callbackCommand));
        }
    }

    async onAgentReturnError(context: AgentContext, command: any, callbackCommand: any, error: Error): Promise<void> {
        for (const plugin of this.getActivePlugins()) {
            await this.executeHook(plugin, 'onAgentReturnError', () => plugin.onAgentReturnError(context, command, callbackCommand, error));
        }
    }

    getHookStats(): Record<string, Record<string, HookStatsSnapshotItem>> {
        const snapshot: Record<string, Record<string, HookStatsSnapshotItem>> = {};
        for (const [pluginName, pluginStats] of this.hookStats.entries()) {
            snapshot[pluginName] = {};
            for (const [hookName, stat] of pluginStats.entries()) {
                const totalRuns = stat.success + stat.failure;
                snapshot[pluginName][hookName] = {
                    ...stat,
                    avgMs: totalRuns > 0 ? stat.totalMs / totalRuns : 0,
                    totalRuns,
                };
            }
        }
        return snapshot;
    }

    logHookStats(): void {
        const stats = this.getHookStats();
        for (const [pluginName, pluginStats] of Object.entries(stats)) {
            for (const [hookName, stat] of Object.entries(pluginStats)) {
                console.info(
                    `Plugin hook stats: plugin=${pluginName} hook=${hookName} total_runs=${stat.totalRuns} success=${stat.success} failure=${stat.failure} timeout=${stat.timeout} avg_ms=${stat.avgMs.toFixed(2)} last_error=${stat.lastError}`
                );
            }
        }
    }

    resetHookStats(pluginName?: string, hookName?: string): void {
        if (!pluginName) {
            this.hookStats.clear();
            return;
        }
        const pluginStats = this.hookStats.get(pluginName);
        if (!pluginStats) {
            return;
        }
        if (!hookName) {
            this.hookStats.delete(pluginName);
            return;
        }
        pluginStats.delete(hookName);
        if (pluginStats.size === 0) {
            this.hookStats.delete(pluginName);
        }
    }
}

import { AgentConfig } from './agent_config';
import type { AgentContext } from '../context';
import type { GatewayWorker } from '../worker';

export class PromptTemplate {
    content: string;
    variables: string[];

    constructor(content: string, variables: string[] = []) {
        this.content = content;
        this.variables = variables.length > 0 ? [...variables] : this.extractVariables(content);
    }

    private extractVariables(content: string): string[] {
        const matches = content.matchAll(/\{([^{}]+)\}/g);
        const vars: string[] = [];
        for (const item of matches) {
            if (item[1]) {
                vars.push(item[1]);
            }
        }
        return vars;
    }

    render(values: Record<string, any>): string {
        const missing = this.variables.filter((v) => !(v in values));
        if (missing.length > 0) {
            throw new Error(`Prompt missing variables: ${missing.join(', ')}`);
        }
        return this.content.replace(/\{([^{}]+)\}/g, (_, key: string) => String(values[key] ?? ''));
    }
}

export interface PluginManifest {
    plugin_id: string;
    version?: string;
    priority?: number;
    enabled?: boolean;
}

export class PluginBuildContext {
    private prevAgentConfigs: ReadonlyArray<AgentConfig> = [];
    constructor(private agentConfigs: AgentConfig[] = []) {}

    setAgentConfigs(configs: AgentConfig[]): void {
        this.agentConfigs = [...configs];
    }

    listAgentConfigs(): AgentConfig[] {
        return [...this.agentConfigs];
    }

    freezePrevAgentConfigs(): void {
        this.prevAgentConfigs = [...this.agentConfigs];
    }

    getPrevAgentConfigs(): ReadonlyArray<AgentConfig> {
        return this.prevAgentConfigs;
    }
}

export abstract class Plugin {
    static registeredPlugins: Array<new () => Plugin> = [];

    manifest: Required<PluginManifest>;
    name: string;
    pluginId: string;
    version: string;
    hookTimeoutSeconds?: number;

    constructor(manifest: PluginManifest, hookTimeoutSeconds?: number) {
        this.manifest = {
            plugin_id: manifest.plugin_id,
            version: manifest.version ?? '1.0.0',
            priority: manifest.priority ?? 0,
            enabled: manifest.enabled ?? true,
        };
        this.name = this.manifest.plugin_id;
        this.pluginId = this.manifest.plugin_id;
        this.version = this.manifest.version;
        this.hookTimeoutSeconds = hookTimeoutSeconds;
    }

    static registerPluginClass<T extends new () => Plugin>(pluginClass: T): T {
        if (!Plugin.registeredPlugins.includes(pluginClass)) {
            Plugin.registeredPlugins.push(pluginClass);
        }
        return pluginClass;
    }

    static getRegisteredPlugins(): Array<new () => Plugin> {
        return [...Plugin.registeredPlugins];
    }

    abstract registerAgentConfigs(buildContext: PluginBuildContext): Promise<AgentConfig[] | null>;

    async onWorkerStartup(_worker: GatewayWorker): Promise<void> {}
    async onWorkerShutdown(_worker: GatewayWorker): Promise<void> {}
    async onTaskStart(_context: AgentContext): Promise<void> {}
    async onTaskComplete(_context: AgentContext, _result: any): Promise<void> {}
    async onTaskError(_context: AgentContext, _error: Error): Promise<void> {}
    async onTaskCancel(_context: AgentContext, _command: any): Promise<void> {}

    /** Triggered before enqueuing a call to another agent. */
    async onCallAgentStart(_context: AgentContext, _command: any): Promise<void> {}
    /** Triggered after successfully enqueuing a call to another agent. */
    async onCallAgentComplete(_context: AgentContext, _command: any, _result: any): Promise<void> {}
    /** Triggered when enqueuing a call to another agent fails. */
    async onCallAgentError(_context: AgentContext, _command: any, _error: Error): Promise<void> {}

    /** Triggered before enqueueing a ResumeCommand back to the caller agent. */
    async onAgentReturnStart(_context: AgentContext, _command: any, _callbackCommand: any): Promise<void> {}
    /** Triggered after successfully enqueueing a ResumeCommand to the caller. */
    async onAgentReturnComplete(_context: AgentContext, _command: any, _callbackCommand: any): Promise<void> {}
    /** Triggered when enqueueing a ResumeCommand to the caller fails. */
    async onAgentReturnError(_context: AgentContext, _command: any, _callbackCommand: any, _error: Error): Promise<void> {}
}


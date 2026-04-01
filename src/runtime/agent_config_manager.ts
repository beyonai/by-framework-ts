/**
 * Agent configuration management.
 *
 * Provides management for AgentConfig instances including:
 * - Storage and retrieval
 * - Search and filtering
 * - Conflict resolution
 * - Default agent configuration handling
 */

import { AgentConfig, ConflictStrategy } from '../extensions/agent_config';

export class AgentConfigManager {
  private readonly configs = new Map<string, AgentConfig>();

  constructor(configs?: AgentConfig[]) {
    if (configs) {
      for (const config of configs) {
        this.addConfig(config);
      }
    }
  }

  addConfig(config: AgentConfig): boolean {
    if (this.configs.has(config.agent_id)) {
      const existingConfig = this.configs.get(config.agent_id)!;
      if (config.on_conflict === 'overwrite') {
        this.configs.set(config.agent_id, config);
        return true;
      } else if (config.on_conflict === 'error') {
        throw new Error(`AgentConfig with id '${config.agent_id}' already exists`);
      } else if (config.on_conflict === 'skip') {
        return false;
      }
    }

    this.configs.set(config.agent_id, config);
    return true;
  }

  addConfigs(configs: Iterable<AgentConfig>): boolean[] {
    const results: boolean[] = [];
    for (const config of configs) {
      try {
        results.push(this.addConfig(config));
      } catch {
        results.push(false);
      }
    }
    return results;
  }

  removeConfig(agentId: string): boolean {
    return this.configs.delete(agentId);
  }

  removeAllConfigs(): void {
    this.configs.clear();
  }

  getConfig(agentId: string): AgentConfig | undefined {
    return this.configs.get(agentId);
  }

  listConfigs(): AgentConfig[] {
    return Array.from(this.configs.values());
  }

  listAgentIds(): string[] {
    return Array.from(this.configs.keys());
  }

  count(): number {
    return this.configs.size;
  }

  hasConfig(agentId: string): boolean {
    return this.configs.has(agentId);
  }

  searchConfigs(options: {
    name?: string;
    toolName?: string;
    callbackType?: string;
    hasSubAgents?: boolean;
  }): AgentConfig[] {
    let results = this.listConfigs();

    if (options.name) {
      const nameLower = options.name.toLowerCase();
      results = results.filter(
        c =>
          (c.name && c.name.toLowerCase().includes(nameLower)) ||
          c.agent_id.toLowerCase().includes(nameLower)
      );
    }

    if (options.toolName) {
      results = results.filter(c => c.tools && options.toolName! in c.tools);
    }

    if (options.callbackType) {
      results = results.filter(
        c => c.callbacks && options.callbackType! in c.callbacks && c.callbacks[options.callbackType as keyof typeof c.callbacks]
      );
    }

    if (options.hasSubAgents !== undefined) {
      if (options.hasSubAgents) {
        results = results.filter(c => c.sub_agents && c.sub_agents.length > 0);
      } else {
        results = results.filter(c => !c.sub_agents || c.sub_agents.length === 0);
      }
    }

    return results;
  }

  getAgentByTool(toolName: string): AgentConfig[] {
    return this.listConfigs().filter(c => c.tools && toolName in c.tools);
  }

  getAgentBySkill(skillName: string): AgentConfig[] {
    return this.listConfigs().filter(c => c.skills && skillName in c.skills);
  }

  getAgentByKnowledgeBase(kbName: string): AgentConfig[] {
    return this.listConfigs().filter(c => c.knowledge_bases && kbName in c.knowledge_bases);
  }

  getSubAgents(agentId: string): AgentConfig[] {
    const config = this.getConfig(agentId);
    if (!config || !config.sub_agents) {
      return [];
    }

    const subAgents: AgentConfig[] = [];
    for (const subAgentId of config.sub_agents) {
      const subConfig = this.getConfig(subAgentId);
      if (subConfig) {
        subAgents.push(subConfig);
      }
    }
    return subAgents;
  }

  updateConfig(agentId: string, updates: Partial<AgentConfig>): AgentConfig | null {
    if (!this.configs.has(agentId)) {
      return null;
    }

    const config = this.configs.get(agentId)!;
    const updatedConfig = { ...config, ...updates };
    this.configs.set(agentId, updatedConfig);
    return updatedConfig;
  }

  setConfigs(configs: Iterable<AgentConfig>): void {
    this.configs.clear();
    for (const config of configs) {
      this.addConfig(config);
    }
  }

  toDict(): Record<string, any> {
    return {
      agent_count: this.count(),
      agent_ids: this.listAgentIds(),
    };
  }
}

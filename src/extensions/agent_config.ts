export type ConflictStrategy = 'error' | 'overwrite' | 'skip';

export enum CallbackType {
    before_model_callback = 'before_model_callback',
    after_model_callback = 'after_model_callback',
    before_tool_callback = 'before_tool_callback',
    after_tool_callback = 'after_tool_callback',
    before_agent_callback = 'before_agent_callback',
    after_agent_callback = 'after_agent_callback',
}

export interface AgentConfig {
    agent_id: string;
    name?: string;
    description?: string;
    prompts?: Record<string, any>;
    tools?: Record<string, any>;
    skills?: Record<string, any>;
    callbacks?: Partial<Record<CallbackType, Array<(...args: any[]) => any>>>;
    knowledge_bases?: Record<string, any>;
    sub_agents?: string[];
    on_conflict?: ConflictStrategy;
}

export function normalizeAgentConfig(config: AgentConfig): AgentConfig {
    return {
        ...config,
        name: config.name ?? '',
        description: config.description ?? '',
        prompts: config.prompts ?? {},
        tools: config.tools ?? {},
        skills: config.skills ?? {},
        callbacks: config.callbacks ?? {},
        knowledge_bases: config.knowledge_bases ?? {},
        sub_agents: config.sub_agents ?? [],
        on_conflict: config.on_conflict ?? 'error',
    };
}


export interface StateChangeEvent {
    state: string;
    metadata?: Record<string, any>;
}

export interface StreamChunkEvent {
    content?: string;
    role?: string;
    function_call?: Record<string, any>;
    tool_calls?: Array<Record<string, any>>;
    metadata?: Record<string, any>;
}

export interface ArtifactEvent {
    url: string;
    metadata?: Record<string, any>;
}

export interface AskUserEvent {
    prompt: string;
    metadata?: Record<string, any>;
}

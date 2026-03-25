export class QueueNames {
    static session_data_stream(sessionId: string): string {
        return `byai_gateway:session:${sessionId}:data_stream`;
    }

    static ctrl_stream(capability: string): string {
        return `byai_gateway:ctrl:capability:${capability}`;
    }

    static worker_ctrl_stream(workerId: string): string {
        return `byai_gateway:ctrl:worker:${workerId}`;
    }
}

export class RegistryKeys {
    static DEFAULT_SESSION_TTL = 7 * 24 * 3600;
    static ACTIVE_WORKERS = "byai_gateway:registry:active_workers";

    static worker_capabilities(workerId: string): string {
        return `byai_gateway:registry:worker:capabilities:${workerId}`;
    }

    static capability_workers(capability: string): string {
        return `byai_gateway:registry:capability:workers:${capability}`;
    }

    static task_group(groupId: string): string {
        return `byai_gateway:task_group:${groupId}`;
    }

    static worker_lock(workerId: string): string {
        return `byai_gateway:registry:worker:lock:${workerId}`;
    }

    static session_registry(sessionId: string): string {
        return `byai_gateway:session:${sessionId}:registry`;
    }

    // Maintaining for task-specific tracking within session
    static execution_detail(executionId: string): string {
        return `byai_gateway:registry:execution:detail:${executionId}`;
    }

    static execution_by_message(messageId: string): string {
        return `byai_gateway:registry:execution:message:${messageId}`;
    }

    static session_executions(sessionId: string): string {
        return `byai_gateway:registry:session:executions:${sessionId}`;
    }
}

export class ConsumerGroups {
    static AGENT_ENGINES = "byai_gateway:consumer_group:agent_engines";
}

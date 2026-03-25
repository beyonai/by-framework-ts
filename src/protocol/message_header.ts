export interface MessageHeaderOptions {
    sourceAgentId?: string;
    targetAgentType?: string;
    parentMessageId?: string;
    taskGroupId?: string;
    tenantId?: string;
    metadata?: Record<string, any>;
}

export class MessageHeader {
    constructor(
        public readonly messageId: string,
        public readonly sessionId: string,
        public readonly traceId: string,
        options: MessageHeaderOptions = {}
    ) {
        this.sourceAgentId = options.sourceAgentId || '';
        this.targetAgentType = options.targetAgentType || '';
        this.parentMessageId = options.parentMessageId || '';
        this.taskGroupId = options.taskGroupId || '';
        this.tenantId = options.tenantId || '';
        this.metadata = options.metadata || {};
    }

    public readonly sourceAgentId: string;
    public readonly targetAgentType: string;
    public readonly parentMessageId: string;
    public readonly taskGroupId: string;
    public readonly tenantId: string;
    public readonly metadata: Record<string, any>;

    toDict(): Record<string, any> {
        return {
            message_id: this.messageId,
            session_id: this.sessionId,
            trace_id: this.traceId,
            source_agent_id: this.sourceAgentId,
            target_agent_type: this.targetAgentType,
            parent_message_id: this.parentMessageId,
            task_group_id: this.taskGroupId,
            tenant_id: this.tenantId,
            metadata: { ...this.metadata },
        };
    }

    static fromDict(data: Record<string, any>): MessageHeader {
        return new MessageHeader(data.message_id, data.session_id, data.trace_id, {
            sourceAgentId: data.source_agent_id || '',
            targetAgentType: data.target_agent_type || '',
            parentMessageId: data.parent_message_id || '',
            taskGroupId: data.task_group_id || '',
            tenantId: data.tenant_id || '',
            metadata: { ...(data.metadata || {}) },
        });
    }
}

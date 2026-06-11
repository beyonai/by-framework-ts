export interface MessageHeaderOptions {
    sourceAgentType?: string;
    targetAgentType?: string;
    parentMessageId?: string;
    taskGroupId?: string;
    userCode?: string;
    userName?: string;
    metadata?: Record<string, any>;
    /** Hex-encoded parent span ID for OTel / Phoenix trace propagation. */
    traceParentSpanId?: string;
    /** Parent observation ID for Langfuse trace nesting. */
    langfuseParentObservationId?: string;
}

export class MessageHeader {
    public readonly sourceAgentType: string;
    public readonly targetAgentType: string;
    public readonly parentMessageId: string;
    public readonly taskGroupId: string;
    public readonly userCode: string;
    public readonly userName: string;
    public readonly metadata: Record<string, any>;
    public readonly traceParentSpanId: string;
    public readonly langfuseParentObservationId: string;

    constructor(
        public readonly messageId: string,
        public readonly sessionId: string,
        public readonly traceId: string,
        options: MessageHeaderOptions = {}
    ) {
        this.sourceAgentType             = options.sourceAgentType || '';
        this.targetAgentType             = options.targetAgentType || '';
        this.parentMessageId             = options.parentMessageId || '';
        this.taskGroupId                 = options.taskGroupId || '';
        this.userCode                    = options.userCode || '';
        this.userName                    = options.userName || '';
        this.metadata                    = options.metadata || {};
        this.traceParentSpanId           = options.traceParentSpanId || '';
        this.langfuseParentObservationId = options.langfuseParentObservationId || '';
    }

    toDict(): Record<string, any> {
        return {
            message_id:                      this.messageId,
            session_id:                      this.sessionId,
            trace_id:                        this.traceId,
            source_agent_type:               this.sourceAgentType,
            target_agent_type:               this.targetAgentType,
            parent_message_id:               this.parentMessageId,
            task_group_id:                   this.taskGroupId,
            user_code:                       this.userCode,
            user_name:                       this.userName,
            metadata:                        { ...this.metadata },
            trace_parent_span_id:            this.traceParentSpanId,
            langfuse_parent_observation_id:  this.langfuseParentObservationId,
        };
    }

    static fromDict(data: Record<string, any>): MessageHeader {
        return new MessageHeader(
            data.message_id,
            data.session_id,
            data.trace_id,
            {
                // Support both old key (source_agent_id) and correct key (source_agent_type)
                sourceAgentType:             data.source_agent_type || data.source_agent_id || '',
                targetAgentType:             data.target_agent_type || '',
                parentMessageId:             data.parent_message_id || '',
                taskGroupId:                 data.task_group_id || '',
                userCode:                    data.user_code || '',
                userName:                    data.user_name || '',
                metadata:                    { ...(data.metadata || {}) },
                traceParentSpanId:           data.trace_parent_span_id || '',
                langfuseParentObservationId: data.langfuse_parent_observation_id || '',
            }
        );
    }
}

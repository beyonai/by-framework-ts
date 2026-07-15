import { v4 as uuidv4 } from 'uuid';
import { QueueNames } from '../constants';
import { AskAgentCommand } from '../protocol/commands';
import { MessageHeader } from '../protocol/message_header';
import type { AskAgentQueueNames } from './ports';
import type { CallAgentPublishInput, AskAgentPublishArtifacts, ExecutionSourceAgentFallback } from './types';

export function generateAskAgentMessageId(): string {
    return `msg-${uuidv4().slice(0, 8)}`;
}

export function generateExecutionId(): string {
    return `exec-${uuidv4().slice(0, 8)}`;
}

export function retargetAskAgentCommand(command: AskAgentCommand, targetAgentType: string): AskAgentCommand {
    const header = command.header;
    return new AskAgentCommand(
        new MessageHeader(header.messageId, header.sessionId, header.traceId, {
            sourceAgentType: header.sourceAgentType, targetAgentType,
            parentMessageId: header.parentMessageId, taskGroupId: header.taskGroupId,
            userCode: header.userCode, userName: header.userName, metadata: header.metadata,
            traceParentSpanId: header.traceParentSpanId,
            langfuseParentObservationId: header.langfuseParentObservationId,
        }),
        command.content, command.waitForReply, command.extraPayload
    );
}

/**
 * Build command + ctrl stream + execution record for one AskAgent publish.
 */
export function buildAskAgentPublishArtifacts(
    input: CallAgentPublishInput,
    messageId: string,
    parentMessageId: string,
    waitForReply: boolean,
    queueNames: AskAgentQueueNames = QueueNames
): AskAgentPublishArtifacts {
    const mergedPayload: Record<string, unknown> = { ...(input.extraPayload || {}) };
    if (waitForReply) {
        mergedPayload.wait_for_reply = true;
    }

    const command = new AskAgentCommand(
        new MessageHeader(messageId, input.sessionId, input.traceId, {
            sourceAgentType: waitForReply ? input.sourceAgentType : '',
            targetAgentType: input.targetAgentType,
            parentMessageId,
            taskGroupId: input.taskGroupId,
            userCode: input.userCode,
            userName: input.userName,
            metadata: input.metadata,
            langfuseParentObservationId: input.langfuseParentObservationId || '',
        }),
        input.content,
        waitForReply,
        Object.fromEntries(Object.entries(mergedPayload).filter(([key]) => key !== 'wait_for_reply'))
    );

    const ctrlStreamName = queueNames.ctrl_stream(input.targetAgentType);
    const executionId = generateExecutionId();
    const executionRecord = buildExecutionRecordForAskAgentCommand(
        command,
        ctrlStreamName,
        executionId,
        'none'
    );

    return {
        command,
        ctrlStreamName,
        messageId,
        parentMessageId,
        waitForReply,
        executionRecord,
    };
}

export function buildExecutionRecordForAskAgentCommand(
    command: AskAgentCommand,
    streamName: string,
    executionId: string,
    sourceAgentFallback: ExecutionSourceAgentFallback
): Record<string, unknown> {
    const header = command.header;
    const source =
        sourceAgentFallback === 'client'
            ? (header.sourceAgentType || 'client')
            : header.sourceAgentType;
    return {
        execution_id: executionId,
        message_id: header.messageId,
        parent_message_id: header.parentMessageId || '',
        session_id: header.sessionId,
        trace_id: header.traceId,
        source_agent_type: source,
        stream_name: streamName,
        worker_id: '',
        target_agent_type: header.targetAgentType,
        status: 'QUEUED',
        cancel_requested: false,
        cancel_reason: '',
    };
}

/**
 * Resolve messageId / parentMessageId / waitForReply from publish input (same defaults as AgentContext.callAgent).
 */
export function resolveCallAgentPublishIds(input: CallAgentPublishInput): {
    messageId: string;
    parentMessageId: string;
    waitForReply: boolean;
} {
    const messageId = input.messageId || generateAskAgentMessageId();
    const parentMessageId = input.parentMessageId || input.defaultParentMessageId;
    const waitForReply = input.waitForReply ?? true;
    return { messageId, parentMessageId, waitForReply };
}

import { ActionType } from '../src/protocol/action_type';
import {
    AskAgentCommand,
    BaseCommand,
    CancelTaskCommand,
    MessageHeader,
    ResumeCommand,
    commandFromDict,
    registerCommand,
    unregisterCommand,
} from '../src';

class CustomCommand extends BaseCommand {
    static actionType = 'CUSTOM_COMMAND';
    readonly actionType = 'CUSTOM_COMMAND';

    constructor(
        public readonly header: MessageHeader,
        public readonly data: Record<string, string>
    ) {
        super(header);
    }

    toDict(): Record<string, any> {
        return {
            action_type: this.actionType,
            header: this.header.toDict(),
            body: { data: { ...this.data } },
        };
    }

    static fromDict(data: Record<string, any>): CustomCommand {
        return new CustomCommand(
            MessageHeader.fromDict(data.header),
            { ...(data.body?.data || {}) }
        );
    }
}

describe('command wire format', () => {
    test('serializes ask agent command as action_type + header + body', () => {
        const command = new AskAgentCommand(
            new MessageHeader('msg-1', 'sess-1', 'trace-1', {
                targetAgentType: 'demo-agent-ts',
                userCode: 'tenant-1',
            }),
            'hello world',
            false,
            { attachments: [] }
        );

        expect(command.toDict()).toEqual({
            action_type: ActionType.ASK_AGENT,
            header: {
                message_id: 'msg-1',
                session_id: 'sess-1',
                trace_id: 'trace-1',
                source_agent_type: '',
                target_agent_type: 'demo-agent-ts',
                parent_message_id: '',
                task_group_id: '',
                user_code: 'tenant-1',
                user_name: '',
                metadata: {},
                trace_parent_span_id: '',
                langfuse_parent_observation_id: '',
            },
            body: {
                content: 'hello world',
                wait_for_reply: false,
                extra_payload: { attachments: [] },
            },
        });
    });

    test('decodes ask agent command with object content', () => {
        const command = commandFromDict({
            action_type: ActionType.ASK_AGENT,
            header: {
                message_id: 'msg-1',
                session_id: 'sess-1',
                trace_id: 'trace-1',
                source_agent_type: '',
                target_agent_type: 'demo-agent-ts',
                parent_message_id: '',
                task_group_id: '',
                user_code: 'tenant-1',
                user_name: '',
                metadata: {},
                trace_parent_span_id: '',
                langfuse_parent_observation_id: '',
            },
            body: {
                content: {
                    role: 'user',
                    content: { text: '你好', files: [] }
                },
                wait_for_reply: false,
            },
        });

        expect(command).toBeInstanceOf(AskAgentCommand);
        expect((command as AskAgentCommand).content).toEqual({
            role: 'user',
            content: { text: '你好', files: [] }
        });
    });

    test('decodes resume command from wire payload', () => {
        const command = commandFromDict({
            action_type: ActionType.RESUME,
            header: {
                message_id: 'msg-2',
                session_id: 'sess-2',
                trace_id: 'trace-2',
                source_agent_id: 'agent-a',
                target_agent_type: 'agent-b',
                parent_message_id: 'msg-1',
                user_code: '',
                user_name: '',
                metadata: {},
            },
            body: {
                content: '',
                status: 'SUCCESS',
                reply_data: { ok: true },
            },
        });

        expect(command).toBeInstanceOf(ResumeCommand);
        const resumeCommand = command as ResumeCommand;
        expect(resumeCommand.header.parentMessageId).toBe('msg-1');
        expect(resumeCommand.status).toBe('SUCCESS');
        expect(resumeCommand.replyData).toEqual({ ok: true });
    });

    test('decodes cancel task command from wire payload', () => {
        const command = commandFromDict({
            action_type: ActionType.CANCEL_TASK,
            header: {
                message_id: 'msg-cancel-1',
                session_id: 'sess-3',
                trace_id: 'trace-3',
                source_agent_id: '',
                target_agent_type: 'demo-agent-ts',
                parent_message_id: 'msg-task-1',
                user_code: '',
                user_name: '',
                metadata: {},
            },
            body: {
                target_message_id: 'msg-task-1',
                target_execution_id: 'exec-1',
                target_worker_id: 'worker-1',
                reason: 'user aborted',
                requested_by: 'frontend',
                cancel_mode: 'graceful',
            },
        });

        expect(command).toBeInstanceOf(CancelTaskCommand);
        const cancelCommand = command as CancelTaskCommand;
        expect(cancelCommand.targetMessageId).toBe('msg-task-1');
        expect(cancelCommand.targetExecutionId).toBe('exec-1');
        expect(cancelCommand.targetWorkerId).toBe('worker-1');
        expect(cancelCommand.reason).toBe('user aborted');
    });

    test('supports registered custom command decoding', () => {
        registerCommand(CustomCommand);
        try {
            const command = commandFromDict({
                action_type: 'CUSTOM_COMMAND',
                header: {
                    message_id: 'custom-1',
                    session_id: 'sess-custom',
                    trace_id: 'trace-custom',
                    source_agent_id: '',
                    target_agent_type: 'custom-agent',
                    parent_message_id: '',
                    user_code: '',
                    user_name: '',
                    metadata: {},
                },
                body: {
                    data: { mode: 'custom' },
                },
            });

            expect(command).toBeInstanceOf(CustomCommand);
            expect((command as CustomCommand).data).toEqual({ mode: 'custom' });
        } finally {
            unregisterCommand(CustomCommand.actionType);
        }
    });
});

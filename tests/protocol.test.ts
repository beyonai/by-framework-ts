import { ActionType } from '../src/protocol/action_type';
import { AgentState } from '../src/protocol/agent_state';
import { EventType } from '../src/protocol/event_type';
import { SseMessageType, SseReasonMessageType } from '../src/protocol/content_type';
import { QueueNames, RegistryKeys, ConsumerGroups } from '../src/constants';
import { MessageHeader } from '../src/protocol/message_header';
import {
    AskAgentCommand,
    ResumeCommand,
    CancelTaskCommand,
    registerCommand,
    unregisterCommand,
    commandFromDict,
    BaseCommand,
} from '../src/protocol/commands';

/**
 * Protocol 层全面测试，对标 Python test_protocol.py
 */
describe('Protocol layer', () => {
    describe('ActionType enum', () => {
        test('contains ASK_AGENT, RESUME, CANCEL_TASK, ASK_USER', () => {
            expect(ActionType.ASK_AGENT).toBe('ASK_AGENT');
            expect(ActionType.RESUME).toBe('RESUME');
            expect(ActionType.CANCEL_TASK).toBe('CANCEL_TASK');
            expect(ActionType.ASK_USER).toBe('ASK_USER');
        });
    });

    describe('AgentState enum', () => {
        test('contains all expected states including CANCELLING and CANCELLED', () => {
            expect(AgentState.STARTING).toBe('STARTING');
            expect(AgentState.CANCELLING).toBe('CANCELLING');
            expect(AgentState.CANCELLED).toBe('CANCELLED');
            expect(AgentState.COMPLETED).toBe('COMPLETED');
            expect(AgentState.FAILED).toBe('FAILED');
            expect(AgentState.RESUMED).toBe('RESUMED');
            expect(AgentState.WAITING_AGENT).toBe('WAITING_AGENT');
            expect(AgentState.WAITING_USER).toBe('WAITING_USER');
            expect(AgentState.QUEUED).toBe('QUEUED');
            expect(AgentState.CALLING_AGENT).toBe('CALLING_AGENT');
        });
    });

    describe('EventType enum', () => {
        test('contains answerDelta and reasoningLogDelta', () => {
            expect(EventType.ANSWER_DELTA).toBe('answerDelta');
            expect(EventType.REASONING_LOG_DELTA).toBe('reasoningLogDelta');
            expect(EventType.APP_STREAM_RESPONSE).toBe('appStreamResponse');
        });
    });

    describe('SseMessageType / SseReasonMessageType', () => {
        test('text content type is 1002', () => {
            expect(SseMessageType.text).toBe('1002');
        });

        test('think_title content type is 3003', () => {
            expect(SseReasonMessageType.think_title).toBe('3003');
        });
    });

    describe('Constants - QueueNames', () => {
        test('session_data_stream includes sessionId', () => {
            expect(QueueNames.session_data_stream('sess-1')).toContain('sess-1');
        });

        test('ctrl_stream includes capability', () => {
            expect(QueueNames.ctrl_stream('agent-x')).toContain('agent-x');
        });

        test('worker_ctrl_stream includes workerId', () => {
            expect(QueueNames.worker_ctrl_stream('w-1')).toContain('w-1');
        });
    });

    describe('Constants - RegistryKeys', () => {
        test('ACTIVE_WORKERS is a known constant', () => {
            expect(RegistryKeys.ACTIVE_WORKERS).toBeTruthy();
        });

        test('worker_capabilities includes workerId', () => {
            expect(RegistryKeys.worker_capabilities('w-1')).toContain('w-1');
        });

        test('capability_workers includes capability', () => {
            expect(RegistryKeys.capability_workers('agent-x')).toContain('agent-x');
        });

        test('task_group includes groupId', () => {
            expect(RegistryKeys.task_group('grp-1')).toContain('grp-1');
        });

        test('execution_detail includes executionId', () => {
            expect(RegistryKeys.execution_detail('exec-1')).toContain('exec-1');
        });

        test('execution_by_message includes messageId', () => {
            expect(RegistryKeys.execution_by_message('msg-1')).toContain('msg-1');
        });

        test('session_executions includes sessionId', () => {
            expect(RegistryKeys.session_executions('sess-1')).toContain('sess-1');
        });
    });

    describe('Constants - ConsumerGroups', () => {
        test('AGENT_ENGINES is defined', () => {
            expect(ConsumerGroups.AGENT_ENGINES).toBeTruthy();
        });
    });

    describe('MessageHeader', () => {
        test('toDict produces wire format with snake_case keys', () => {
            const header = new MessageHeader('msg-1', 'sess-1', 'trace-1', {
                sourceAgentId: 'src-agent',
                targetAgentType: 'tgt-agent',
                parentMessageId: 'parent-1',
                taskGroupId: 'grp-1',
                tenantId: 'tenant-1',
                metadata: { key: 'value' },
            });

            const dict = header.toDict();
            expect(dict.message_id).toBe('msg-1');
            expect(dict.session_id).toBe('sess-1');
            expect(dict.trace_id).toBe('trace-1');
            expect(dict.source_agent_id).toBe('src-agent');
            expect(dict.target_agent_type).toBe('tgt-agent');
            expect(dict.parent_message_id).toBe('parent-1');
            expect(dict.task_group_id).toBe('grp-1');
            expect(dict.tenant_id).toBe('tenant-1');
            expect(dict.metadata).toEqual({ key: 'value' });
        });

        test('fromDict round trips correctly', () => {
            const original = new MessageHeader('msg-1', 'sess-1', 'trace-1', {
                sourceAgentId: 'src',
                targetAgentType: 'tgt',
                metadata: { x: 1 },
            });

            const restored = MessageHeader.fromDict(original.toDict());
            expect(restored.messageId).toBe('msg-1');
            expect(restored.sessionId).toBe('sess-1');
            expect(restored.traceId).toBe('trace-1');
            expect(restored.sourceAgentId).toBe('src');
            expect(restored.targetAgentType).toBe('tgt');
            expect(restored.metadata).toEqual({ x: 1 });
        });
    });

    describe('AskAgentCommand validation', () => {
        test('requires non-empty content', () => {
            expect(() => {
                new AskAgentCommand(
                    new MessageHeader('m', 's', 't'),
                    ''
                );
            }).toThrow('non-empty content');
        });

        test('accepts array content', () => {
            const cmd = new AskAgentCommand(
                new MessageHeader('m', 's', 't'),
                [{ role: 'user', content: 'hi' }]
            );
            expect(cmd.content).toHaveLength(1);
        });
    });

    describe('ResumeCommand validation', () => {
        test('requires status or content', () => {
            expect(() => {
                new ResumeCommand(new MessageHeader('m', 's', 't'), '', '');
            }).toThrow('status or content');
        });

        test('accepts status without content', () => {
            const cmd = new ResumeCommand(
                new MessageHeader('m', 's', 't'),
                '',
                'COMPLETED'
            );
            expect(cmd.status).toBe('COMPLETED');
        });
    });

    describe('CancelTaskCommand validation', () => {
        test('requires targetMessageId', () => {
            expect(() => {
                new CancelTaskCommand(new MessageHeader('m', 's', 't'), '');
            }).toThrow('targetMessageId');
        });

        test('serializes to header + body wire format', () => {
            const cmd = new CancelTaskCommand(
                new MessageHeader('msg-1', 'sess-1', 'trace-1'),
                'target-msg-1',
                'exec-1',
                'worker-1',
                'user requested',
                'admin',
                'graceful'
            );

            const dict = cmd.toDict();
            expect(dict.action_type).toBe(ActionType.CANCEL_TASK);
            const body = dict.body as { target_message_id: string; target_execution_id: string; target_worker_id: string; reason: string; requested_by: string; cancel_mode: string };
            expect(body.target_message_id).toBe('target-msg-1');
            expect(body.target_execution_id).toBe('exec-1');
            expect(body.target_worker_id).toBe('worker-1');
            expect(body.reason).toBe('user requested');
            expect(body.requested_by).toBe('admin');
            expect(body.cancel_mode).toBe('graceful');
        });
    });

    describe('Command round-trip via commandFromDict', () => {
        test('AskAgentCommand round trips from wire dict', () => {
            const original = new AskAgentCommand(
                new MessageHeader('msg-1', 'sess-1', 'trace-1', {
                    targetAgentType: 'agent-a',
                    sourceAgentId: 'agent-b',
                }),
                'hello',
                true,
                { key: 'val' }
            );

            const decoded = commandFromDict(original.toDict());
            expect(decoded).toBeInstanceOf(AskAgentCommand);
            const ask = decoded as AskAgentCommand;
            expect(ask.content).toBe('hello');
            expect(ask.waitForReply).toBe(true);
            expect(ask.extraPayload.key).toBe('val');
            expect(ask.header.targetAgentType).toBe('agent-a');
        });

        test('ResumeCommand round trips from wire dict', () => {
            const original = new ResumeCommand(
                new MessageHeader('msg-2', 'sess-2', 'trace-2', {
                    parentMessageId: 'parent-1',
                }),
                'reply content',
                'SUCCESS',
                { answer: 42 }
            );

            const decoded = commandFromDict(original.toDict());
            expect(decoded).toBeInstanceOf(ResumeCommand);
            const resume = decoded as ResumeCommand;
            expect(resume.status).toBe('SUCCESS');
            expect((resume.replyData as { answer: number }).answer).toBe(42);
            expect(resume.header.parentMessageId).toBe('parent-1');
        });

        test('CancelTaskCommand round trips from wire dict', () => {
            const original = new CancelTaskCommand(
                new MessageHeader('msg-3', 'sess-3', 'trace-3'),
                'target-msg',
                'exec-id',
                'worker-id',
                'timeout'
            );

            const decoded = commandFromDict(original.toDict());
            expect(decoded).toBeInstanceOf(CancelTaskCommand);
            const cancel = decoded as CancelTaskCommand;
            expect(cancel.targetMessageId).toBe('target-msg');
            expect(cancel.reason).toBe('timeout');
        });
    });

    describe('toRedisPayload serialization', () => {
        test('returns data field as JSON string', () => {
            const cmd = new AskAgentCommand(
                new MessageHeader('msg-1', 'sess-1', 'trace-1'),
                'test'
            );

            const payload = cmd.toRedisPayload();
            expect(typeof payload.data).toBe('string');
            const parsed = JSON.parse(payload.data);
            expect(parsed.action_type).toBe(ActionType.ASK_AGENT);
        });
    });

    describe('Custom command registration', () => {
        class CustomCommand extends BaseCommand {
            static actionType = 'CUSTOM_ACTION';
            readonly actionType = 'CUSTOM_ACTION';

            constructor(header: MessageHeader, public readonly value: string) {
                super(header);
            }

            toDict(): Record<string, any> {
                return {
                    action_type: this.actionType,
                    header: this.header.toDict(),
                    body: { value: this.value },
                };
            }

            static fromDict(data: Record<string, any>): CustomCommand {
                return new CustomCommand(
                    MessageHeader.fromDict(data.header),
                    data.body?.value || ''
                );
            }
        }

        afterEach(() => {
            unregisterCommand('CUSTOM_ACTION');
        });

        test('registerCommand enables decoding of custom commands', () => {
            registerCommand(CustomCommand);

            const decoded = commandFromDict({
                action_type: 'CUSTOM_ACTION',
                header: { message_id: 'm', session_id: 's', trace_id: 't' },
                body: { value: 'custom-data' },
            });

            expect(decoded).toBeInstanceOf(CustomCommand);
            expect((decoded as CustomCommand).value).toBe('custom-data');
        });

        test('unregisterCommand prevents decoding', () => {
            registerCommand(CustomCommand);
            unregisterCommand('CUSTOM_ACTION');

            expect(() => {
                commandFromDict({
                    action_type: 'CUSTOM_ACTION',
                    header: { message_id: 'm', session_id: 's', trace_id: 't' },
                    body: {},
                });
            }).toThrow('Unsupported action_type');
        });
    });
});

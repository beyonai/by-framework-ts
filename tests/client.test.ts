import { GatewayClient } from '../src/client';
import { BaseCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { BaiYingMessageRole } from '../src/protocol/message';
import { ActionType } from '../src/protocol/action_type';

class CustomCommand extends BaseCommand {
    static actionType = 'CUSTOM_CLIENT';
    readonly actionType = 'CUSTOM_CLIENT';

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

    static fromDict(_data: Record<string, any>): CustomCommand {
        throw new Error('unused');
    }
}

// 模拟依赖
jest.mock('../src/redis_client', () => ({
    getRedis: jest.fn().mockReturnValue({
        xadd: jest.fn().mockResolvedValue('test-msg-id'),
    }),
}));

jest.mock('../src/registry', () => {
    return {
        WorkerRegistry: jest.fn().mockImplementation(() => ({
            getTargetWorker: jest.fn().mockResolvedValue('worker-123'),
            hasAgentType: jest.fn().mockResolvedValue([true, ['worker-123']]),
            hasOnlineAgentType: jest.fn().mockResolvedValue([true, ['worker-123']]),
            isWorkerAlive: jest.fn().mockResolvedValue(true),
            isWorkerOnline: jest.fn().mockResolvedValue(true),
            getExecutionByMessageId: jest.fn().mockResolvedValue(null),
            markExecutionCancelling: jest.fn().mockResolvedValue(undefined),
            saveExecution: jest.fn().mockResolvedValue(undefined),
        })),
    };
});

describe('GatewayClient', () => {
    let client: GatewayClient;
    const mockRedis = require('../src/redis_client').getRedis();

    beforeEach(() => {
        jest.clearAllMocks();
        client = new GatewayClient();
        // 强制替换私有属性中的 redis 为我们的 mock
        (client as any).redis = mockRedis;
    });

    describe('Complex Messages', () => {
        it('should queue a plain string message using targetAgentType', async () => {
            const response = await client.sendMessage({
                targetAgentType: 'demo-agent-ts',
                sessionId: 'test-session-ts',
                content: 'Hello from verification script!',
                tenantId: 'test-tenant'
            });

            expect(response.success).toBe(true);
            expect(response.status).toBe('QUEUED');
            expect(response.target_worker_id).toBe('worker-123');

            const callArgs = mockRedis.xadd.mock.calls[0];
            const serializedMsg = JSON.parse(callArgs[3]);

            expect(serializedMsg.action_type).toBe(ActionType.ASK_AGENT);
            expect(serializedMsg.header.target_agent_type).toBe('demo-agent-ts');
            expect(serializedMsg.header.session_id).toBe('test-session-ts');
            expect(serializedMsg.header.tenant_id).toBe('test-tenant');
            expect(serializedMsg.body.content).toBe('Hello from verification script!');
            expect(serializedMsg.body.wait_for_reply).toBe(false);
            expect((client as any).registry.saveExecution).toHaveBeenCalledWith(
                expect.objectContaining({
                    message_id: response.message_id,
                    session_id: 'test-session-ts',
                    target_agent_type: 'demo-agent-ts',
                    status: 'QUEUED',
                    worker_id: '',
                    cancel_requested: false,
                })
            );
        });

        it('should correctly serialize complex message list', async () => {
            const complexMessages = [
                {
                    role: BaiYingMessageRole.USER,
                    content: "hello world"
                },
                {
                    role: BaiYingMessageRole.ASSISTANT,
                    content: {
                        text: "这是一个包含文件的回复",
                        files: [
                            {
                                fileId: 101,
                                fileUrl: "http://example.com/asset.png",
                                fileType: "image" as const,
                                fileName: "asset.png"
                            }
                        ]
                    }
                }
            ];

            const response = await client.sendMessage({
                targetAgentType: 'agent-b',
                sessionId: 'session-demo',
                content: complexMessages,
                actionType: ActionType.ASK_AGENT
            });

            expect(response.success).toBe(true);
            expect(mockRedis.xadd).toHaveBeenCalled();

            // Extract the JSON string passed to xadd (argument index 3: xadd(stream, '*', 'data', jsonStr))
            const callArgs = mockRedis.xadd.mock.calls[0];
            const serializedMsg = JSON.parse(callArgs[3]);

            expect(serializedMsg.action_type).toBe(ActionType.ASK_AGENT);
            expect(serializedMsg.body.content).toBeInstanceOf(Array);
            expect(serializedMsg.body.content.length).toBe(2);
            expect(serializedMsg.body.content[0].role).toBe('user');
            expect(serializedMsg.body.content[0].content).toBe('hello world');

            expect(serializedMsg.body.content[1].role).toBe('assistant');
            expect(serializedMsg.body.content[1].content.text).toBe('这是一个包含文件的回复');
            expect(serializedMsg.body.content[1].content.files[0].fileName).toBe('asset.png');
        });

        it('should bypass serialization if content is pure dictionary without strict structure', async () => {
            const rawDictMessages = [
                { role: 'user', content: 'raw dict text' },
                { unknown_key: 'bypassed object', content: undefined }
            ];

            const response = await client.sendMessage({
                targetAgentType: 'agent-b',
                sessionId: 'session-demo',
                content: rawDictMessages as any,
                actionType: ActionType.ASK_AGENT
            });

            expect(response.success).toBe(true);
            const callArgs = mockRedis.xadd.mock.calls[0];
            const serializedMsg = JSON.parse(callArgs[3]);

            expect(serializedMsg.body.content[0].role).toBe('user');
            expect(serializedMsg.body.content[0].content).toBe('raw dict text');
            expect(serializedMsg.body.content[1].unknown_key).toBe('bypassed object');
        });

        it('should send a custom command directly', async () => {
            const command = new CustomCommand(
                new MessageHeader('custom-1', 'sess-custom', 'trace-custom', {
                    targetAgentType: 'demo-agent-ts',
                }),
                { mode: 'custom' }
            );

            const response = await client.sendCommand(command);

            expect(response.success).toBe(true);
            const callArgs = mockRedis.xadd.mock.calls[0];
            const serializedMsg = JSON.parse(callArgs[3]);
            expect(serializedMsg.action_type).toBe('CUSTOM_CLIENT');
            expect(serializedMsg.body.data).toEqual({ mode: 'custom' });
        });

        it('should return NOT_FOUND when cancel target execution does not exist', async () => {
            const response = await client.cancelTask({
                messageId: 'missing-msg',
                sessionId: 'sess-1',
            });

            expect(response.success).toBe(false);
            expect(response.status).toBe('NOT_FOUND');
            expect(response.message_id).toBe('missing-msg');
            expect(response.execution_id).toBe('');
            expect(response.worker_id).toBe('');
        });

        it('should return ALREADY_FINISHED when execution is already terminal', async () => {
            const mockRegistry = (client as any).registry;
            mockRegistry.getExecutionByMessageId.mockResolvedValue({
                execution_id: 'exec-1',
                worker_id: 'worker-123',
                session_id: 'sess-1',
                status: 'CANCELLED',
            });

            const response = await client.cancelTask({
                messageId: 'msg-1',
                sessionId: 'sess-1',
            });

            expect(response.success).toBe(false);
            expect(response.status).toBe('ALREADY_FINISHED');
            expect(response.execution_id).toBe('exec-1');
            expect(response.worker_id).toBe('worker-123');
            expect(mockRedis.xadd).not.toHaveBeenCalled();
        });

        it('should route cancel task command to worker control stream', async () => {
            const mockRegistry = (client as any).registry;
            mockRegistry.getExecutionByMessageId.mockResolvedValue({
                execution_id: 'exec-1',
                worker_id: 'worker-123',
                session_id: 'sess-1',
                target_agent_type: 'demo-agent-ts',
                status: 'RUNNING',
            });

            const response = await client.cancelTask({
                messageId: 'msg-1',
                sessionId: 'sess-1',
                reason: 'user aborted',
                requestedBy: 'frontend',
                cancelMode: 'graceful',
            });

            expect(response.success).toBe(true);
            expect(response.status).toBe('CANCEL_REQUESTED');
            expect(response.execution_id).toBe('exec-1');
            expect(response.worker_id).toBe('worker-123');
            expect(mockRegistry.markExecutionCancelling).toHaveBeenCalledWith('exec-1', 'sess-1', 'user aborted');

            const callArgs = mockRedis.xadd.mock.calls[0];
            expect(callArgs[0]).toBe('byai_gateway:ctrl:worker:worker-123');

            const serializedMsg = JSON.parse(callArgs[3]);
            expect(serializedMsg.action_type).toBe(ActionType.CANCEL_TASK);
            expect(serializedMsg.header.parent_message_id).toBe('msg-1');
            expect(serializedMsg.body.target_message_id).toBe('msg-1');
            expect(serializedMsg.body.target_execution_id).toBe('exec-1');
            expect(serializedMsg.body.target_worker_id).toBe('worker-123');
            expect(serializedMsg.body.reason).toBe('user aborted');
            expect(serializedMsg.body.requested_by).toBe('frontend');
            expect(serializedMsg.body.cancel_mode).toBe('graceful');
        });

        it('should mark queued execution as cancelling even when worker has not claimed it yet', async () => {
            const mockRegistry = (client as any).registry;
            mockRegistry.getExecutionByMessageId.mockResolvedValue({
                execution_id: 'exec-queued',
                worker_id: '',
                session_id: 'sess-1',
                target_agent_type: 'demo-agent-ts',
                status: 'QUEUED',
            });

            const response = await client.cancelTask({
                messageId: 'msg-queued',
                sessionId: 'sess-1',
                reason: 'user aborted',
            });

            expect(response.success).toBe(true);
            expect(response.status).toBe('CANCEL_REQUESTED');
            expect(response.execution_id).toBe('exec-queued');
            expect(response.worker_id).toBe('');
            expect(mockRegistry.markExecutionCancelling).toHaveBeenCalledWith('exec-queued', 'sess-1', 'user aborted');
            expect(mockRedis.xadd).not.toHaveBeenCalled();
        });
    });
});

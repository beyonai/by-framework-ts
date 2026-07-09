import { GatewayClient } from '../src/client';
import { BaseCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { BaiYingMessageRole } from '../src/protocol/message';
import { ActionType } from '../src/protocol/action_type';
import { spanIdHex } from '../src/trace/span_recorder';

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
            isWorkerOnline: jest.fn().mockResolvedValue(true),
            getExecutionByMessageId: jest.fn().mockResolvedValue(null),
            getAllSessionExecutions: jest.fn().mockResolvedValue([]),
            markExecutionCancelling: jest.fn().mockResolvedValue(undefined),
            markCancelRequested: jest.fn().mockResolvedValue(undefined),
            saveExecution: jest.fn().mockResolvedValue(undefined),
            initializeExecution: jest.fn().mockResolvedValue(undefined),
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
                userCode: 'test-tenant'
            });

            expect(response.success).toBe(true);
            expect(response.status).toBe('QUEUED');
            expect(response.target_worker_id).toBe('');
            expect((client as any).registry.getTargetWorker).not.toHaveBeenCalled();

            const callArgs = mockRedis.xadd.mock.calls[0];
            expect(callArgs[0]).toBe('byai_gateway:ctrl:agent_type:demo-agent-ts');
            const serializedMsg = JSON.parse(callArgs[3]);

            expect(serializedMsg.action_type).toBe(ActionType.ASK_AGENT);
            expect(serializedMsg.header.target_agent_type).toBe('demo-agent-ts');
            expect(serializedMsg.header.session_id).toBe('test-session-ts');
            expect(serializedMsg.header.user_code).toBe('test-tenant');
            expect(serializedMsg.header.user_name).toBe('');
            expect(serializedMsg.body.content).toBe('Hello from verification script!');
            expect(serializedMsg.body.wait_for_reply).toBe(false);
            expect((client as any).registry.initializeExecution).toHaveBeenCalledWith(
                expect.objectContaining({
                    message_id: response.message_id,
                    session_id: 'test-session-ts',
                    trace_id: response.trace_id,
                    source_agent_type: 'client',
                    target_agent_type: 'demo-agent-ts',
                    stream_name: 'byai_gateway:ctrl:agent_type:demo-agent-ts',
                    status: 'QUEUED',
                    worker_id: '',
                    cancel_requested: false,
                })
            );
            expect((client as any).registry.initializeExecution.mock.invocationCallOrder[0]).toBeLessThan(
                mockRedis.xadd.mock.invocationCallOrder[0]
            );
        });

        it('should queue directly to worker stream when targetWorkerId is provided', async () => {
            const response = await client.sendMessage({
                targetAgentType: 'demo-agent-ts',
                targetWorkerId: 'worker-123',
                sessionId: 'test-session-ts',
                content: 'direct worker message',
            });

            expect(response.success).toBe(true);
            expect(response.status).toBe('QUEUED');
            expect(response.target_worker_id).toBe('worker-123');
            expect((client as any).registry.isWorkerOnline).toHaveBeenCalledWith('worker-123');
            expect((client as any).registry.hasOnlineAgentType).not.toHaveBeenCalled();

            const callArgs = mockRedis.xadd.mock.calls[0];
            expect(callArgs[0]).toBe('byai_gateway:ctrl:worker:worker-123');
            const serializedMsg = JSON.parse(callArgs[3]);
            expect(serializedMsg.header.target_agent_type).toBe('demo-agent-ts');
        });

        it('should write client dispatch trace parents to the header', async () => {
            const observation = {
                id: 'obs-client-dispatch',
                end: jest.fn(),
                update: jest.fn(),
            };
            const spanRecorder = { recordSpan: jest.fn().mockResolvedValue(undefined) };
            const dispatchFn = jest.fn().mockReturnValue(observation);
            const tracedClient = new GatewayClient(
                undefined,
                mockRedis as any,
                undefined,
                spanRecorder as any,
                dispatchFn
            );

            await tracedClient.sendMessage({
                targetAgentType: 'demo-agent-ts',
                targetWorkerId: 'worker-123',
                sessionId: 'test-session-ts',
                content: 'direct worker message',
                messageId: 'msg-client',
                traceId: 'trace-client',
                metadata: { request_id: 'req-1' },
            });

            const callArgs = mockRedis.xadd.mock.calls[0];
            const serializedMsg = JSON.parse(callArgs[3]);

            expect(serializedMsg.header.metadata).toEqual({ request_id: 'req-1' });
            expect(serializedMsg.header.trace_parent_span_id).toBe(
                spanIdHex('msg-client:client.dispatch')
            );
            expect(serializedMsg.header.langfuse_parent_observation_id).toBe('obs-client-dispatch');
            expect(dispatchFn).toHaveBeenCalledWith(expect.objectContaining({
                traceId: 'trace-client',
                messageId: 'msg-client',
                targetAgentType: 'demo-agent-ts',
                sessionId: 'test-session-ts',
                content: 'direct worker message',
                metadata: { request_id: 'req-1' },
            }));
            expect(observation.end).toHaveBeenCalledWith({
                output: expect.objectContaining({
                    success: true,
                    message_id: 'msg-client',
                    trace_id: 'trace-client',
                    target_worker_id: 'worker-123',
                }),
            });
            expect(spanRecorder.recordSpan).toHaveBeenCalledWith(expect.objectContaining({
                traceId: 'trace-client',
                spanId: 'msg-client:client.dispatch',
                routePolicy: 'FAIL_FAST',
                routeStatus: 'DIRECT_WORKER',
            }));
        });

        it('should honor caller-provided trace parent metadata without leaking it', async () => {
            const spanRecorder = { recordSpan: jest.fn().mockResolvedValue(undefined) };
            const dispatchFn = jest.fn();
            const tracedClient = new GatewayClient(
                undefined,
                mockRedis as any,
                undefined,
                spanRecorder as any,
                dispatchFn
            );

            await tracedClient.sendMessage({
                targetAgentType: 'demo-agent-ts',
                targetWorkerId: 'worker-123',
                sessionId: 'test-session-ts',
                content: 'direct worker message',
                messageId: 'msg-client',
                traceId: 'trace-client',
                metadata: {
                    request_id: 'req-1',
                    trace_parent_span_id: 'parent-span',
                    langfuse_parent_observation_id: 'parent-observation',
                },
            });

            const callArgs = mockRedis.xadd.mock.calls[0];
            const serializedMsg = JSON.parse(callArgs[3]);

            expect(serializedMsg.header.metadata).toEqual({ request_id: 'req-1' });
            expect(serializedMsg.header.trace_parent_span_id).toBe('parent-span');
            expect(serializedMsg.header.langfuse_parent_observation_id).toBe('parent-observation');
            expect(dispatchFn).not.toHaveBeenCalled();
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
            expect((client as any).registry.initializeExecution.mock.invocationCallOrder[0]).toBeLessThan(
                mockRedis.xadd.mock.invocationCallOrder[0]
            );
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
            mockRegistry.getAllSessionExecutions.mockResolvedValue([{
                execution_id: 'exec-1',
                message_id: 'msg-1',
                worker_id: 'worker-123',
                session_id: 'sess-1',
                status: 'CANCELLED',
            }]);

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
            mockRegistry.getAllSessionExecutions.mockResolvedValue([{
                execution_id: 'exec-1',
                message_id: 'msg-1',
                worker_id: 'worker-123',
                session_id: 'sess-1',
                target_agent_type: 'demo-agent-ts',
                status: 'RUNNING',
            }]);

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
            expect(serializedMsg.body.target_message_id).toBe('msg-1');
            expect(serializedMsg.body.target_execution_id).toBe('exec-1');
            expect(serializedMsg.body.target_worker_id).toBe('worker-123');
            expect(serializedMsg.body.reason).toBe('user aborted');
            expect(serializedMsg.body.requested_by).toBe('frontend');
            expect(serializedMsg.body.cancel_mode).toBe('graceful');
        });

        it('should mark queued execution as cancelling even when worker has not claimed it yet', async () => {
            const mockRegistry = (client as any).registry;
            mockRegistry.getAllSessionExecutions.mockResolvedValue([{
                execution_id: 'exec-queued',
                message_id: 'msg-queued',
                worker_id: '',
                session_id: 'sess-1',
                target_agent_type: 'demo-agent-ts',
                status: 'QUEUED',
            }]);

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

        it('should cancel every non-terminal execution in a session via cancelSession', async () => {
            const mockRegistry = (client as any).registry;
            mockRegistry.getAllSessionExecutions.mockResolvedValue([
                {
                    execution_id: 'exec-a',
                    message_id: 'msg-a',
                    worker_id: 'worker-1',
                    session_id: 'sess-1',
                    target_agent_type: 'agent-a',
                    status: 'RUNNING',
                },
                {
                    execution_id: 'exec-b',
                    message_id: 'msg-b',
                    worker_id: 'worker-2',
                    session_id: 'sess-1',
                    target_agent_type: 'agent-b',
                    status: 'QUEUED',
                },
            ]);

            const response = await client.cancelSession({
                sessionId: 'sess-1',
                reason: 'user abort',
            });

            expect(response.success).toBe(true);
            expect(response.status).toBe('CANCEL_REQUESTED');
            expect(response.cancelled_count).toBe(2);
            expect(response.already_finished_count).toBe(0);

            expect(mockRegistry.markExecutionCancelling).toHaveBeenCalledWith('exec-a', 'sess-1', 'user abort');
            expect(mockRegistry.markExecutionCancelling).toHaveBeenCalledWith('exec-b', 'sess-1', 'user abort');

            expect(mockRedis.xadd).toHaveBeenCalledTimes(2);
            const targetStreams = mockRedis.xadd.mock.calls.map((call: any[]) => call[0]);
            expect(targetStreams).toContain('byai_gateway:ctrl:worker:worker-1');
            expect(targetStreams).toContain('byai_gateway:ctrl:worker:worker-2');
        });

        it('should flag terminal executions without re-cancelling them in cancelSession', async () => {
            const mockRegistry = (client as any).registry;
            mockRegistry.getAllSessionExecutions.mockResolvedValue([
                {
                    execution_id: 'exec-a',
                    message_id: 'msg-a',
                    worker_id: 'worker-1',
                    session_id: 'sess-1',
                    target_agent_type: 'agent-a',
                    status: 'COMPLETED',
                },
                {
                    execution_id: 'exec-b',
                    message_id: 'msg-b',
                    worker_id: 'worker-2',
                    session_id: 'sess-1',
                    target_agent_type: 'agent-b',
                    status: 'RUNNING',
                },
            ]);

            const response = await client.cancelSession({
                sessionId: 'sess-1',
                reason: 'user abort',
            });

            expect(response.success).toBe(true);
            expect(response.status).toBe('CANCEL_REQUESTED');
            expect(response.cancelled_count).toBe(1);
            expect(response.already_finished_count).toBe(1);

            expect(mockRegistry.markExecutionCancelling).toHaveBeenCalledTimes(1);
            expect(mockRegistry.markExecutionCancelling).toHaveBeenCalledWith('exec-b', 'sess-1', 'user abort');
            expect(mockRegistry.markCancelRequested).toHaveBeenCalledWith('exec-a', 'sess-1', 'user abort');

            expect(mockRedis.xadd).toHaveBeenCalledTimes(1);
            expect(mockRedis.xadd.mock.calls[0][0]).toBe('byai_gateway:ctrl:worker:worker-2');
        });

        it('should return ALREADY_FINISHED from cancelSession when every execution is terminal', async () => {
            const mockRegistry = (client as any).registry;
            mockRegistry.getAllSessionExecutions.mockResolvedValue([
                {
                    execution_id: 'exec-a',
                    message_id: 'msg-a',
                    worker_id: 'worker-1',
                    session_id: 'sess-1',
                    status: 'COMPLETED',
                },
                {
                    execution_id: 'exec-b',
                    message_id: 'msg-b',
                    worker_id: 'worker-2',
                    session_id: 'sess-1',
                    status: 'FAILED',
                },
            ]);

            const response = await client.cancelSession({ sessionId: 'sess-1', reason: 'user abort' });

            expect(response.success).toBe(false);
            expect(response.status).toBe('ALREADY_FINISHED');
            expect(response.cancelled_count).toBe(0);
            expect(response.already_finished_count).toBe(2);

            expect(mockRegistry.markExecutionCancelling).not.toHaveBeenCalled();
            expect(mockRegistry.markCancelRequested).toHaveBeenCalledTimes(2);
            expect(mockRedis.xadd).not.toHaveBeenCalled();
        });

        it('should return NOT_FOUND from cancelSession for an empty session', async () => {
            const mockRegistry = (client as any).registry;
            mockRegistry.getAllSessionExecutions.mockResolvedValue([]);

            const response = await client.cancelSession({ sessionId: 'sess-unknown' });

            expect(response.success).toBe(false);
            expect(response.status).toBe('NOT_FOUND');
            expect(response.cancelled_count).toBe(0);
            expect(response.already_finished_count).toBe(0);
            expect(mockRegistry.markExecutionCancelling).not.toHaveBeenCalled();
            expect(mockRegistry.markCancelRequested).not.toHaveBeenCalled();
            expect(mockRedis.xadd).not.toHaveBeenCalled();
        });

        it('should mark unclaimed queued executions cancelling without dispatching in cancelSession', async () => {
            const mockRegistry = (client as any).registry;
            mockRegistry.getAllSessionExecutions.mockResolvedValue([{
                execution_id: 'exec-queued',
                message_id: 'msg-queued',
                worker_id: '',
                session_id: 'sess-1',
                status: 'QUEUED',
            }]);

            const response = await client.cancelSession({ sessionId: 'sess-1', reason: 'user abort' });

            expect(response.success).toBe(true);
            expect(response.status).toBe('CANCEL_REQUESTED');
            expect(response.cancelled_count).toBe(1);
            expect(mockRegistry.markExecutionCancelling).toHaveBeenCalledWith('exec-queued', 'sess-1', 'user abort');
            expect(mockRedis.xadd).not.toHaveBeenCalled();
        });

        it('should inherit cancelSession unmodified on ByaiGatewayClient', async () => {
            const { ByaiGatewayClient } = require('../src/byai_client');
            const byaiClient = new ByaiGatewayClient();
            (byaiClient as any).redis = mockRedis;
            const mockRegistry = (byaiClient as any).registry;
            mockRegistry.getAllSessionExecutions.mockResolvedValue([{
                execution_id: 'exec-a',
                message_id: 'msg-a',
                worker_id: 'worker-1',
                session_id: 'sess-1',
                status: 'RUNNING',
            }]);

            const response = await byaiClient.cancelSession({ sessionId: 'sess-1', reason: 'user abort' });

            expect(response.success).toBe(true);
            expect(response.status).toBe('CANCEL_REQUESTED');
            expect(response.cancelled_count).toBe(1);
            expect(mockRegistry.markExecutionCancelling).toHaveBeenCalledWith('exec-a', 'sess-1', 'user abort');
            expect(mockRedis.xadd).toHaveBeenCalledTimes(1);
        });
    });
});

import { WorkerRunner } from '../src/runner';
import { GatewayWorker } from '../src/worker';
import { AgentContext } from '../src/context';
import { AskAgentCommand, CancelTaskCommand, GatewayCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { QueueNames } from '../src/constants';

class MockRedisRunner {
    public ackCalls: Array<[string, string, string]> = [];
    public groupCreateCalls: Array<[string, string, string, string]> = [];
    public xaddCalls: Array<any[]> = [];
    public xreadgroupCalls: any[][] = [];

    async xack(name: string, groupName: string, msgId: string): Promise<number> {
        this.ackCalls.push([name, groupName, msgId]);
        return 1;
    }

    async xgroup(...args: string[]): Promise<'OK'> {
        this.groupCreateCalls.push([args[1], args[2], args[3], args[4]]);
        return 'OK';
    }

    async xadd(...args: any[]): Promise<string> {
        this.xaddCalls.push(args);
        return '1-0';
    }

    async xreadgroup(...args: any[]): Promise<null> {
        this.xreadgroupCalls.push(args);
        return null;
    }

    pipeline() {
        const self = this;
        const pipe = {
            xadd: (...args: any[]) => {
                self.xadd(...args);
                return pipe;
            },
            expire: (key: string, seconds: number) => {
                return pipe;
            },
            exec: async () => {
                return [];
            }
        };
        return pipe;
    }
}

class MockRedisRunnerWithDuplicate extends MockRedisRunner {
    public duplicates: MockRedisRunner[] = [];

    duplicate(): MockRedisRunner {
        const child = new MockRedisRunner();
        this.duplicates.push(child);
        return child;
    }
}

class TestRegistry {
    public markExecutionCancelling = jest.fn().mockResolvedValue(undefined);
    public markExecutionFinished = jest.fn().mockResolvedValue(undefined);
    public saveExecution = jest.fn().mockResolvedValue(undefined);
    public getExecutionByMessageId = jest.fn().mockResolvedValue(null);
    public registerWorker = jest.fn().mockResolvedValue(undefined);
    public claimWorkerId = jest.fn().mockResolvedValue('lock-token');
    public refreshWorkerIdLock = jest.fn().mockResolvedValue(true);
    public releaseWorkerId = jest.fn().mockResolvedValue(true);
}

class SlowWorker extends GatewayWorker {
    public startedResolver: (() => void) | null = null;

    constructor(
        workerId: string,
        registry: any,
        redisClient: any
    ) {
        super(workerId, registry, redisClient);
    }

    getAgentTypes(): string[] {
        return ['dummy-agent'];
    }

    async processCommand(_command: GatewayCommand, context: AgentContext): Promise<any> {
        this.startedResolver?.();
        while (true) {
            await context.checkCancelled();
            await new Promise((resolve) => setTimeout(resolve, 5));
        }
    }
}

describe('WorkerRunner cancellation flow', () => {
    test('sets up worker control stream', async () => {
        const redis = new MockRedisRunner();
        const registry = new TestRegistry();
        const worker = new SlowWorker('worker-1', registry as any, redis as any);
        const runner = new WorkerRunner(worker, { redisClient: redis as any, groupName: 'test-group' });

        await (runner as any).setupControlStreams();

        expect(redis.groupCreateCalls).toContainEqual([
            QueueNames.worker_ctrl_stream('worker-1'),
            'test-group',
            '0',
            'MKSTREAM',
        ]);
    });

    test('control message cancels running execution and acks both messages', async () => {
        const redis = new MockRedisRunner();
        const registry = new TestRegistry();
        registry.getExecutionByMessageId.mockResolvedValue({
            execution_id: 'exec-1',
            session_id: 'sess-1',
            worker_id: 'worker-1',
            status: 'RUNNING',
            cancel_requested: false,
            cancel_reason: '',
        });
        const worker = new SlowWorker('worker-1', registry as any, redis as any);
        const runner = new WorkerRunner(worker, { redisClient: redis as any, groupName: 'test-group' });

        const started = new Promise<void>((resolve) => {
            worker.startedResolver = resolve;
        });

        const askCommand = new AskAgentCommand(
            new MessageHeader('msg-1', 'sess-1', 'trace-1', {
                targetAgentType: 'dummy-agent',
            }),
            'hello'
        );

        const processing = runner.processAndAck(
            QueueNames.ctrl_stream('dummy-agent'),
            '1-0',
            askCommand
        );

        await started;

        const cancelCommand = new CancelTaskCommand(
            new MessageHeader('ctl-1', 'sess-1', 'trace-2', {
                targetAgentType: 'dummy-agent',
                parentMessageId: 'msg-1',
            }),
            'msg-1',
            'exec-1',
            'worker-1',
            'user aborted',
            'frontend',
            'graceful'
        );

        await (runner as any)._handleControlMessage(
            QueueNames.worker_ctrl_stream('worker-1'),
            '2-0',
            cancelCommand
        );

        await expect(processing).resolves.toBeUndefined();
        expect(registry.markExecutionCancelling).toHaveBeenCalledWith('exec-1', 'sess-1', 'user aborted');
        expect(registry.markExecutionFinished).toHaveBeenCalledWith('exec-1', 'sess-1', 'CANCELLED');
        expect(redis.ackCalls).toContainEqual([QueueNames.worker_ctrl_stream('worker-1'), 'test-group', '2-0']);
        expect(redis.ackCalls).toContainEqual([QueueNames.ctrl_stream('dummy-agent'), 'test-group', '1-0']);
    });

    test('queued execution marked cancelling is skipped and acked without processing', async () => {
        const redis = new MockRedisRunner();
        const registry = new TestRegistry();
        registry.getExecutionByMessageId.mockResolvedValue({
            execution_id: 'exec-queued',
            session_id: 'sess-1',
            worker_id: '',
            status: 'CANCELLING',
            cancel_requested: true,
            cancel_reason: 'user aborted',
        });
        const worker = new SlowWorker('worker-1', registry as any, redis as any);
        const runner = new WorkerRunner(worker, { redisClient: redis as any, groupName: 'test-group' });

        const askCommand = new AskAgentCommand(
            new MessageHeader('msg-queued', 'sess-1', 'trace-1', {
                targetAgentType: 'dummy-agent',
            }),
            'hello'
        );

        await runner.processAndAck(
            QueueNames.ctrl_stream('dummy-agent'),
            '3-0',
            askCommand
        );

        expect(registry.markExecutionFinished).toHaveBeenCalledWith('exec-queued', 'sess-1', 'CANCELLED');
        expect(redis.ackCalls).toContainEqual([QueueNames.ctrl_stream('dummy-agent'), 'test-group', '3-0']);
    });

    test('existing queued execution is upserted to running with worker id', async () => {
        const redis = new MockRedisRunner();
        const registry = new TestRegistry();
        registry.getExecutionByMessageId.mockResolvedValue({
            execution_id: 'exec-queued-2',
            session_id: 'sess-2',
            worker_id: '',
            status: 'QUEUED',
            cancel_requested: false,
            cancel_reason: '',
            created_at: 123,
        });
        const worker = new SlowWorker('worker-1', registry as any, redis as any);
        const runner = new WorkerRunner(worker, { redisClient: redis as any, groupName: 'test-group' });
        const started = new Promise<void>((resolve) => {
            worker.startedResolver = resolve;
        });

        const askCommand = new AskAgentCommand(
            new MessageHeader('msg-queued-2', 'sess-2', 'trace-2', {
                targetAgentType: 'dummy-agent',
            }),
            'hello'
        );

        const processing = runner.processAndAck(
            QueueNames.ctrl_stream('dummy-agent'),
            '4-0',
            askCommand
        );
        await started;

        // Cancel it quickly to stop the worker loop.
        const cancelCommand = new CancelTaskCommand(
            new MessageHeader('ctl-2', 'sess-2', 'trace-3', {
                targetAgentType: 'dummy-agent',
                parentMessageId: 'msg-queued-2',
            }),
            'msg-queued-2',
            'exec-queued-2',
            'worker-1',
            'user aborted',
            'frontend',
            'graceful'
        );
        await (runner as any)._handleControlMessage(
            QueueNames.worker_ctrl_stream('worker-1'),
            '5-0',
            cancelCommand
        );
        await expect(processing).resolves.toBeUndefined();

        expect(registry.saveExecution).toHaveBeenCalledWith(expect.objectContaining({
            execution_id: 'exec-queued-2',
            message_id: 'msg-queued-2',
            session_id: 'sess-2',
            worker_id: 'worker-1',
            status: 'RUNNING',
        }));
    });

    test('uses dedicated Redis duplicates for blocking task and control reads', async () => {
        const redis = new MockRedisRunnerWithDuplicate();
        const registry = new TestRegistry();
        const worker = new SlowWorker('worker-1', registry as any, redis as any);
        const runner = new WorkerRunner(worker, { redisClient: redis as any, groupName: 'test-group' });

        expect(redis.duplicates).toHaveLength(2);
        expect((runner as any).streamReadRedis).toBe(redis.duplicates[0]);
        expect((runner as any).controlReadRedis).toBe(redis.duplicates[1]);

        await runner.poll({ block: 1 });
        await runner.runControlOnce(1);

        expect(redis.xreadgroupCalls).toHaveLength(0);
        expect(redis.duplicates[0].xreadgroupCalls).toHaveLength(1);
        expect(redis.duplicates[1].xreadgroupCalls).toHaveLength(1);
    });
});

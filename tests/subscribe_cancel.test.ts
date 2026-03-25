import { WorkerRunner } from '../src/runner';
import { CancelTaskCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { QueueNames } from '../src/constants';

class MockRedis {
    public xreadgroupCalls: any[] = [];
    public ackCalls: any[] = [];
    private messages: any[] = [];

    setMessages(messages: any[]) {
        this.messages = messages;
    }

    async xreadgroup(...args: any[]): Promise<any> {
        this.xreadgroupCalls.push(args);
        if (this.messages.length > 0) {
            const batch = this.messages;
            this.messages = [];
            return batch;
        }
        return null;
    }

    async xack(...args: any[]): Promise<number> {
        this.ackCalls.push(args);
        return 1;
    }
}

describe('WorkerRunner.subscribeCancel', () => {
    let mockRedis: MockRedis;
    let runner: WorkerRunner;

    beforeEach(() => {
        mockRedis = new MockRedis();
        // @ts-ignore
        runner = new WorkerRunner({
            workerId: 'worker-test',
            getCapabilities: () => ['test-cap'],
            handleMessage: async () => 'SUCCESS'
        }, {
            redisClient: mockRedis as any
        });
    });

    test('should receive and handle CancelTaskCommand via subscribeCancel', (done) => {
        const cancelCmd = new CancelTaskCommand(
            new MessageHeader('msg-1', 'sess-1', 'trace-1', { targetAgentType: 'test-cap' }),
            'target-msg-id',
            'exec-1',
            'worker-test',
            'user cancel'
        );

        const streamName = QueueNames.worker_ctrl_stream('worker-test');
        
        // Mock Redis response for xreadgroup
        mockRedis.setMessages([
            [streamName, [['1-0', ['data', JSON.stringify(cancelCmd.toDict())]]]]
        ]);

        const subscription = runner.subscribeCancel(async (cmd) => {
            try {
                expect(cmd).toBeInstanceOf(CancelTaskCommand);
                expect(cmd.targetMessageId).toBe('target-msg-id');
                expect(cmd.reason).toBe('user cancel');
                subscription.stop();
                done();
            } catch (err) {
                done(err);
            }
        }, { pollInterval: 10 });
    });

    test('should stop polling when stop() is called', async () => {
        const handler = jest.fn();
        const subscription = runner.subscribeCancel(handler, { pollInterval: 10 });
        
        subscription.stop();
        
        // Wait a bit to ensure no more polls happen
        await new Promise(resolve => setTimeout(resolve, 50));
        
        // Initial poll might have happened, but let's check that it stops.
        const callCountAfterStop = mockRedis.xreadgroupCalls.length;
        await new Promise(resolve => setTimeout(resolve, 50));
        expect(mockRedis.xreadgroupCalls.length).toBe(callCountAfterStop);
    });
});

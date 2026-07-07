import { WorkerRunner } from '../src/runner';
import { QueueNames } from '../src/constants';
import { AskAgentCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';

function askAgentPayload(targetAgentType: string, messageId: string): string {
    const header = new MessageHeader(messageId, 'sess-1', 'trace-1', { targetAgentType });
    const cmd = new AskAgentCommand(header, 'hi');
    return JSON.stringify(cmd.toDict());
}

class MockRedisTwoPhase {
    public xreadgroupCalls: any[][] = [];
    public duplicates: MockRedisTwoPhase[] = [];
    private queues = new Map<string, any[]>();
    /** When set, xreadgroup calls that include 'BLOCK' resolve only after this promise settles. */
    blockGate: Promise<void> | null = null;

    duplicate(): MockRedisTwoPhase {
        const child = new MockRedisTwoPhase();
        this.duplicates.push(child);
        return child;
    }

    enqueue(streamName: string, msgId: string, dataStr: string) {
        if (!this.queues.has(streamName)) this.queues.set(streamName, []);
        this.queues.get(streamName)!.push([msgId, ['data', dataStr]]);
    }

    async xgroup(..._args: any[]): Promise<'OK'> {
        return 'OK';
    }

    async xreadgroup(...args: any[]): Promise<any> {
        this.xreadgroupCalls.push(args);
        const isBlocking = args.includes('BLOCK');
        if (isBlocking && this.blockGate) {
            await this.blockGate;
        }
        const streamsIndex = args.indexOf('STREAMS');
        const rest = args.slice(streamsIndex + 1);
        const streamName = rest[0]; // single-stream calls only, per the two-phase design
        const pending = this.queues.get(streamName) || [];
        if (pending.length === 0) return null;
        const messages = [...pending];
        this.queues.set(streamName, []);
        return [[streamName, messages]];
    }
}

function makeWorker(agentTypes: string[]) {
    return {
        workerId: 'worker-1',
        getAgentTypes: () => agentTypes,
        registry: {} as any,
        startHeartbeat: async () => {},
        stopHeartbeat: () => {},
        handleMessage: async () => { throw new Error('unused'); },
    } as any;
}

describe('WorkerRunner.poll two-phase XREADGROUP', () => {
    test('phase one (non-blocking) picks up a message without ever issuing a BLOCK call', async () => {
        const redis = new MockRedisTwoPhase();
        const worker = makeWorker(['a', 'b', 'c']);
        const runner = new WorkerRunner(worker, { redisClient: redis as any, groupName: 'g' });
        const streamReader = redis.duplicates[0];
        const streamB = QueueNames.ctrl_stream('b');
        streamReader.enqueue(streamB, '1-0', askAgentPayload('b', 'm-1'));

        const results = await runner.poll({ block: 5000 });

        expect(results).toHaveLength(1);
        expect(results[0].streamName).toBe(streamB);
        // 3 agent_types declared -> exactly 3 non-blocking phase-one calls, no phase-two blocking call
        expect(streamReader.xreadgroupCalls).toHaveLength(3);
        for (const call of streamReader.xreadgroupCalls) {
            expect(call).not.toContain('BLOCK');
        }
    });

    test('phase two blocks on a single primary stream, rotating round-robin across agent_types', async () => {
        const redis = new MockRedisTwoPhase();
        const worker = makeWorker(['a', 'b', 'c']);
        const runner = new WorkerRunner(worker, { redisClient: redis as any, groupName: 'g' });
        const streamReader = redis.duplicates[0];

        const primaries: string[] = [];
        for (let i = 0; i < 6; i++) {
            await runner.poll({ block: 1 });
            const blockingCall = streamReader.xreadgroupCalls[streamReader.xreadgroupCalls.length - 1];
            const streamsIndex = blockingCall.indexOf('STREAMS');
            primaries.push(blockingCall[streamsIndex + 1]);
        }

        expect(primaries).toEqual([
            QueueNames.ctrl_stream('a'),
            QueueNames.ctrl_stream('b'),
            QueueNames.ctrl_stream('c'),
            QueueNames.ctrl_stream('a'),
            QueueNames.ctrl_stream('b'),
            QueueNames.ctrl_stream('c'),
        ]);
        // Every poll() = 3 non-blocking phase-one calls + 1 blocking phase-two call, all on the
        // one existing streamReadRedis connection — no new duplicate() connections were opened.
        expect(streamReader.xreadgroupCalls).toHaveLength(6 * 4);
        expect(redis.duplicates).toHaveLength(2);
    });

    test('a message on a non-primary stream arriving mid phase-two-block is only picked up after that block ends', async () => {
        const redis = new MockRedisTwoPhase();
        const worker = makeWorker(['a', 'b']);
        const runner = new WorkerRunner(worker, { redisClient: redis as any, groupName: 'g' });
        const streamReader = redis.duplicates[0];

        let releaseBlock: () => void = () => {};
        streamReader.blockGate = new Promise((resolve) => { releaseBlock = resolve; });

        // First poll(): phase one finds nothing, phase two blocks on primary 'a'.
        const pollPromise = runner.poll({ block: 30_000 });

        // A message "arrives" on the non-primary stream ('b') while phase two is still blocked.
        await Promise.resolve();
        streamReader.enqueue(QueueNames.ctrl_stream('b'), '1-0', askAgentPayload('b', 'm-2'));

        // The in-flight poll() must not see it — it's blocked on stream 'a', not scanning 'b' again.
        releaseBlock();
        const firstResults = await pollPromise;
        expect(firstResults).toHaveLength(0);

        // Only the *next* poll()'s phase-one scan picks it up.
        streamReader.blockGate = null;
        const secondResults = await runner.poll({ block: 1 });
        expect(secondResults).toHaveLength(1);
        expect(secondResults[0].streamName).toBe(QueueNames.ctrl_stream('b'));
    });
});

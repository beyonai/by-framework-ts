import { WorkerRunner } from '../src/runner';
import { AskAgentCommand, GatewayCommand, ResumeCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';

function deferred() {
    let resolve!: () => void;
    const promise = new Promise<void>(done => { resolve = done; });
    return { promise, resolve };
}

function makeRunner(maxConcurrency?: number) {
    const runner = new WorkerRunner({
        workerId: 'dispatch-probe',
        getAgentTypes: () => ['probe'],
        handleMessage: async () => undefined,
        registry: {},
    } as any, { redisClient: {} as any, maxConcurrency });
    const release = jest.spyOn(runner, 'release').mockResolvedValue(undefined);
    return { runner, release };
}

function ask(index: number) {
    return new AskAgentCommand(new MessageHeader(`m-${index}`, `s-${index}`, `t-${index}`), 'work');
}

describe('WorkerRunner asynchronous dispatch', () => {
    test.each([undefined, 1, 8, 50])('dispatches beyond legacy limit %s and waits for tasks on stop', async legacyLimit => {
        const { runner, release } = makeRunner(legacyLimit);
        const work = deferred();
        const started: string[] = [];
        jest.spyOn(runner, 'processAndAck').mockImplementation(async (_stream, _id, command) => {
            started.push(command.header.messageId);
            await work.promise;
        });
        jest.spyOn(runner, 'poll')
            .mockResolvedValueOnce(Array.from({ length: 64 }, (_, index) => ({
                streamName: 'tasks', msgId: `${index}-0`, data: ask(index),
            })))
            .mockImplementation(async () => { runner.stop(); return []; });

        const run = runner.start({ initialize: false });
        try {
            await new Promise<void>(resolve => setImmediate(resolve));
            expect(started).toHaveLength(64);
            expect(release).not.toHaveBeenCalled();
        } finally {
            runner.stop();
            work.resolve();
            await run;
        }
        expect(release).toHaveBeenCalledTimes(1);
    });

    test('reads a later Resume while every earlier request is waiting for that reply', async () => {
        const { runner } = makeRunner(8);
        const reply = deferred();
        let resumed = false;
        // The message-processing seam suspends requests until a later Resume arrives.
        jest.spyOn(runner, 'processAndAck').mockImplementation(async (_stream, _id, command: GatewayCommand) => {
            if (command instanceof ResumeCommand) {
                resumed = true;
                reply.resolve();
            } else {
                await reply.promise;
            }
        });
        jest.spyOn(runner, 'poll')
            .mockResolvedValueOnce(Array.from({ length: 9 }, (_, index) => ({
                streamName: 'tasks', msgId: `${index}-0`, data: ask(index),
            })))
            .mockResolvedValueOnce([{
                streamName: 'tasks', msgId: '10-0',
                data: new ResumeCommand(new MessageHeader('reply', 's-0', 't-0'), 'answer'),
            }])
            .mockImplementation(async () => { runner.stop(); return []; });
        const run = runner.start({ initialize: false });
        try {
            await new Promise<void>(resolve => setImmediate(resolve));
            expect(resumed).toBe(true);
        } finally {
            runner.stop();
            reply.resolve();
            await run;
        }
    });
});

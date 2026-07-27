import { AgentContext } from '../src/context';
import { GatewayWorker } from '../src/worker';
import { ResumeCommand, GatewayCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { AgentState } from '../src/protocol/agent_state';
import { PluginRegistry } from '../src/extensions/registry';
import { QueueNames, TASK_GROUP_FIELD_ABORTED } from '../src/constants';

/**
 * Minimal fake Redis supporting the hash + stream operations exercised by
 * AgentContext.callAgents/dispatchGroup and GatewayWorker's Group Join path.
 */
class FakeRedis {
    calls: Array<{ name: string; payload: string }> = [];
    private hashes = new Map<string, Record<string, string>>();
    /** When set, xadd throws once the call count reaches this index (0-based). */
    failXaddAtCallIndex: number | null = null;
    private xaddCallCount = 0;

    async xadd(name: string, _id: string, field: string, payload: string): Promise<string> {
        if (this.failXaddAtCallIndex !== null && this.xaddCallCount === this.failXaddAtCallIndex) {
            this.xaddCallCount += 1;
            throw new Error('simulated dispatch failure');
        }
        this.xaddCallCount += 1;
        if (field !== 'data') throw new Error(`unsupported field: ${field}`);
        this.calls.push({ name, payload });
        return '1-0';
    }

    async hset(key: string, fieldOrMapping: string | Record<string, string>, value?: string): Promise<number> {
        const existing = this.hashes.get(key) || {};
        if (typeof fieldOrMapping === 'string') {
            existing[fieldOrMapping] = value as string;
        } else {
            Object.assign(existing, fieldOrMapping);
        }
        this.hashes.set(key, existing);
        return 1;
    }

    async hget(key: string, field: string): Promise<string | null> {
        return this.hashes.get(key)?.[field] ?? null;
    }

    async hgetall(key: string): Promise<Record<string, string>> {
        return this.hashes.get(key) || {};
    }

    async hincrby(key: string, field: string, increment: number): Promise<number> {
        const existing = this.hashes.get(key) || {};
        const next = (parseInt(existing[field] || '0', 10)) + increment;
        existing[field] = String(next);
        this.hashes.set(key, existing);
        return next;
    }

    async expire(_key: string, _seconds: number): Promise<number> {
        return 1;
    }

    pipeline() {
        const pipe = {
            hset: () => pipe,
            expire: () => pipe,
            xadd: () => pipe,
            exec: async () => [],
        };
        return pipe;
    }
}

class RecordingWorker extends GatewayWorker {
    public receivedCommands: GatewayCommand[] = [];

    getAgentTypes(): ReadonlyArray<string> {
        return ['caller-agent'];
    }

    async processCommand(command: GatewayCommand): Promise<any> {
        this.receivedCommands.push(command);
        return 'ok';
    }
}

function memberReply(taskGroupId: string, messageId: string, replyData: unknown): ResumeCommand {
    return new ResumeCommand(
        new MessageHeader(messageId, 'sess-group', 'trace-group', {
            targetAgentType: 'caller-agent',
            taskGroupId,
        }),
        '',
        AgentState.COMPLETED,
        replyData
    );
}

describe('callAgents / dispatchGroup naming (ADR-0001)', () => {
    test('dispatchGroup is a pure alias delegating to callAgents — no forked implementation', async () => {
        const redis = new FakeRedis();
        const ctx = new AgentContext('sess-alias', 'trace-alias', redis as any, 'caller-agent', 'parent-msg');
        const spy = jest.spyOn(ctx, 'callAgents');

        const params = { tasks: [{ targetAgentType: 'sub-agent', content: 'x' }] };
        await ctx.dispatchGroup(params);

        expect(spy).toHaveBeenCalledTimes(1);
        expect(spy).toHaveBeenCalledWith(params);
    });
});

describe('Group Join aggregation (ADR-0001)', () => {
    test('resumes the caller with the aggregate of every member result, not one arbitrary reply', async () => {
        const redis = new FakeRedis();
        const worker = new RecordingWorker('worker-1', undefined, redis as any, new PluginRegistry());

        const taskGroupId = 'tg-test1';
        await redis.hset(QueueNames.task_group(taskGroupId), {
            total: '2',
            completed: '0',
            source_agent_type: 'caller-agent',
        });

        const firstReply = memberReply(taskGroupId, 'member-a', { answer: 'A' });
        const firstResult = await worker.handleMessage(firstReply);

        // First sibling to land: group isn't complete yet, business logic must not run.
        expect(firstResult.status).toContain('waiting_for_group');
        expect(worker.receivedCommands).toHaveLength(0);

        const secondReply = memberReply(taskGroupId, 'member-b', { answer: 'B' });
        await worker.handleMessage(secondReply);

        // Second (last) sibling triggers Group Join: business logic runs exactly once,
        // and must see BOTH members' results aggregated — not just member-b's reply.
        expect(worker.receivedCommands).toHaveLength(1);
        const resumedCommand = worker.receivedCommands[0] as ResumeCommand;
        const aggregate = resumedCommand.replyData as Array<{ message_id: string; reply_data: unknown }>;
        expect(Array.isArray(aggregate)).toBe(true);
        expect(aggregate).toHaveLength(2);
        const answers = aggregate.map((r) => (r.reply_data as any).answer).sort();
        expect(answers).toEqual(['A', 'B']);
    });
});

describe('Aborted Task Group safeguard (ADR-0001)', () => {
    test('a dispatch failure partway through fan-out marks the group aborted', async () => {
        const redis = new FakeRedis();
        redis.failXaddAtCallIndex = 1; // second task's xadd throws
        const ctx = new AgentContext('sess-abort', 'trace-abort', redis as any, 'caller-agent', 'parent-msg');

        await expect(ctx.callAgents({
            tasks: [
                { targetAgentType: 'sub-agent-1', content: 'first' },
                { targetAgentType: 'sub-agent-2', content: 'second' },
            ],
        })).rejects.toThrow('simulated dispatch failure');

        // We don't know the generated taskGroupId from the caller's side directly,
        // but exactly one task_group hash should have been written, and it must be aborted.
        const groupKeys = (redis as any).hashes as Map<string, Record<string, string>>;
        const groupEntry = [...groupKeys.entries()].find(([key]) => key.includes(':task_group:'));
        expect(groupEntry).toBeDefined();
        const [, fields] = groupEntry!;
        expect(fields[TASK_GROUP_FIELD_ABORTED]).toBe('1');
    });

    test('an aborted group discards a late member reply instead of resuming the caller', async () => {
        const redis = new FakeRedis();
        const worker = new RecordingWorker('worker-1', undefined, redis as any, new PluginRegistry());

        const taskGroupId = 'tg-test2';
        await redis.hset(QueueNames.task_group(taskGroupId), {
            total: '2',
            completed: '1',
            source_agent_type: 'caller-agent',
            [TASK_GROUP_FIELD_ABORTED]: '1',
        });

        const lateReply = memberReply(taskGroupId, 'member-late', { answer: 'late' });
        const result = await worker.handleMessage(lateReply);

        expect(result.status).toContain('group_aborted');
        expect(worker.receivedCommands).toHaveLength(0);
    });
});

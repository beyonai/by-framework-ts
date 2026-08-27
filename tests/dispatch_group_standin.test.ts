import { AgentContext } from '../src/context';
import { AnonymousWorker } from '../src/worker';
import { GatewayProcessor } from '../src/processor';
import { WorkerRunner } from '../src/runner';
import { WorkerRegistry } from '../src/registry';
import { PluginRegistry } from '../src/extensions/registry';
import { AskAgentCommand, ResumeCommand, GatewayCommand, commandFromDict } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { AgentState } from '../src/protocol/agent_state';
import {
    DEFAULT_REPLY_TIMEOUT_MS,
    QueueNames,
    TASK_GROUP_FIELD_ABORTED,
    TASK_GROUP_FIELD_COMPLETED,
    TASK_GROUP_FIELD_TOTAL,
} from '../src/constants';
import { ALLOW_CLAIMED, ALLOW_UNREGISTERED, consumeWaitEntry } from '../src/liveness/wait_gate';
import { decodeMember, waitIndexKey } from '../src/liveness/wait_index';
import { registerWait } from '../src/liveness/wait_registration';
import { SYNTHESIZED_BY_DISPATCH } from '../src/liveness/wait_reply';
import { MockRedis, bringAgentTypeOnline } from './helpers/mock_redis';

/**
 * D6: the dispatcher must never book a Task Group itself.
 *
 * A member whose target agent type is unavailable will never be answered by a
 * worker, so the group needs one more accounting event from somewhere. Writing
 * task_group_results + HINCRBY `completed` from the dispatch loop is a SECOND
 * implementation of the accounting GatewayWorker's Group Join already owns, and
 * when that copy is the increment that reaches `total` there is no reply left to
 * trigger the join — the caller stays suspended forever.
 *
 * The compensation is therefore a *reply* (a stand-in FAILED ResumeCommand),
 * flushed after the handler returns, so the last accounting event is always a
 * reply and the join always runs.
 */

function buildWorker(redis: MockRedis, onTask: (c: GatewayCommand, ctx: AgentContext) => Promise<any>) {
    return new AnonymousWorker({
        workerId: 'worker-d6',
        agentTypes: ['caller-agent'],
        registry: new WorkerRegistry(redis as any),
        redisClient: redis as any,
        pluginRegistry: new PluginRegistry(),
        onTask,
    });
}

function askCaller(sessionId: string = 'sess-d6'): AskAgentCommand {
    return new AskAgentCommand(
        new MessageHeader('msg-caller', sessionId, 'trace-d6', {
            targetAgentType: 'caller-agent',
            sourceAgentType: 'upstream-agent',
            parentMessageId: 'msg-upstream',
        }),
        'do the fan-out'
    );
}

/**
 * Replies sitting on the caller's own control stream, decoded by the SAME
 * commandFromDict the runner uses — a hand-rolled parse here would not prove the
 * stand-in is on the wire in a shape the runner can actually read back.
 */
function callerCtrlReplies(redis: MockRedis): ResumeCommand[] {
    return redis.getStreamPayloads(QueueNames.ctrl_stream('caller-agent'))
        .map((payload) => commandFromDict(payload) as ResumeCommand);
}

describe('the dispatch loop never books the group itself', () => {
    test('an unavailable member writes no result and no completed increment', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext(
            'sess-d6', 'trace-d6', redis as any, 'caller-agent', 'msg-caller', askCaller()
        );

        const group = await ctx.dispatchGroup({
            tasks: [{ targetAgentType: 'missing-agent', content: 'x' }],
        });

        const groupKey = QueueNames.task_group(group.taskGroupId);
        expect(await redis.hget(groupKey, TASK_GROUP_FIELD_TOTAL)).toBe('1');
        // The load-bearing assertion: the dispatcher counted NOTHING. If it had
        // incremented here, this increment would be the one that reaches total
        // and no reply would be left to trigger the join.
        expect(await redis.hget(groupKey, TASK_GROUP_FIELD_COMPLETED)).toBe('0');
        expect(await redis.hgetall(QueueNames.task_group_results(group.taskGroupId))).toEqual({});
    });

    test('the compensation is queued, not sent inline', async () => {
        // Sending inline would put the reply on the caller's OWN control stream
        // strictly before the caller's handler returns and its execution is
        // recorded as suspended — turning the pre-existing "a very fast sibling
        // replies first" race from unlikely into certain.
        const redis = new MockRedis();
        const ctx = new AgentContext(
            'sess-d6', 'trace-d6', redis as any, 'caller-agent', 'msg-caller', askCaller()
        );

        await ctx.dispatchGroup({ tasks: [{ targetAgentType: 'missing-agent', content: 'x' }] });

        expect(callerCtrlReplies(redis)).toEqual([]);
        expect(ctx.takePendingGroupReplies()).toHaveLength(1);
    });

    test('a wait entry is registered before the stand-in is queued', async () => {
        // Not a nicety: the flush is fail-soft by necessity (throwing there
        // would destroy the caller's own result), so a stand-in that never gets
        // delivered must still leave something a sweep can find. Leaning on the
        // gate's ALLOW_UNREGISTERED instead would mean a lost stand-in leaves
        // nothing behind at all — this fix opening a hole of its own.
        const redis = new MockRedis();
        const ctx = new AgentContext(
            'sess-d6', 'trace-d6', redis as any, 'caller-agent', 'msg-caller', askCaller()
        );

        const group = await ctx.dispatchGroup({
            tasks: [{ targetAgentType: 'missing-agent', content: 'x' }],
        });

        const entries = await redis.zrangebyscore(waitIndexKey('sess-d6'), '-inf', '+inf');
        expect(entries).toHaveLength(1);
        expect(decodeMember(entries[0])).toEqual({
            sessionId: 'sess-d6',
            parentMessageId: 'msg-caller',
            childMessageId: group.dispatchedTasks[0].message_id,
            taskGroupId: group.taskGroupId,
        });
    });

    test('the undispatched member is recorded FAILED so a sweep can triage it', async () => {
        const redis = new MockRedis();
        const registry = new WorkerRegistry(redis as any);
        const ctx = new AgentContext(
            'sess-d6', 'trace-d6', redis as any, 'caller-agent', 'msg-caller', askCaller()
        );

        const group = await ctx.dispatchGroup({
            tasks: [{ targetAgentType: 'missing-agent', content: 'x' }],
        });

        const record = await registry.getExecutionByMessageId(
            group.dispatchedTasks[0].message_id, 'sess-d6'
        );
        expect(record!.status).toBe(AgentState.FAILED);
        expect(record!.task_group_id).toBe(group.taskGroupId);
        expect(String(record!.availability_error_code)).toBe('AGENT_TYPE_NOT_FOUND');
    });

    test('nothing is XADDed to the unreachable target', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext(
            'sess-d6', 'trace-d6', redis as any, 'caller-agent', 'msg-caller', askCaller()
        );

        await ctx.dispatchGroup({ tasks: [{ targetAgentType: 'missing-agent', content: 'x' }] });

        expect(redis.getStreamPayloads(QueueNames.ctrl_stream('missing-agent'))).toEqual([]);
    });

    test('an online member is dispatched normally and queues no stand-in', async () => {
        const redis = new MockRedis();
        await bringAgentTypeOnline(redis, 'live-agent');
        const ctx = new AgentContext(
            'sess-d6', 'trace-d6', redis as any, 'caller-agent', 'msg-caller', askCaller()
        );

        await ctx.dispatchGroup({ tasks: [{ targetAgentType: 'live-agent', content: 'x' }] });

        expect(redis.getStreamPayloads(QueueNames.ctrl_stream('live-agent'))).toHaveLength(1);
        expect(ctx.takePendingGroupReplies()).toEqual([]);
    });

    test('a fire-and-forget fan-out is not probed and keeps its old behaviour', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext(
            'sess-d6', 'trace-d6', redis as any, 'caller-agent', 'msg-caller', askCaller()
        );

        await ctx.dispatchGroup({
            tasks: [{ targetAgentType: 'missing-agent', content: 'x' }],
            waitForReply: false,
        });

        // No group accounting exists to starve, so nothing is compensated.
        expect(redis.getStreamPayloads(QueueNames.ctrl_stream('missing-agent'))).toHaveLength(1);
        expect(ctx.takePendingGroupReplies()).toEqual([]);
    });
});

describe('the stand-in is shaped exactly like a real sub-agent reply', () => {
    test('the three ids are not swapped, and provenance rides on metadata', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext(
            'sess-d6', 'trace-d6', redis as any, 'caller-agent', 'msg-caller', askCaller()
        );
        const group = await ctx.dispatchGroup({
            tasks: [{ targetAgentType: 'missing-agent', content: 'x' }],
        });
        const childMessageId = group.dispatchedTasks[0].message_id;

        const [standIn] = ctx.takePendingGroupReplies();

        expect(standIn).toBeInstanceOf(ResumeCommand);
        // The caller reattaches its suspended execution by header.messageId.
        expect(standIn.header.messageId).toBe('msg-caller');
        // The only per-sibling-unique id; Group Join keys results by it.
        expect(standIn.header.parentMessageId).toBe(childMessageId);
        expect(standIn.header.taskGroupId).toBe(group.taskGroupId);
        expect(standIn.header.targetAgentType).toBe('caller-agent');
        expect(standIn.header.sourceAgentType).toBe('missing-agent');
        expect(standIn.status).toBe(AgentState.FAILED);
        // Provenance is for operators only — business code reads replyData, and
        // a caller that can tell a synthesized failure from a real one has two
        // error paths again.
        expect(standIn.header.metadata).toMatchObject({
            synthesized_by: SYNTHESIZED_BY_DISPATCH,
            child_message_id: childMessageId,
        });
    });

    test('the failure detail rides in replyData, where a real failure puts it', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext(
            'sess-d6', 'trace-d6', redis as any, 'caller-agent', 'msg-caller', askCaller()
        );
        const group = await ctx.dispatchGroup({
            tasks: [{ targetAgentType: 'missing-agent', content: 'x' }],
        });

        const [standIn] = ctx.takePendingGroupReplies();

        expect(standIn.replyData).toMatchObject({
            error_code: 'AGENT_TYPE_NOT_FOUND',
            child_message_id: group.dispatchedTasks[0].message_id,
        });
        expect(String((standIn.replyData as any).error)).toContain('missing-agent');
        expect((standIn.replyData as any).synthesized_by).toBeUndefined();
    });

    test('the stand-in passes the gate as ALLOW_CLAIMED, not ALLOW_UNREGISTERED', async () => {
        // Proof that the wait entry registered in step one really does match the
        // stand-in the same dispatch queued. If they disagreed, the entry would
        // linger for a sweep to compensate a member that has already been
        // compensated.
        const redis = new MockRedis();
        const ctx = new AgentContext(
            'sess-d6', 'trace-d6', redis as any, 'caller-agent', 'msg-caller', askCaller()
        );
        await ctx.dispatchGroup({ tasks: [{ targetAgentType: 'missing-agent', content: 'x' }] });
        const [standIn] = ctx.takePendingGroupReplies();

        const decision = await consumeWaitEntry(redis as any, standIn);

        expect(decision.reason).toBe(ALLOW_CLAIMED);
        expect(decision.reason).not.toBe(ALLOW_UNREGISTERED);
        expect(await redis.zrangebyscore(waitIndexKey('sess-d6'), '-inf', '+inf')).toEqual([]);
    });

    test('a context with no agent type registers the wait but queues no reply', async () => {
        // Nothing to address the reply to — a real sub-agent could not have
        // replied either. The wait entry is what gets this group unstuck.
        const redis = new MockRedis();
        const ctx = new AgentContext('sess-d6', 'trace-d6', redis as any, '', 'msg-caller');

        await ctx.dispatchGroup({ tasks: [{ targetAgentType: 'missing-agent', content: 'x' }] });

        expect(ctx.takePendingGroupReplies()).toEqual([]);
        expect(await redis.zrangebyscore(waitIndexKey('sess-d6'), '-inf', '+inf')).toHaveLength(1);
    });
});

describe('the flush happens after the handler returns, and only then', () => {
    test('GatewayWorker delivers the queued stand-ins to the caller ctrl stream', async () => {
        const redis = new MockRedis();
        let groupId = '';
        const worker = buildWorker(redis, async (_command, context) => {
            const group = await context.dispatchGroup({
                tasks: [{ targetAgentType: 'missing-agent', content: 'x' }],
            });
            groupId = group.taskGroupId;
            return { status: AgentState.QUEUED };
        });

        await worker.handleMessage(askCaller());

        const replies = callerCtrlReplies(redis);
        expect(replies).toHaveLength(1);
        expect(replies[0].header.taskGroupId).toBe(groupId);
        expect(replies[0].header.messageId).toBe('msg-caller');
        expect(replies[0].status).toBe(AgentState.FAILED);
    });

    test('a handler that throws sends no stand-in at all', async () => {
        // The caller execution these replies would resume is the one that just
        // failed; waking it with them is worse than the group timing out.
        const redis = new MockRedis();
        const worker = buildWorker(redis, async (_command, context) => {
            await context.dispatchGroup({
                tasks: [{ targetAgentType: 'missing-agent', content: 'x' }],
            });
            throw new Error('handler blew up after fanning out');
        });

        await worker.handleMessage(askCaller());

        expect(
            callerCtrlReplies(redis).filter((r) => r.header.taskGroupId)
        ).toEqual([]);
    });

    test('a delivery failure does not destroy the caller\'s own result', async () => {
        const redis = new MockRedis();
        const worker = buildWorker(redis, async (_command, context) => {
            await context.dispatchGroup({
                tasks: [{ targetAgentType: 'missing-agent', content: 'x' }],
            });
            return { status: AgentState.COMPLETED, replyData: { mine: 'ok' } };
        });
        const realXadd = redis.xadd.bind(redis);
        jest.spyOn(redis, 'xadd').mockImplementation(async (stream: string, ...rest: any[]) => {
            if (stream === QueueNames.ctrl_stream('caller-agent')) {
                throw new Error('ctrl stream unavailable');
            }
            return realXadd(stream, ...(rest as [any, ...any[]]));
        });

        const result = await worker.handleMessage(askCaller());

        expect(result.status).toBe(AgentState.COMPLETED);
    });

    test('GatewayProcessor flushes on the same rule', async () => {
        const redis = new MockRedis();
        const processor = new GatewayProcessor('worker-proc-d6', redis as any);

        await processor.process(askCaller(), async (_command, context) => {
            await context.dispatchGroup({
                tasks: [{ targetAgentType: 'missing-agent', content: 'x' }],
            });
            return { status: AgentState.QUEUED };
        });

        expect(callerCtrlReplies(redis).filter((r) => r.header.taskGroupId)).toHaveLength(1);
    });

    test('a second flush does not duplicate an already-sent stand-in', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext(
            'sess-d6', 'trace-d6', redis as any, 'caller-agent', 'msg-caller', askCaller()
        );
        await ctx.dispatchGroup({ tasks: [{ targetAgentType: 'missing-agent', content: 'x' }] });
        const { flushPendingGroupReplies } = await import('../src/liveness/wait_reply');

        await flushPendingGroupReplies(redis as any, ctx, 'w');
        await flushPendingGroupReplies(redis as any, ctx, 'w');

        // A duplicate would be dropped by the gate as "already consumed" — and
        // the real sibling reply behind it would be too.
        expect(callerCtrlReplies(redis)).toHaveLength(1);
    });
});

describe('both trigger paths end with the join running, not with a hung caller', () => {
    // These are the two ways dispatcher-side accounting reaches `total`:
    // (1) every target unavailable, (2) a fast sibling reply followed by a later
    // unavailable target. Both must finish with a REPLY as the last accounting
    // event.

    async function driveGroupToCompletion(params: {
        redis: MockRedis;
        tasks: ReadonlyArray<{ targetAgentType: string; content: string }>;
        /** Sibling replies to inject between dispatch and flush. */
        replyFor?: (childMessageId: string, taskGroupId: string) => ResumeCommand | null;
    }): Promise<{ groupId: string; joinRan: boolean; completed: string | null; total: string | null }> {
        const { redis, tasks } = params;
        const registry = new WorkerRegistry(redis as any);
        let groupId = '';
        let dispatched: ReadonlyArray<{ message_id: string; target_agent_type: string }> = [];
        const worker = buildWorker(redis, async (command, context) => {
            if (command instanceof ResumeCommand) {
                return { status: AgentState.COMPLETED };
            }
            const group = await context.dispatchGroup({ tasks: [...tasks] });
            groupId = group.taskGroupId;
            dispatched = group.dispatchedTasks;
            return { status: AgentState.QUEUED };
        });
        const runner = new WorkerRunner(worker, {
            redisClient: redis as any, groupName: 'group-d6',
        });
        await registry.initializeExecution({
            execution_id: 'exec-caller',
            message_id: 'msg-caller',
            session_id: 'sess-d6',
            source_agent_type: 'upstream-agent',
            parent_message_id: 'msg-upstream',
            target_agent_type: 'caller-agent',
            status: 'QUEUED',
        });

        await worker.handleMessage(askCaller());

        // Any sibling that DID reach a worker replies here, before the queued
        // stand-ins were sent... except they were already flushed by
        // handleMessage above, so replay the whole control stream in order,
        // interleaving the injected replies first (the race this covers).
        const injected: ResumeCommand[] = [];
        for (const task of dispatched) {
            const r = params.replyFor?.(task.message_id, groupId);
            if (r) injected.push(r);
        }
        const queued = callerCtrlReplies(redis)
            .filter((r) => r.header.taskGroupId === groupId);

        const joinCounts: number[] = [];
        let index = 0;
        for (const command of [...injected, ...queued]) {
            await runner.processAndAck(QueueNames.ctrl_stream('caller-agent'), `${++index}-0`, command);
            joinCounts.push(
                Number(await redis.hget(QueueNames.task_group(groupId), TASK_GROUP_FIELD_COMPLETED))
            );
        }

        const groupKey = QueueNames.task_group(groupId);
        return {
            groupId,
            joinRan: joinCounts.length > 0,
            completed: await redis.hget(groupKey, TASK_GROUP_FIELD_COMPLETED),
            total: await redis.hget(groupKey, TASK_GROUP_FIELD_TOTAL),
        };
    }

    test('path 1: EVERY target unavailable — the join still reaches total', async () => {
        const redis = new MockRedis();

        const outcome = await driveGroupToCompletion({
            redis,
            tasks: [
                { targetAgentType: 'missing-a', content: 'a' },
                { targetAgentType: 'missing-b', content: 'b' },
            ],
        });

        expect(outcome.total).toBe('2');
        // Every increment came from a REPLY going through Group Join, so the
        // final one had a reply behind it and the caller was woken.
        expect(outcome.completed).toBe('2');
        expect(Object.keys(await redis.hgetall(QueueNames.task_group_results(outcome.groupId))))
            .toHaveLength(2);
    });

    test('path 2: a fast sibling reply, then the unavailable target fills the group', async () => {
        // The race the old shape lost: the dispatcher's own increment for the
        // LAST member is the one that reaches total, so nothing is left to
        // trigger the join and the caller hangs. Here the last increment is the
        // stand-in reply, so the join runs.
        const redis = new MockRedis();
        await bringAgentTypeOnline(redis, 'live-agent');

        const outcome = await driveGroupToCompletion({
            redis,
            tasks: [
                { targetAgentType: 'live-agent', content: 'fast' },
                { targetAgentType: 'missing-agent', content: 'slow' },
            ],
            replyFor: (childMessageId, taskGroupId) => (
                // Only the online sibling actually ran; it replies immediately.
                childMessageId && taskGroupId
                    ? new ResumeCommand(
                        new MessageHeader('msg-caller', 'sess-d6', 'trace-d6', {
                            sourceAgentType: 'live-agent',
                            targetAgentType: 'caller-agent',
                            parentMessageId: childMessageId,
                            taskGroupId,
                        }),
                        'fast result', AgentState.COMPLETED, { from: 'live' }
                    )
                    : null
            ),
        });

        expect(outcome.total).toBe('2');
        expect(outcome.completed).toBe('2');
    });

    test('a stand-in that is never delivered still leaves a wait entry behind', async () => {
        // The backstop for the fail-soft flush: registration happens first, so a
        // lost stand-in degrades to "a wait a sweep can compensate" rather than
        // "a member nobody will ever account for".
        const redis = new MockRedis();
        const worker = buildWorker(redis, async (_command, context) => {
            await context.dispatchGroup({
                tasks: [{ targetAgentType: 'missing-agent', content: 'x' }],
            });
            return { status: AgentState.QUEUED };
        });
        const realXadd = redis.xadd.bind(redis);
        jest.spyOn(redis, 'xadd').mockImplementation(async (stream: string, ...rest: any[]) => {
            if (stream === QueueNames.ctrl_stream('caller-agent')) {
                throw new Error('ctrl stream unavailable');
            }
            return realXadd(stream, ...(rest as [any, ...any[]]));
        });

        await worker.handleMessage(askCaller());

        expect(callerCtrlReplies(redis)).toEqual([]);
        expect(await redis.zrangebyscore(waitIndexKey('sess-d6'), '-inf', '+inf')).toHaveLength(1);
    });
});

describe('wait_reply is the single construction shared with the sweeper', () => {
    test('flushPendingGroupReplies tolerates a context that has none', async () => {
        const redis = new MockRedis();
        const { flushPendingGroupReplies } = await import('../src/liveness/wait_reply');

        await expect(flushPendingGroupReplies(redis as any, {}, 'w')).resolves.toBeUndefined();
        await expect(flushPendingGroupReplies(redis as any, null, 'w')).resolves.toBeUndefined();
    });

    test('a standInReply built by hand is claimable by the same wait entry', async () => {
        // Guards the sweeper's future use of the same helper: whatever the
        // sweeper builds must clear the entry the dispatch registered.
        const redis = new MockRedis();
        const { standInReply, SYNTHESIZED_BY_SWEEPER } = await import('../src/liveness/wait_reply');
        await registerWait(redis as any, {
            sessionId: 'sess-share', parentMessageId: 'msg-caller',
            childMessageId: 'msg-child', taskGroupId: 'tg-share',
            timeoutMs: DEFAULT_REPLY_TIMEOUT_MS,
        });

        const decision = await consumeWaitEntry(redis as any, standInReply({
            sessionId: 'sess-share',
            callerMessageId: 'msg-caller',
            callerAgentType: 'caller-agent',
            childMessageId: 'msg-child',
            childAgentType: 'child-agent',
            taskGroupId: 'tg-share',
            status: AgentState.FAILED,
            synthesizedBy: SYNTHESIZED_BY_SWEEPER,
        }));

        expect(decision.reason).toBe(ALLOW_CLAIMED);
    });
});

describe('a fan-out that dies partway marks the group aborted', () => {
    /**
     * The `aborted` field is a cross-runtime contract on the task_group hash:
     * Python's dispatcher writes it, Python's join honours it, and a sweep in
     * any SDK must clean an orphaned member of such a group up rather than
     * compensate it. TS wrote it nowhere and read it nowhere, so a TS worker
     * joining a Python-aborted group counted replies for a caller that no longer
     * exists, and a TS-aborted group could not be recognized at all.
     */
    test('the group tracker records the abort when the fan-out throws', async () => {
        const redis = new MockRedis();
        await bringAgentTypeOnline(redis, 'child-agent');
        // The group id is minted inside dispatchGroup and the call throws
        // before returning it, so it is recovered from the tracker write.
        const groupKeys: string[] = [];
        const realHset = redis.hset.bind(redis);
        jest.spyOn(redis, 'hset').mockImplementation(async (key: string, ...rest: any[]) => {
            groupKeys.push(key);
            return realHset(key, ...(rest as [any, any]));
        });
        jest.spyOn(redis, 'xadd').mockImplementation(async () => {
            throw new Error('control stream unavailable');
        });
        const ctx = new AgentContext(
            'sess-abort', 'trace-abort', redis as any, 'caller-agent', 'msg-caller', askCaller('sess-abort')
        );

        await expect(ctx.dispatchGroup({
            tasks: [{ targetAgentType: 'child-agent', content: 'x' }],
        })).rejects.toThrow('control stream unavailable');

        const groupKey = groupKeys.find((key) => key.includes('task_group'));
        expect(groupKey).toBeDefined();
        expect(await redis.hget(groupKey!, TASK_GROUP_FIELD_ABORTED)).toBe('1');
    });

    test('the join discards a late sibling reply for an aborted group', async () => {
        // Counting it would resume an execution that the failed fan-out already
        // ended, and once `completed` reached `total` it would aggregate a group
        // that was never fully dispatched.
        const redis = new MockRedis();
        const worker = buildWorker(redis, async () => 'unreachable');
        const groupKey = QueueNames.task_group('tg-aborted');
        await redis.hset(groupKey, {
            [TASK_GROUP_FIELD_TOTAL]: '2',
            [TASK_GROUP_FIELD_COMPLETED]: '0',
        });
        await redis.hset(groupKey, TASK_GROUP_FIELD_ABORTED, '1');

        const result = await worker.handleMessage(new ResumeCommand(
            new MessageHeader('msg-caller', 'sess-abort', 'trace-abort', {
                targetAgentType: 'caller-agent',
                sourceAgentType: 'child-agent',
                parentMessageId: 'msg-sib1',
                taskGroupId: 'tg-aborted',
            }),
            'late', AgentState.COMPLETED, { ok: true }
        ), {});

        expect(result.status).toBe(`${AgentState.CANCELLED}: group_aborted`);
        expect(await redis.hget(groupKey, TASK_GROUP_FIELD_COMPLETED)).toBe('0');
        expect(await redis.hgetall(QueueNames.task_group_results('tg-aborted'))).toEqual({});
    });
});

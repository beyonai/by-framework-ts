import { GatewayWorker } from '../src/worker';
import { AgentContext } from '../src/context';
import { AskAgentCommand, ResumeCommand, GatewayCommand, commandFromDict } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { AgentState } from '../src/protocol/agent_state';
import { PluginRegistry } from '../src/extensions/registry';
import {
    QueueNames,
    RegistryKeys,
    TASK_GROUP_FIELD_TOTAL,
    TASK_GROUP_FIELD_COMPLETED,
    TASK_GROUP_FIELD_ABORTED,
    TASK_GROUP_FIELD_PROTOCOL_VERSION,
    TASK_GROUP_FIELD_TASK_ORDER,
    TASK_GROUP_PROTOCOL_V2,
} from '../src/constants';

/**
 * Stateful in-memory Redis double: enough hash/stream surface for a Task Group
 * to be dispatched and joined for real, so the completion counter, the result
 * hash keying and the aggregation are exercised rather than stubbed.
 */
class MockRedis {
    data: Record<string, Record<string, string>> = {};
    streams: Record<string, string[]> = {};
    offlineAgentTypes: Set<string>;

    constructor(offlineAgentTypes: string[] = []) {
        this.offlineAgentTypes = new Set(offlineAgentTypes);
    }

    /** ioredis accepts both hset(key, {field: value}) and hset(key, field, value). */
    async hset(key: string, fieldOrValues: string | Record<string, string>, value?: string): Promise<number> {
        const bucket = (this.data[key] = this.data[key] || {});
        if (typeof fieldOrValues === 'string') {
            bucket[fieldOrValues] = value as string;
        } else {
            Object.assign(bucket, fieldOrValues);
        }
        return 1;
    }
    async hget(key: string, field: string): Promise<string | null> {
        const v = this.data[key]?.[field];
        return v === undefined ? null : v;
    }
    async hgetall(key: string): Promise<Record<string, string>> {
        return { ...(this.data[key] || {}) };
    }
    async hincrby(key: string, field: string, increment: number): Promise<number> {
        const bucket = (this.data[key] = this.data[key] || {});
        const next = parseInt(bucket[field] || '0', 10) + increment;
        bucket[field] = String(next);
        return next;
    }
    async expire(_key: string, _seconds: number): Promise<number> {
        return 1;
    }
    async xadd(name: string, _id: string, _field: string, payload: string): Promise<string> {
        (this.streams[name] = this.streams[name] || []).push(payload);
        return '1-0';
    }
    /**
     * Worker presence leases resolve; availability's control-plane probes
     * (circuit / quota / fallback) return null, i.e. nothing configured.
     */
    async get(key: string): Promise<string | null> {
        if (key === RegistryKeys.worker_online_lease('worker-1')) {
            return JSON.stringify({ token: 'tok', last_seen: Date.now() });
        }
        return null;
    }
    async smembers(name: string): Promise<string[]> {
        for (const agentType of this.offlineAgentTypes) {
            if (name === RegistryKeys.agentTypeMembers(agentType)) return [];
        }
        return ['worker-1'];
    }
    async sismember(): Promise<number> {
        return 1;
    }
    async zrangebyscore(): Promise<string[]> {
        return ['worker-1'];
    }
    pipeline() {
        const ops: Array<() => Promise<unknown>> = [];
        const self = this as any;
        const pipe: any = new Proxy({}, {
            get(_t, prop: string) {
                if (prop === 'exec') {
                    return async () => {
                        const out = [];
                        for (const op of ops) out.push([null, await op()]);
                        ops.length = 0;
                        return out;
                    };
                }
                return (...args: any[]) => {
                    ops.push(() => (typeof self[prop] === 'function' ? self[prop](...args) : Promise.resolve(null)));
                    return pipe;
                };
            },
        });
        return pipe;
    }
}

class RecordingWorker extends GatewayWorker {
    received: GatewayCommand[] = [];

    getAgentTypes(): string[] {
        return ['caller_agent'];
    }

    async processCommand(command: GatewayCommand, _context: AgentContext): Promise<any> {
        this.received.push(command);
        return { status: 'ok' };
    }
}

function makeWorker(redis: any): RecordingWorker {
    // GatewayWorker's constructor is positional
    // (workerId, registry, redisClient, pluginRegistry, workspaceManager, ...).
    // Passing an options object leaves redisClient undefined, which falls back to
    // getRedis() and hangs on a real connection.
    const registry = {
        // Execution bookkeeping is a no-op here; these tests are about the join.
        getExecutionByMessageId: async () => null,
        initializeExecution: async () => undefined,
        saveExecution: async () => undefined,
        markExecutionFinished: async () => undefined,
        updateExecutionStatus: async () => undefined,
        hasOnlineAgentType: async () => [true, ['worker-1']],
    };
    const workspaceManager = {
        setupWorkspace: async () => ({ private: '/tmp/tg', public: '/tmp/tg' }),
        cleanupTask: async () => undefined,
    };
    return new RecordingWorker(
        'test-tg',
        registry as any,
        redis as any,
        new PluginRegistry(),
        workspaceManager as any
    );
}

function makeCallerContext(redis: any): AgentContext {
    return new AgentContext('s1', 't1', redis, 'caller_agent', 'parent-msg');
}

/** A sub-agent's reply, shaped the way enqueueAgentReturn shapes it. */
function reply(params: {
    taskGroupId: string;
    taskMessageId: string;
    sourceAgentType: string;
    status?: string;
    content?: string;
    replyData?: unknown;
}): ResumeCommand {
    return new ResumeCommand(
        new MessageHeader('parent-msg', 's1', 't1', {
            sourceAgentType: params.sourceAgentType,
            targetAgentType: 'caller_agent',
            parentMessageId: params.taskMessageId,
            taskGroupId: params.taskGroupId,
        }),
        params.content ?? '',
        params.status ?? AgentState.COMPLETED,
        params.replyData as any
    );
}

describe('Task Group join (protocol v2)', () => {
    test('every target offline still resumes the caller', async () => {
        // Regression for the deadlock where the dispatcher counted the failure
        // itself: `completed` reached `total` inside callAgents, where nothing
        // knows how to resume anyone, so no reply was left to trigger Group Join.
        const redis = new MockRedis(['agent-b']);
        const worker = makeWorker(redis);
        const ctx = makeCallerContext(redis);

        const result = await ctx.callAgents({
            tasks: [{ targetAgentType: 'agent-b', content: 'one' }],
        });
        const groupKey = QueueNames.task_group(result.taskGroupId);
        const msgB = result.dispatchedTasks[0].message_id;

        expect(result.dispatchedTasks[0].status).toBe(AgentState.FAILED);
        // Nothing counted yet — the dispatcher does not book-keep the group.
        expect(redis.data[groupKey][TASK_GROUP_FIELD_COMPLETED]).toBe('0');

        await (worker as any).flushPendingGroupReplies(ctx);
        const callerStream = redis.streams[QueueNames.ctrl_stream('caller_agent')];
        expect(callerStream).toHaveLength(1);

        const flushed = commandFromDict(JSON.parse(callerStream[0]));
        await (worker as any).handleMessage(flushed);

        expect(worker.received).toHaveLength(1);
        const aggregate = (worker.received[0] as ResumeCommand).replyData as any[];
        expect(aggregate).toHaveLength(1);
        expect(aggregate[0].message_id).toBe(msgB);
        expect(aggregate[0].status).toBe(AgentState.FAILED);
        expect(aggregate[0].target_agent_type).toBe('agent-b');
        expect(aggregate[0].reply_data.error_code).toBe('AGENT_TYPE_UNAVAILABLE');
    });

    test('a fast reply preceding an offline sibling still resumes exactly once', async () => {
        const redis = new MockRedis(['agent-c']);
        const worker = makeWorker(redis);
        const ctx = makeCallerContext(redis);

        const result = await ctx.callAgents({
            tasks: [
                { targetAgentType: 'agent-b', content: 'one' },
                { targetAgentType: 'agent-c', content: 'two' },
            ],
        });
        const [msgB, msgC] = result.dispatchedTasks.map((t) => t.message_id);

        await (worker as any).handleMessage(
            reply({ taskGroupId: result.taskGroupId, taskMessageId: msgB, sourceAgentType: 'agent-b', content: 'B result' })
        );
        expect(worker.received).toHaveLength(0);

        await (worker as any).flushPendingGroupReplies(ctx);
        const flushed = commandFromDict(JSON.parse(redis.streams[QueueNames.ctrl_stream('caller_agent')][0]));
        await (worker as any).handleMessage(flushed);

        expect(worker.received).toHaveLength(1);
        const aggregate = (worker.received[0] as ResumeCommand).replyData as any[];
        expect(aggregate.map((a) => a.message_id)).toEqual([msgB, msgC]);
        expect(aggregate[0].status).toBe(AgentState.COMPLETED);
        expect(aggregate[1].status).toBe(AgentState.FAILED);
    });

    test('aggregate follows dispatch order, not completion order, and clears content', async () => {
        const redis = new MockRedis();
        const worker = makeWorker(redis);
        const ctx = makeCallerContext(redis);

        const result = await ctx.callAgents({
            tasks: [
                { targetAgentType: 'agent-b', content: 'one' },
                { targetAgentType: 'agent-c', content: 'two' },
                { targetAgentType: 'agent-d', content: 'three' },
            ],
        });
        const [msgB, msgC, msgD] = result.dispatchedTasks.map((t) => t.message_id);

        for (const [msgId, agentType] of [[msgD, 'agent-d'], [msgC, 'agent-c'], [msgB, 'agent-b']] as const) {
            await (worker as any).handleMessage(
                reply({ taskGroupId: result.taskGroupId, taskMessageId: msgId, sourceAgentType: agentType, content: `${agentType} result` })
            );
        }

        expect(worker.received).toHaveLength(1);
        const resumed = worker.received[0] as ResumeCommand;
        expect((resumed.replyData as any[]).map((a) => a.message_id)).toEqual([msgB, msgC, msgD]);
        expect((resumed.replyData as any[]).map((a) => a.target_agent_type)).toEqual(['agent-b', 'agent-c', 'agent-d']);
        // replyData is the single aggregation channel.
        expect(resumed.content).toBe('');
    });

    test('a group without a protocol stamp keeps the legacy join', async () => {
        const redis = new MockRedis();
        const worker = makeWorker(redis);

        const taskGroupId = 'tg-legacy1';
        // Exactly what a pre-v2 dispatcher wrote: no protocol_version, no task_order.
        await redis.hset(QueueNames.task_group(taskGroupId), {
            [TASK_GROUP_FIELD_TOTAL]: '1',
            [TASK_GROUP_FIELD_COMPLETED]: '0',
        });

        await (worker as any).handleMessage(
            reply({ taskGroupId, taskMessageId: 'msg-b', sourceAgentType: 'agent-b', content: 'B result', replyData: { value: 'b' } })
        );

        expect(worker.received).toHaveLength(1);
        const resumed = worker.received[0] as ResumeCommand;
        expect(resumed.replyData).toEqual({ value: 'b' });
        expect(resumed.content).toBe('B result');
        // Stored under the legacy (caller messageId) key.
        expect(Object.keys(redis.data[QueueNames.task_group_results(taskGroupId)])).toEqual(['parent-msg']);
    });

    test('a reply for an aborted group is discarded and not counted', async () => {
        const redis = new MockRedis();
        const worker = makeWorker(redis);
        const ctx = makeCallerContext(redis);

        const result = await ctx.callAgents({ tasks: [{ targetAgentType: 'agent-b', content: 'one' }] });
        const groupKey = QueueNames.task_group(result.taskGroupId);
        await redis.hset(groupKey, { [TASK_GROUP_FIELD_ABORTED]: '1' });

        await (worker as any).handleMessage(
            reply({
                taskGroupId: result.taskGroupId,
                taskMessageId: result.dispatchedTasks[0].message_id,
                sourceAgentType: 'agent-b',
            })
        );

        expect(worker.received).toHaveLength(0);
        expect(redis.data[groupKey][TASK_GROUP_FIELD_COMPLETED]).toBe('0');
    });

    test('an incomplete result set resumes the caller but logs loudly', async () => {
        const redis = new MockRedis();
        const worker = makeWorker(redis);
        const ctx = makeCallerContext(redis);

        const result = await ctx.callAgents({
            tasks: [
                { targetAgentType: 'agent-b', content: 'one' },
                { targetAgentType: 'agent-c', content: 'two' },
            ],
        });
        const [msgB, msgC] = result.dispatchedTasks.map((t) => t.message_id);

        await (worker as any).handleMessage(
            reply({ taskGroupId: result.taskGroupId, taskMessageId: msgB, sourceAgentType: 'agent-b' })
        );
        // agent-c's result never lands, but the counter still reaches total.
        await redis.hincrby(QueueNames.task_group(result.taskGroupId), TASK_GROUP_FIELD_COMPLETED, 1);

        const errorSpy = jest.spyOn(console, 'error').mockImplementation(() => undefined);
        const aggregate = await (worker as any).aggregateTaskGroup({
            groupKey: QueueNames.task_group(result.taskGroupId),
            resultsKey: QueueNames.task_group_results(result.taskGroupId),
            taskGroupId: result.taskGroupId,
            total: 2,
        });

        expect(aggregate.map((a: any) => a.message_id)).toEqual([msgB]);
        expect(errorSpy).toHaveBeenCalledTimes(1);
        const logged = String(errorSpy.mock.calls[0][0]);
        expect(logged).toContain('expected 2');
        expect(logged).toContain(msgC);
        errorSpy.mockRestore();
    });
});

describe('callAgents dispatch semantics', () => {
    test('stamps protocol_version and task_order after the loop', async () => {
        const redis = new MockRedis();
        const ctx = makeCallerContext(redis);

        const result = await ctx.callAgents({
            tasks: [
                { targetAgentType: 'agent-b', content: 'one', messageId: 'mid-b' },
                { targetAgentType: 'agent-c', content: 'two', messageId: 'mid-c' },
            ],
        });

        const group = redis.data[QueueNames.task_group(result.taskGroupId)];
        expect(group[TASK_GROUP_FIELD_PROTOCOL_VERSION]).toBe(TASK_GROUP_PROTOCOL_V2);
        expect(JSON.parse(group[TASK_GROUP_FIELD_TASK_ORDER])).toEqual(['mid-b', 'mid-c']);
        expect(result.dispatchedTasks.map((t) => t.message_id)).toEqual(['mid-b', 'mid-c']);
    });

    test('rejects an empty task list and a batch-shared messageId', async () => {
        const redis = new MockRedis();
        const ctx = makeCallerContext(redis);

        await expect(ctx.callAgents({ tasks: [] })).rejects.toThrow('at least one task');
        await expect(
            ctx.callAgents({
                tasks: [
                    { targetAgentType: 'agent-b', content: 'one' },
                    { targetAgentType: 'agent-c', content: 'two' },
                ],
                messageId: 'shared',
            })
        ).rejects.toThrow('messageId');

        // Still allowed for a single task, where nothing can collide.
        const single = await ctx.callAgents({
            tasks: [{ targetAgentType: 'agent-b', content: 'one' }],
            messageId: 'single-msg',
        });
        expect(single.status).toBe(AgentState.QUEUED);
        expect(single.dispatchedTasks[0].message_id).toBe('single-msg');
    });

    test('per-task SEND_ANYWAY reaches an agent type with no online worker', async () => {
        const redis = new MockRedis(['agent-b', 'agent-c']);
        const ctx = makeCallerContext(redis);

        const result = await ctx.callAgents({
            tasks: [
                { targetAgentType: 'agent-b', content: 'one' },
                { targetAgentType: 'agent-c', content: 'two', routePolicy: 'SEND_ANYWAY' as any },
            ],
        });

        expect(result.dispatchedTasks[0].status).toBe(AgentState.FAILED);
        expect(result.dispatchedTasks[1].status).toBe(AgentState.QUEUED);
        expect(redis.streams[QueueNames.ctrl_stream('agent-c')]).toHaveLength(1);
        expect(redis.streams[QueueNames.ctrl_stream('agent-b')]).toBeUndefined();
    });

    test('dispatchGroup is an alias producing the same result shape', async () => {
        const redis = new MockRedis();
        const ctx = makeCallerContext(redis);

        const viaAlias = await ctx.dispatchGroup({ tasks: [{ targetAgentType: 'agent-b', content: 'one' }] });
        expect(viaAlias.status).toBe(AgentState.QUEUED);
        expect(viaAlias.dispatchedTasks).toHaveLength(1);
        expect(redis.data[QueueNames.task_group(viaAlias.taskGroupId)][TASK_GROUP_FIELD_PROTOCOL_VERSION])
            .toBe(TASK_GROUP_PROTOCOL_V2);
    });

    test('collectGroupResults returns one entry per sub-task, in dispatch order', async () => {
        const redis = new MockRedis();
        const worker = makeWorker(redis);
        const ctx = makeCallerContext(redis);

        const result = await ctx.callAgents({
            tasks: [
                { targetAgentType: 'agent-b', content: 'one' },
                { targetAgentType: 'agent-c', content: 'two' },
            ],
        });
        const [msgB, msgC] = result.dispatchedTasks.map((t) => t.message_id);

        for (const [msgId, agentType] of [[msgC, 'agent-c'], [msgB, 'agent-b']] as const) {
            await (worker as any).handleMessage(
                reply({ taskGroupId: result.taskGroupId, taskMessageId: msgId, sourceAgentType: agentType, content: `${agentType} result` })
            );
        }

        const collected = await ctx.collectGroupResults(result.taskGroupId, 1);
        expect(collected.map((r) => r.message_id)).toEqual([msgB, msgC]);
    });
});

describe('agent return execution identity', () => {
    test('a reply carries the caller message id so the suspended execution resolves', async () => {
        const redis = new MockRedis();
        const worker = makeWorker(redis);

        // An inbound sub-task: caller "caller_agent" (message parent-msg) called
        // "agent-b" with dispatch message id msg-sub.
        const inbound = new AskAgentCommand(
            new MessageHeader('msg-sub', 's1', 't1', {
                sourceAgentType: 'caller_agent',
                targetAgentType: 'agent-b',
                parentMessageId: 'parent-msg',
            }),
            'do work',
            true
        );

        await (worker as any).enqueueAgentReturn(inbound, AgentState.COMPLETED, { value: 1 }, { content: 'done' });

        const stream = redis.streams[QueueNames.ctrl_stream('caller_agent')];
        expect(stream).toHaveLength(1);
        const sent = commandFromDict(JSON.parse(stream[0])) as ResumeCommand;
        // WorkerRunner reattaches the suspended caller execution by this field.
        expect(sent.header.messageId).toBe('parent-msg');
        // Unique per sibling — what Group Join keys the v2 result hash by.
        expect(sent.header.parentMessageId).toBe('msg-sub');
    });
});

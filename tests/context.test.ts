import { AgentContext } from '../src/context';
import { EventType } from '../src/protocol/event_type';
import { ActionType } from '../src/protocol/action_type';
import { QueueNames } from '../src/constants';
import { RoutePolicy } from '../src/availability';
import { AskAgentCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';

class MockRedis {
    calls: Array<{ name: string; payload: string }> = [];
    private storage: Map<string, string> = new Map();
    private hashStorage: Map<string, Record<string, string>> = new Map();
    private setStorage: Map<string, string[]> = new Map([
        ['agent_type:workers:demo-agent-ts', ['worker-123']],
    ]);
    wakeupDecision: Record<string, unknown> | null = null;
    wakeupDecisions: Record<string, unknown>[] = [];
    onlineAfterWakeup = '';

    setValue(key: string, value: Record<string, unknown>): void {
        this.storage.set(key, JSON.stringify(value));
    }

    setOnline(agentType: string, workerId = 'worker-123'): void {
        this.setStorage.set(`agent_type:workers:${agentType}`, [workerId]);
    }

    async xadd(name: string, _id: string, field: string, payload: string): Promise<string> {
        if (field !== 'data') {
            throw new Error('unexpected field');
        }
        this.calls.push({ name, payload });
        return '1-0';
    }

    async get(key: string): Promise<string | null> {
        // Simulate worker online lease key existing
        if (key.includes('worker:online:')) {
            return '1';
        }
        return this.storage.get(key) ?? null;
    }

    async smembers(key: string): Promise<string[]> {
        // Match key patterns like "byai_gateway:registry:agent_type:workers:demo-agent-ts"
        const matchKey = key.replace(/^byai_gateway:registry:/, '');
        return this.setStorage.get(matchKey) || [];
    }

    async hget(key: string, field: string): Promise<string | null> {
        const hash = this.hashStorage.get(key);
        return hash ? (hash[field] ?? null) : null;
    }

    async hset(key: string, mapping: Record<string, string>): Promise<number> {
        const existing = this.hashStorage.get(key) || {};
        for (const [k, v] of Object.entries(mapping)) {
            existing[k] = v;
        }
        this.hashStorage.set(key, existing);
        return 0;
    }

    async expire(_key: string, _seconds: number): Promise<number> {
        return 1;
    }

    async zrangebyscore(_key: string, _min: string, _max: string): Promise<string[]> {
        return ['worker-123'];
    }

    async xread(..._args: unknown[]): Promise<unknown> {
        const decision = this.wakeupDecisions.shift() || this.wakeupDecision;
        if (!decision) return null;
        if (this.onlineAfterWakeup) this.setOnline(this.onlineAfterWakeup);
        return [['result-stream', [[`${Date.now()}-0`, ['data', JSON.stringify(decision)]]]]];
    }

    async hgetall(key: string): Promise<Record<string, string> | null> {
        return this.hashStorage.get(key) || null;
    }

    pipeline() {
        const self = this;
        const pipe = {
            xadd: (name: string, id: string, field: string, payload: string) => {
                self.xadd(name, id, field, payload);
                return pipe;
            },
            hset: (_key: string, _fieldOrMapping: string | Record<string, string>, _value?: string) => {
                return pipe;
            },
            expire: (_key: string, _seconds: number) => {
                return pipe;
            },
            exec: async () => {
                return [];
            }
        };
        return pipe;
    }
}

describe('AgentContext data message format', () => {
    test('emitChunk writes latest stream format with timestamp and OpenAI choices', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext('sess-1', 'trace-1', redis as any, 'agent-a', 'msg-1');

        await ctx.emitChunk('hello');

        expect(redis.calls.length).toBe(1);
        expect(redis.calls[0].name).toBe('byai_gateway:session:sess-1:data_stream');

        const payload = JSON.parse(redis.calls[0].payload);
        expect(payload.trace_id).toBe('trace-1');
        expect(payload.session_id).toBe('sess-1');
        expect(payload.source_agent_type).toBe('agent-a');
        expect(payload.event_type).toBe(EventType.ANSWER_DELTA);
        expect(typeof payload.timestamp).toBe('number');
        expect(payload.data.contentType).toBe('1002');
        expect(payload.data.choices[0].delta.content).toBe('hello');
        expect(payload.data.choices[0].delta.role).toBe('assistant');
        expect(payload.state_msg).toBe('');
        expect(payload.artifact_url).toBe('');
        expect(payload.metadata).toEqual({});
        expect(payload).not.toHaveProperty('chunk_content');
    });

    test('emitState keeps state_msg and uses empty data object', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext('sess-2', 'trace-2', redis as any, 'agent-b', 'msg-2');

        await ctx.emitState('thinking');

        const payload = JSON.parse(redis.calls[0].payload);
        expect(payload.event_type).toBe(EventType.REASONING_LOG_DELTA);
        expect(payload.data.contentType).toBe('3003');
        expect(payload.data.choices[0].delta.content).toBe('thinking');
        expect(payload.data.choices[0].delta.role).toBe(null);
    });

    test('callAgent returns targetAgentType and queues the message', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext('sess-3', 'trace-3', redis as any, 'agent-c', 'msg-3');

        const result = await ctx.callAgent({
            targetAgentType: 'demo-agent-ts',
            content: 'delegate this',
        });

        expect(result.status).toBe('QUEUED');
        expect(result.targetAgentType).toBe('demo-agent-ts');
        expect(result).not.toHaveProperty('targetAgentId');
        expect(typeof result.messageId).toBe('string');

        expect(redis.calls.length).toBe(1);
        expect(redis.calls[0].name).toBe('byai_gateway:ctrl:agent_type:demo-agent-ts');

        const payload = JSON.parse(redis.calls[0].payload);
        expect(payload.action_type).toBe(ActionType.ASK_AGENT);
        expect(payload.header.target_agent_type).toBe('demo-agent-ts');
        expect(payload.header.source_agent_type).toBe('agent-c');
        expect(payload.header.session_id).toBe('sess-3');
        expect(payload.header.parent_message_id).toBe('msg-3');
        expect(payload.body.content).toBe('delegate this');
        expect(payload.body.wait_for_reply).toBe(true);
    });

    test('callAgent SEND_ANYWAY publishes for an offline AgentType', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext('sess-send', 'trace-send', redis as any, 'caller', 'parent');
        const result = await ctx.callAgent({
            targetAgentType: 'cold-agent', content: 'work', routePolicy: RoutePolicy.SEND_ANYWAY,
        });
        expect(result.status).toBe('QUEUED');
        expect(redis.calls.map(call => call.name)).toEqual([QueueNames.ctrl_stream('cold-agent')]);
    });

    test('callAgent accepts object content and canonical extraPayload', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext('sess-object', 'trace-object', redis as any, 'caller', 'parent');
        await ctx.callAgent({
            targetAgentType: 'demo-agent-ts',
            content: { role: 'user', content: { text: 'hello' } },
            extraPayload: { source: 'orchestrator' },
        });
        const command = JSON.parse(redis.calls[0].payload);
        expect(command.body.content).toEqual({ role: 'user', content: { text: 'hello' } });
        expect(command.body.extra_payload).toEqual({ source: 'orchestrator' });
    });

    test('callAgent QUEUE_ONLY stores a pending delivery without target publish', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext('sess-queue', 'trace-queue', redis as any, 'caller', 'parent');
        const result = await ctx.callAgent({
            targetAgentType: 'cold-agent', content: 'work', routePolicy: RoutePolicy.QUEUE_ONLY,
            region: 'cn', priority: 7,
        });
        expect(result.status).toBe('QUEUED');
        expect(redis.calls.map(call => call.name)).toEqual([QueueNames.control_plane_delivery_pending_stream()]);
        const pending = JSON.parse(redis.calls[0].payload);
        expect(pending.delivery_stream).toBe(QueueNames.ctrl_stream('cold-agent'));
        expect(pending.region).toBe('cn');
        expect(pending.priority).toBe(7);
    });

    test('callAgent WAKE_AND_QUEUE requests wakeup and stores pending delivery', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext('sess-wq', 'trace-wq', redis as any, 'caller', 'parent');
        const result = await ctx.callAgent({
            targetAgentType: 'cold-agent', content: 'work', routePolicy: RoutePolicy.WAKE_AND_QUEUE,
        });
        expect(result.status).toBe('QUEUED');
        expect(redis.calls.map(call => call.name)).toEqual([
            QueueNames.control_plane_wakeup_stream(), QueueNames.control_plane_delivery_pending_stream(),
        ]);
    });

    test('callAgent WAKE_AND_WAIT publishes only after READY and membership recheck', async () => {
        const redis = new MockRedis();
        redis.wakeupDecision = { status: 'READY' };
        redis.onlineAfterWakeup = 'cold-agent';
        const ctx = new AgentContext('sess-ww', 'trace-ww', redis as any, 'caller', 'parent');
        const result = await ctx.callAgent({
            targetAgentType: 'cold-agent', content: 'work', routePolicy: RoutePolicy.WAKE_AND_WAIT,
            availabilityTimeoutMs: 10,
        });
        expect(result.status).toBe('QUEUED');
        expect(redis.calls.map(call => call.name)).toEqual([
            QueueNames.control_plane_wakeup_stream(), QueueNames.ctrl_stream('cold-agent'),
        ]);
    });

    test('callAgent WAKE_AND_WAIT continues through STARTING until READY', async () => {
        const redis = new MockRedis();
        redis.wakeupDecisions = [{ status: 'STARTING' }, { status: 'READY' }];
        redis.onlineAfterWakeup = 'cold-agent';
        const ctx = new AgentContext('sess-start', 'trace-start', redis as any, 'caller', 'parent');
        const result = await ctx.callAgent({
            targetAgentType: 'cold-agent', content: 'work', routePolicy: RoutePolicy.WAKE_AND_WAIT,
            availabilityTimeoutMs: 50,
        });
        expect(result.status).toBe('QUEUED');
        expect(redis.calls.at(-1)?.name).toBe(QueueNames.ctrl_stream('cold-agent'));
    });

    test('callAgent rejects an unsupported policy even when the AgentType is online', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext('sess-invalid', 'trace-invalid', redis as any, 'caller', 'parent');
        const result = await ctx.callAgent({
            targetAgentType: 'demo-agent-ts', content: 'work', routePolicy: 'INVALID' as any,
        });
        expect(result.status).toBe('FAILED');
        expect(result.messageId).not.toBe('');
        expect(redis.calls).toEqual([]);
    });

    test('callAgent routes an offline AgentType to an online configured fallback', async () => {
        const redis = new MockRedis();
        redis.setValue(QueueNames.control_plane_agent_fallback('cold-agent'), { selected_agent_type: 'warm-agent' });
        redis.setOnline('warm-agent');
        const ctx = new AgentContext('sess-fb', 'trace-fb', redis as any, 'caller', 'parent');
        const result = await ctx.callAgent({ targetAgentType: 'cold-agent', content: 'work' });
        expect(result.targetAgentType).toBe('warm-agent');
        expect(redis.calls.map(call => call.name)).toEqual([QueueNames.ctrl_stream('warm-agent')]);
        expect(JSON.parse(redis.calls[0].payload).header.target_agent_type).toBe('warm-agent');
    });

    test('callAgent rejects an open circuit without delivery writes', async () => {
        const redis = new MockRedis();
        redis.setValue(QueueNames.control_plane_agent_circuit('demo-agent-ts'), { state: 'OPEN', reason: 'maintenance' });
        const ctx = new AgentContext('sess-circuit', 'trace-circuit', redis as any, 'caller', 'parent');
        const result = await ctx.callAgent({ targetAgentType: 'demo-agent-ts', content: 'work' });
        expect(result.status).toBe('FAILED');
        expect(result.error_code).toBe('AGENT_CIRCUIT_OPEN');
        expect(redis.calls).toEqual([]);
    });

    test('callAgent rejects an exhausted user quota before delivery', async () => {
        const redis = new MockRedis();
        redis.setValue(QueueNames.control_plane_user_quota('user-1'), { available: false, reason: 'limit' });
        const currentCommand = new AskAgentCommand(
            new MessageHeader('parent', 'sess-quota', 'trace-quota', { userCode: 'user-1' }),
            'current'
        );
        const ctx = new AgentContext('sess-quota', 'trace-quota', redis as any, 'caller', 'parent', currentCommand);
        const result = await ctx.callAgent({
            targetAgentType: 'demo-agent-ts', content: 'work',
            routePolicy: RoutePolicy.SEND_ANYWAY,
        });
        expect(result.status).toBe('FAILED');
        expect(result.error_code).toBe('TENANT_QUOTA_EXCEEDED');
        expect(redis.calls).toEqual([]);
    });

    test('callAgent WAKE_AND_WAIT times out without target delivery', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext('sess-timeout', 'trace-timeout', redis as any, 'caller', 'parent');
        const result = await ctx.callAgent({
            targetAgentType: 'cold-agent', content: 'work', routePolicy: RoutePolicy.WAKE_AND_WAIT,
            availabilityTimeoutMs: 0,
        });
        expect(result.status).toBe('FAILED');
        expect(result.error_code).toBe('AGENT_TYPE_UNAVAILABLE');
        expect(redis.calls.map(call => call.name)).toEqual([QueueNames.control_plane_wakeup_stream()]);
    });

    test('isCancelRequested is false by default', () => {
        const ctx = new AgentContext('sess-4', 'trace-4', new MockRedis() as any);
        expect(ctx.isCancelRequested()).toBe(false);
    });

    test('checkCancelled throws when cancel event is set', async () => {
        const event = { is_set: true } as any;
        const ctx = new AgentContext('sess-5', 'trace-5', new MockRedis() as any, 'agent-e', 'msg-5', undefined, event, 'user aborted');

        await expect(ctx.checkCancelled()).rejects.toThrow('user aborted');
    });
});

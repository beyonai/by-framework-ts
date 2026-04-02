import { AgentContext } from '../src/context';
import { EventType } from '../src/protocol/event_type';
import { ActionType } from '../src/protocol/action_type';

class MockRedis {
    calls: Array<{ name: string; payload: string }> = [];
    private storage: Map<string, string> = new Map();
    private hashStorage: Map<string, Record<string, string>> = new Map();
    private setStorage: Map<string, string[]> = new Map([
        ['capability:workers:demo-agent-ts', ['worker-123']],
    ]);

    async xadd(name: string, _id: string, field: string, payload: string): Promise<string> {
        if (field !== 'data') {
            throw new Error('unexpected field');
        }
        this.calls.push({ name, payload });
        return '1-0';
    }

    async smembers(key: string): Promise<string[]> {
        // Match key patterns like "byai_gateway:registry:capability:workers:demo-agent-ts"
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
        expect(payload.source_agent_id).toBe('agent-a');
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
        expect(redis.calls[0].name).toBe('byai_gateway:ctrl:capability:demo-agent-ts');

        const payload = JSON.parse(redis.calls[0].payload);
        expect(payload.action_type).toBe(ActionType.ASK_AGENT);
        expect(payload.header.target_agent_type).toBe('demo-agent-ts');
        expect(payload.header.source_agent_id).toBe('agent-c');
        expect(payload.header.session_id).toBe('sess-3');
        expect(payload.header.parent_message_id).toBe('msg-3');
        expect(payload.body.content).toBe('delegate this');
        expect(payload.body.wait_for_reply).toBe(true);
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

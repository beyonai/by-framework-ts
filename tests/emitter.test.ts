import { GatewayDataEmitter } from '../src/emitter';
import { EventType } from '../src/protocol/event_type';

class MockRedis {
    calls: Array<{ name: string; payload: string }> = [];

    async xadd(name: string, _id: string, field: string, payload: string): Promise<string> {
        this.calls.push({ name, payload });
        return '1-0';
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

/**
 * GatewayDataEmitter 测试，对标 Python 中 emitter 逻辑
 */
describe('GatewayDataEmitter', () => {
    let redis: MockRedis;
    let emitter: GatewayDataEmitter;

    beforeEach(() => {
        redis = new MockRedis();
        emitter = new GatewayDataEmitter(redis as any);
    });

    test('emitEvent writes to session data stream', async () => {
        await emitter.emitEvent({
            sessionId: 'sess-1',
            traceId: 'trace-1',
            eventType: 'custom_event',
            sourceAgentId: 'agent-a',
            messageId: 'msg-1',
        });

        expect(redis.calls).toHaveLength(1);
        expect(redis.calls[0].name).toContain('sess-1');

        const payload = JSON.parse(redis.calls[0].payload);
        expect(payload.session_id).toBe('sess-1');
        expect(payload.trace_id).toBe('trace-1');
        expect(payload.event_type).toBe('custom_event');
        expect(payload.source_agent_id).toBe('agent-a');
        expect(payload.message_id).toBe('msg-1');
        expect(typeof payload.timestamp).toBe('number');
    });

    test('emitChunk uses ANSWER_DELTA event type and SSE layout', async () => {
        await emitter.emitChunk('sess-1', 'trace-1', 'hello world', {
            sourceAgentId: 'agent-a',
            messageId: 'msg-1',
        });

        const payload = JSON.parse(redis.calls[0].payload);
        expect(payload.event_type).toBe(EventType.ANSWER_DELTA);
        expect(payload.data.contentType).toBe('1002');
        expect(payload.data.choices).toHaveLength(1);
        expect(payload.data.choices[0].delta.content).toBe('hello world');
        expect(payload.data.choices[0].delta.role).toBe('assistant');
    });

    test('emitChunk with StreamChunkEvent object', async () => {
        await emitter.emitChunk('sess-2', 'trace-2', {
            content: 'structured content',
            role: 'assistant',
            metadata: { key: 'val' },
        });

        const payload = JSON.parse(redis.calls[0].payload);
        expect(payload.data.choices[0].delta.content).toBe('structured content');
        expect(payload.data.choices[0].delta.role).toBe('assistant');
    });

    test('emitChunk with custom event type', async () => {
        await emitter.emitChunk('sess-3', 'trace-3', 'data', {
            eventType: 'customChunkEvent',
        });

        const payload = JSON.parse(redis.calls[0].payload);
        expect(payload.event_type).toBe('customChunkEvent');
    });

    test('emitState uses REASONING_LOG_DELTA and 3003 content type', async () => {
        await emitter.emitState('sess-1', 'trace-1', 'PROCESSING', {
            sourceAgentId: 'agent-b',
        });

        const payload = JSON.parse(redis.calls[0].payload);
        expect(payload.event_type).toBe(EventType.REASONING_LOG_DELTA);
        expect(payload.data.contentType).toBe('3003');
        expect(payload.data.choices[0].delta.content).toBe('PROCESSING');
        expect(payload.data.choices[0].delta.role).toBeNull();
    });

    test('emitState with StateChangeEvent object', async () => {
        await emitter.emitState('sess-2', 'trace-2', {
            state: 'COMPLETED',
            metadata: { duration: 100 },
        });

        const payload = JSON.parse(redis.calls[0].payload);
        expect(payload.data.choices[0].delta.content).toBe('COMPLETED');
    });

    test('emitArtifact sends artifact URL', async () => {
        await emitter.emitArtifact('sess-1', 'trace-1', 'http://example.com/doc.pdf');

        const payload = JSON.parse(redis.calls[0].payload);
        expect(payload.event_type).toBe(EventType.REASONING_LOG_DELTA);
        // Artifact URL is embedded in SSE layout content as JSON
        const content = payload.data.choices[0].delta.content;
        const files = JSON.parse(content);
        expect(files[0].fileUrl).toBe('http://example.com/doc.pdf');
    });

    test('emitArtifact with ArtifactEvent object', async () => {
        await emitter.emitArtifact('sess-2', 'trace-2', {
            url: 'http://example.com/report.xlsx',
            metadata: { size: 1024 },
        });

        const payload = JSON.parse(redis.calls[0].payload);
        const content = payload.data.choices[0].delta.content;
        const files = JSON.parse(content);
        expect(files[0].fileUrl).toBe('http://example.com/report.xlsx');
    });

    test('askUser sends user input form', async () => {
        await emitter.askUser('sess-1', 'trace-1', 'What is your name?', {
            sourceAgentId: 'agent-c',
        });

        const payload = JSON.parse(redis.calls[0].payload);
        expect(payload.event_type).toBe(EventType.REASONING_LOG_DELTA);

        const content = payload.data.choices[0].delta.content;
        const form = JSON.parse(content);
        expect(form.formStatus).toBe(0);
        expect(form.pluginMachineFields).toHaveLength(1);
        expect(form.pluginMachineFields[0].description).toBe('What is your name?');
        expect(form.pluginMachineFields[0].fieldCode).toBe('user_input');
    });

    test('askUser with AskUserEvent object', async () => {
        await emitter.askUser('sess-2', 'trace-2', {
            prompt: 'Enter your age',
            metadata: { required: true },
        });

        const payload = JSON.parse(redis.calls[0].payload);
        const content = payload.data.choices[0].delta.content;
        const form = JSON.parse(content);
        expect(form.pluginMachineFields[0].description).toBe('Enter your age');
    });

    test('SSE layout contains id, created, contentType, and choices', async () => {
        await emitter.emitChunk('sess-1', 'trace-1', 'test');

        const payload = JSON.parse(redis.calls[0].payload);
        const data = payload.data;

        expect(typeof data.id).toBe('string');
        expect(typeof data.created).toBe('number');
        expect(data.contentType).toBe('1002');
        expect(Array.isArray(data.choices)).toBe(true);
        expect(data.choices[0].index).toBe(0);
        expect(data.choices[0].delta).toBeDefined();
    });
});

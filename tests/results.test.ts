import { AgentState } from '../src/protocol/agent_state';
import { AgentTaskResult, normalizeProcessResult } from '../src/protocol/results';

describe('AgentTaskResult', () => {
    test('normalizes structured result fields', () => {
        const result = normalizeProcessResult(new AgentTaskResult({
            status: AgentState.COMPLETED,
            content: 'done',
            replyData: { answer: 42 },
            metadata: { tokens: 123 },
            extraPayload: { debug_id: 'abc' },
        }));

        expect(result.status).toBe(AgentState.COMPLETED);
        expect(result.content).toBe('done');
        expect(result.replyData).toEqual({ answer: 42 });
        expect(result.metadata).toEqual({ tokens: 123 });
        expect(result.extraPayload).toEqual({ debug_id: 'abc' });
    });

    test('copies legacy metadata without removing replyData copy', () => {
        const result = normalizeProcessResult({
            status: AgentState.COMPLETED,
            answer: '42',
            metadata: { tokens: 123 },
        });

        expect(result.status).toBe(AgentState.COMPLETED);
        expect(result.replyData).toEqual({
            status: AgentState.COMPLETED,
            answer: '42',
            metadata: { tokens: 123 },
        });
        expect(result.metadata).toEqual({ tokens: 123 });
    });

    test('rejects non-json-safe class instances', () => {
        class CustomResult {
            constructor(public readonly value: string) {}
        }

        expect(() => normalizeProcessResult({ item: new CustomResult('x') }))
            .toThrow(/replyData\.item/);
    });
});

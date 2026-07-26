import { serializeByaiContent, deserializeByaiContent, ByaiContentCodec } from '../src/protocol/byai_codec';
import { BaiYingMessageRole } from '../src/protocol/message';

describe('deserializeByaiContent', () => {
    test('passes a plain string through unchanged', () => {
        expect(deserializeByaiContent('hello')).toBe('hello');
    });

    test('passes non-message arrays through unchanged', () => {
        const content = [{ foo: 'bar' }];
        expect(deserializeByaiContent(content)).toBe(content);
    });

    test('passes an empty array through unchanged', () => {
        expect(deserializeByaiContent([])).toEqual([]);
    });

    test('unwraps a single-message wire array into a scalar BaiYingMessage', () => {
        const wire = [{ role: 'user', content: { text: 'hi', files: [], resources: [] } }];
        const result = deserializeByaiContent(wire) as any;
        expect(Array.isArray(result)).toBe(false);
        expect(result.role).toBe(BaiYingMessageRole.USER);
        expect(result.content).toEqual({ text: 'hi', files: [], resources: [] });
    });

    test('decodes a string message content without a MessageContent shape', () => {
        const wire = [{ role: 'assistant', content: 'plain text reply' }];
        const result = deserializeByaiContent(wire) as any;
        expect(result.role).toBe(BaiYingMessageRole.ASSISTANT);
        expect(result.content).toBe('plain text reply');
    });

    test('keeps a multi-message wire array as an array of BaiYingMessage', () => {
        const wire = [
            { role: 'user', content: { text: 'first' } },
            { role: 'assistant', content: { text: 'second' } },
        ];
        const result = deserializeByaiContent(wire) as any[];
        expect(result).toHaveLength(2);
        expect(result[0].role).toBe(BaiYingMessageRole.USER);
        expect(result[0].content.text).toBe('first');
        expect(result[1].role).toBe(BaiYingMessageRole.ASSISTANT);
    });

    test('defaults missing files/resources to empty arrays', () => {
        const wire = [{ role: 'user', content: { text: 'hi' } }];
        const result = deserializeByaiContent(wire) as any;
        expect(result.content.files).toEqual([]);
        expect(result.content.resources).toEqual([]);
    });
});

describe('serializeByaiContent', () => {
    test('passes a plain string through unchanged', () => {
        expect(serializeByaiContent('hello')).toBe('hello');
    });

    test('wraps a scalar BaiYingMessage into a single-item wire array', () => {
        const message = { role: BaiYingMessageRole.USER, content: { text: 'hi', files: [], resources: [] } };
        const wire = serializeByaiContent(message) as any[];
        expect(wire).toEqual([{ role: 'user', content: { text: 'hi', files: [], resources: [] } }]);
    });

    test('serializes an array of BaiYingMessage, normalizing missing files/resources', () => {
        const messages = [{ role: BaiYingMessageRole.USER, content: { text: 'hi' } }];
        const wire = serializeByaiContent(messages) as any[];
        expect(wire).toEqual([{ role: 'user', content: { text: 'hi', files: [], resources: [] } }]);
    });

    test('passes plain dict list items through unchanged', () => {
        const content = [{ foo: 'bar' }];
        expect(serializeByaiContent(content)).toEqual(content);
    });
});

describe('ByaiContentCodec', () => {
    test('round-trips a scalar message through serialize -> deserialize', () => {
        const codec = new ByaiContentCodec();
        const original = { role: BaiYingMessageRole.USER, content: { text: 'round trip', files: [], resources: [] } };
        const wire = codec.serialize(original);
        const decoded = codec.deserialize(wire) as any;
        expect(decoded).toEqual(original);
    });
});

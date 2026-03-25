import { ByaiMessageInterceptor } from '../src/interceptors';
import { BaiYingMessageRole } from '../src/protocol/message';

/**
 * ByaiMessageInterceptor 测试，对标 Python/Java interceptor 逻辑
 */
describe('ByaiMessageInterceptor', () => {
    const interceptor = new ByaiMessageInterceptor();

    test('string content passes through unchanged', () => {
        const result = interceptor.beforeSend({
            targetAgentType: 'agent-x',
            sessionId: 'sess-1',
            content: 'hello world',
        });

        expect(result.content).toBe('hello world');
    });

    test('single BaiYingMessage is converted to list of maps', () => {
        const message = {
            role: BaiYingMessageRole.USER,
            content: 'hello from user',
        };

        const result = interceptor.beforeSend({
            targetAgentType: 'agent-x',
            sessionId: 'sess-1',
            content: message,
        });

        expect(Array.isArray(result.content)).toBe(true);
        expect(result.content).toHaveLength(1);
        expect(result.content[0].role).toBe('user');
        expect(result.content[0].content).toBe('hello from user');
    });

    test('list of BaiYingMessages converted to list of maps', () => {
        const messages = [
            { role: 'user', content: 'question' },
            { role: 'assistant', content: 'answer' },
        ];

        const result = interceptor.beforeSend({
            targetAgentType: 'agent-x',
            sessionId: 'sess-1',
            content: messages,
        });

        expect(result.content).toHaveLength(2);
        expect(result.content[0].role).toBe('user');
        expect(result.content[0].content).toBe('question');
        expect(result.content[1].role).toBe('assistant');
        expect(result.content[1].content).toBe('answer');
    });

    test('message with structured MessageContent preserves content object', () => {
        const message = {
            role: 'user',
            content: {
                text: 'check this file',
                files: [
                    {
                        fileId: 1,
                        fileUrl: 'http://example.com/doc.pdf',
                        fileType: 'file',
                        fileName: 'doc.pdf',
                    },
                ],
            },
        };

        const result = interceptor.beforeSend({
            targetAgentType: 'agent-x',
            sessionId: 'sess-1',
            content: message,
        });

        expect(result.content).toHaveLength(1);
        expect(result.content[0].role).toBe('user');
        expect(result.content[0].content.text).toBe('check this file');
        expect(result.content[0].content.files).toHaveLength(1);
        expect(result.content[0].content.files[0].fileUrl).toBe('http://example.com/doc.pdf');
    });

    test('other params fields are preserved', () => {
        const result = interceptor.beforeSend({
            targetAgentType: 'agent-x',
            sessionId: 'sess-1',
            content: 'test',
            tenantId: 'tenant-1',
            actionType: 'ASK_AGENT',
            parentMessageId: 'parent-1',
            metadata: { key: 'value' },
        });

        expect(result.targetAgentType).toBe('agent-x');
        expect(result.sessionId).toBe('sess-1');
        expect(result.tenantId).toBe('tenant-1');
        expect(result.actionType).toBe('ASK_AGENT');
        expect(result.parentMessageId).toBe('parent-1');
        expect(result.metadata.key).toBe('value');
    });
});

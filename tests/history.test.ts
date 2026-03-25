import { InMemoryHistoryStorage, HistoryProvider } from '../src/history';

/**
 * History 持久化测试，对标 Python test_history_persistence.py
 */
describe('InMemoryHistoryStorage', () => {
    test('saveMessage and getSessionHistory round trip', async () => {
        const storage = new InMemoryHistoryStorage();
        await storage.saveMessage('sess-1', 'user', 'hello');
        await storage.saveMessage('sess-1', 'assistant', 'hi there');

        const history = await storage.getSessionHistory('sess-1');
        expect(history).toHaveLength(2);
        expect(history[0].role).toBe('user');
        expect(history[0].content).toBe('hello');
        expect(history[1].role).toBe('assistant');
        expect(history[1].content).toBe('hi there');
    });

    test('getSessionHistory respects limit', async () => {
        const storage = new InMemoryHistoryStorage();
        for (let i = 0; i < 20; i++) {
            await storage.saveMessage('sess-2', 'user', `message ${i}`);
        }

        const limited = await storage.getSessionHistory('sess-2', 5);
        expect(limited).toHaveLength(5);
        // Should return the last 5 messages
        expect(limited[0].content).toBe('message 15');
        expect(limited[4].content).toBe('message 19');
    });

    test('getSessionHistory returns all when limit is 0', async () => {
        const storage = new InMemoryHistoryStorage();
        for (let i = 0; i < 5; i++) {
            await storage.saveMessage('sess-3', 'user', `msg ${i}`);
        }

        const all = await storage.getSessionHistory('sess-3', 0);
        expect(all).toHaveLength(5);
    });

    test('sessions are isolated', async () => {
        const storage = new InMemoryHistoryStorage();
        await storage.saveMessage('sess-a', 'user', 'message for A');
        await storage.saveMessage('sess-b', 'user', 'message for B');

        const historyA = await storage.getSessionHistory('sess-a');
        const historyB = await storage.getSessionHistory('sess-b');

        expect(historyA).toHaveLength(1);
        expect(historyA[0].content).toBe('message for A');
        expect(historyB).toHaveLength(1);
        expect(historyB[0].content).toBe('message for B');
    });

    test('empty session returns empty array', async () => {
        const storage = new InMemoryHistoryStorage();
        const history = await storage.getSessionHistory('nonexistent');
        expect(history).toEqual([]);
    });

    test('metadata is preserved', async () => {
        const storage = new InMemoryHistoryStorage();
        await storage.saveMessage('sess-m', 'user', 'with meta', { trace_id: 't-1', agent_id: 'a-1' });

        const history = await storage.getSessionHistory('sess-m');
        expect(history[0].metadata).toEqual({ trace_id: 't-1', agent_id: 'a-1' });
    });
});

describe('HistoryProvider', () => {
    test('setStorage switches backend', async () => {
        const storage1 = new InMemoryHistoryStorage();
        const storage2 = new InMemoryHistoryStorage();

        HistoryProvider.setStorage(storage1);
        await HistoryProvider.saveMessage('sess-1', 'user', 'in storage 1');

        HistoryProvider.setStorage(storage2);
        await HistoryProvider.saveMessage('sess-1', 'user', 'in storage 2');

        // storage1 should have the first message
        const h1 = await storage1.getSessionHistory('sess-1');
        expect(h1).toHaveLength(1);
        expect(h1[0].content).toBe('in storage 1');

        // storage2 should have the second message
        const h2 = await storage2.getSessionHistory('sess-1');
        expect(h2).toHaveLength(1);
        expect(h2[0].content).toBe('in storage 2');
    });

    test('delegates to underlying storage', async () => {
        const storage = new InMemoryHistoryStorage();
        HistoryProvider.setStorage(storage);

        await HistoryProvider.saveMessage('sess-2', 'assistant', 'response');
        const history = await HistoryProvider.getSessionHistory('sess-2');

        expect(history).toHaveLength(1);
        expect(history[0].role).toBe('assistant');
        expect(history[0].content).toBe('response');
    });
});

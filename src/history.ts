export interface HistoryMessage {
    role: string;
    content: string | Record<string, any>;
    metadata?: Record<string, any>;
}

export interface BaseHistoryStorage {
    saveMessage(
        sessionId: string,
        role: string,
        content: string | Record<string, any>,
        metadata?: Record<string, any>
    ): Promise<void>;
    getSessionHistory(sessionId: string, limit?: number): Promise<HistoryMessage[]>;
}

export class InMemoryHistoryStorage implements BaseHistoryStorage {
    private readonly data = new Map<string, HistoryMessage[]>();

    async saveMessage(
        sessionId: string,
        role: string,
        content: string | Record<string, any>,
        metadata?: Record<string, any>
    ): Promise<void> {
        if (!this.data.has(sessionId)) {
            this.data.set(sessionId, []);
        }
        this.data.get(sessionId)!.push({ role, content, metadata: metadata || {} });
    }

    async getSessionHistory(sessionId: string, limit: number = 10): Promise<HistoryMessage[]> {
        const history = this.data.get(sessionId) || [];
        if (limit <= 0) {
            return [...history];
        }
        return history.slice(-limit);
    }
}

export class HistoryProvider {
    private static storage: BaseHistoryStorage = new InMemoryHistoryStorage();

    static setStorage(storage: BaseHistoryStorage): void {
        this.storage = storage;
    }

    static async saveMessage(
        sessionId: string,
        role: string,
        content: string | Record<string, any>,
        metadata?: Record<string, any>
    ): Promise<void> {
        await this.storage.saveMessage(sessionId, role, content, metadata);
    }

    static async getSessionHistory(sessionId: string, limit: number = 10): Promise<HistoryMessage[]> {
        return this.storage.getSessionHistory(sessionId, limit);
    }
}


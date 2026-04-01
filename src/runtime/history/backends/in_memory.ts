/**
 * In-memory history storage implementation.
 *
 * Provides a simple in-memory storage backend for session history.
 * Suitable for development and testing.
 */

import { BaseHistoryBackend, HistoryMessage } from '../base';

export class InMemoryHistoryBackend implements BaseHistoryBackend {
  private readonly data = new Map<string, HistoryMessage[]>();

  async getHistory(sessionId: string, limit: number = 10): Promise<HistoryMessage[]> {
    const messages = this.data.get(sessionId) || [];
    return messages.slice(-limit);
  }

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
}

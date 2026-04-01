/**
 * Session management for agent runtime.
 *
 * Provides simple session-level management.
 */

import { FileManager } from './file_manager';
import { FileStorage } from './filestore';
import { BaseHistoryBackend, HistoryMessage } from './history';
import { InMemoryHistoryBackend } from './history';

export class SessionManager {
  private readonly sessionId: string;
  private readonly tenantId?: string;
  private readonly fileManager: FileManager;
  private historyBackend: BaseHistoryBackend;
  private messageCount = 0;

  constructor(
    sessionId: string,
    tenantId?: string,
    storage?: FileStorage,
    workspaceDir?: string
  ) {
    this.sessionId = sessionId;
    this.tenantId = tenantId;
    this.fileManager = new FileManager(sessionId, storage, workspaceDir);
    this.historyBackend = new InMemoryHistoryBackend();
  }

  get session_id(): string {
    return this.sessionId;
  }

  get tenant_id(): string | undefined {
    return this.tenantId;
  }

  get file_manager(): FileManager {
    return this.fileManager;
  }

  get history(): BaseHistoryBackend {
    return this.historyBackend;
  }

  setHistoryBackend(backend: BaseHistoryBackend): void {
    this.historyBackend = backend;
  }

  get message_count(): number {
    return this.messageCount;
  }

  async saveMessage(
    role: string,
    content: string | Record<string, any>,
    metadata?: Record<string, any>
  ): Promise<void> {
    this.messageCount++;
    await this.historyBackend.saveMessage(this.sessionId, role, content, metadata);
  }

  async getHistory(limit: number = 10): Promise<HistoryMessage[]> {
    return this.historyBackend.getHistory(this.sessionId, limit);
  }
}

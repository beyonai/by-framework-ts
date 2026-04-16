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
  private readonly userCode?: string;
  private readonly userName?: string;
  private readonly fileManager: FileManager;
  private historyBackend: BaseHistoryBackend;
  private messageCount = 0;

  constructor(
    sessionId: string,
    userCode?: string,
    userName?: string,
    storage?: FileStorage,
    workspaceDir?: string
  ) {
    this.sessionId = sessionId;
    this.userCode = userCode;
    this.userName = userName;
    this.fileManager = new FileManager(sessionId, storage, workspaceDir);
    this.historyBackend = new InMemoryHistoryBackend();
  }

  get session_id(): string {
    return this.sessionId;
  }

  get user_code(): string | undefined {
    return this.userCode;
  }
  
  get user_name(): string | undefined {
    return this.userName;
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

/**
 * Base history storage interface.
 *
 * Defines the abstract base class for all history storage backends.
 */

export interface HistoryMessage {
  role: string;
  content: string | Record<string, any>;
  metadata?: Record<string, any>;
}

export interface BaseHistoryBackend {
  getHistory(sessionId: string, limit?: number): Promise<HistoryMessage[]>;
  saveMessage(
    sessionId: string,
    role: string,
    content: string | Record<string, any>,
    metadata?: Record<string, any>
  ): Promise<void>;
}

/**
 * PostgreSQL history storage implementation.
 *
 * Provides a PostgreSQL-based storage backend for session history
 * with proper schema initialization and connection pooling.
 */

import { BaseHistoryBackend, HistoryMessage } from '../base';

export interface PostgresHistoryBackendConfig {
  dsn?: string;
  minSize?: number;
  maxSize?: number;
  commandTimeout?: number;
  pool?: any;
}

const CREATE_TABLE_SQL = `
CREATE TABLE IF NOT EXISTS gateway_session_messages (
    id BIGSERIAL PRIMARY KEY,
    session_id VARCHAR(128) NOT NULL,
    role VARCHAR(32) NOT NULL CHECK (role IN ('user', 'assistant', 'system', 'tool')),
    content TEXT NOT NULL,
    metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
`;

const CREATE_INDEX_SQL = `
CREATE INDEX IF NOT EXISTS idx_gateway_session_messages_session_created_at
ON gateway_session_messages (session_id, created_at DESC, id DESC);
`;

const INSERT_SQL = `
INSERT INTO gateway_session_messages (session_id, role, content, metadata)
VALUES ($1, $2, $3, $4);
`;

const SELECT_SQL = `
SELECT role, content, metadata
FROM gateway_session_messages
WHERE session_id = $1
ORDER BY created_at DESC, id DESC
LIMIT $2;
`;

export class PostgresHistoryBackend implements BaseHistoryBackend {
  private pool: any = null;
  private readonly externalPool: boolean;
  private readonly dsn: string;
  private readonly minSize: number;
  private readonly maxSize: number;
  private readonly commandTimeout: number;
  private schemaReady = false;
  private poolLock = false;
  private schemaLock = false;

  constructor(config: PostgresHistoryBackendConfig = {}) {
    this.externalPool = config.pool !== undefined;
    this.pool = config.pool || null;
    this.dsn = config.dsn || process.env.BYAI_HISTORY_PG_DSN || '';
    this.minSize = config.minSize ?? 1;
    this.maxSize = config.maxSize ?? 10;
    this.commandTimeout = config.commandTimeout ?? 30.0;
  }

  private async ensurePool(): Promise<boolean> {
    if (this.pool !== null) {
      return true;
    }
    if (!this.dsn) {
      console.warn(
        'PostgresHistoryBackend disabled: missing connection_pool and BYAI_HISTORY_PG_DSN/dsn'
      );
      return false;
    }

    if (this.poolLock) {
      // Wait for pool initialization
      while (this.poolLock) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      return this.pool !== null;
    }

    this.poolLock = true;
    try {
      const { Pool } = require('pg');
      this.pool = new Pool({
        dsn: this.dsn,
        min: this.minSize,
        max: this.maxSize,
        commandTimeout: this.commandTimeout * 1000,
      });
      console.info('PostgresHistoryBackend connection pool initialized');
      return true;
    } catch (err) {
      console.error('Failed to create PostgreSQL pool:', err);
      this.pool = null;
      return false;
    } finally {
      this.poolLock = false;
    }
  }

  private async ensureSchema(): Promise<void> {
    if (this.schemaReady) {
      return;
    }
    if (!(await this.ensurePool())) {
      return;
    }

    if (this.schemaLock) {
      while (this.schemaLock) {
        await new Promise(resolve => setTimeout(resolve, 10));
      }
      return;
    }

    this.schemaLock = true;
    try {
      if (!this.schemaReady) {
        const client = await this.pool.connect();
        try {
          await client.query(CREATE_TABLE_SQL);
          await client.query(CREATE_INDEX_SQL);
          this.schemaReady = true;
        } finally {
          client.release();
        }
      }
    } finally {
      this.schemaLock = false;
    }
  }

  async getHistory(sessionId: string, limit: number = 10): Promise<HistoryMessage[]> {
    if (limit <= 0) {
      return [];
    }
    if (!(await this.ensurePool())) {
      return [];
    }

    await this.ensureSchema();

    const result = await this.pool.query(SELECT_SQL, [sessionId, limit]);

    // SQL fetches recent N rows in DESC order, reverse to chronological order
    const history: HistoryMessage[] = [];
    const rows = result.rows;
    for (let i = rows.length - 1; i >= 0; i--) {
      const row = rows[i];
      history.push({
        role: row.role,
        content: row.content,
        metadata: row.metadata || {},
      });
    }
    return history;
  }

  async saveMessage(
    sessionId: string,
    role: string,
    content: string | Record<string, any>,
    metadata?: Record<string, any>
  ): Promise<void> {
    if (!(await this.ensurePool())) {
      return;
    }

    await this.ensureSchema();
    await this.pool.query(INSERT_SQL, [
      sessionId,
      role,
      typeof content === 'string' ? content : JSON.stringify(content),
      metadata || {},
    ]);
  }

  async close(): Promise<void> {
    if (this.externalPool) {
      return;
    }
    if (this.pool === null) {
      return;
    }
    const closeFn = this.pool.end;
    if (closeFn) {
      await closeFn();
    }
    this.pool = null;
  }
}

/**
 * File management for agent runtime sessions.
 *
 * Provides file operations within an agent session using pluggable storage backends.
 */

import { FileStorage } from './filestore';
import { LocalFileStorage } from './filestore';

export class FileManager {
  private readonly sessionId: string;
  private readonly storage: FileStorage;

  constructor(sessionId: string, storage?: FileStorage, workspaceDir?: string) {
    this.sessionId = sessionId;

    if (storage) {
      this.storage = storage;
    } else {
      const workspace = workspaceDir || 'workspace';
      this.storage = new LocalFileStorage(`${workspace}/session_${sessionId}`);
    }
  }

  get storageInstance(): FileStorage {
    return this.storage;
  }

  get workspaceDir(): string {
    return `session_${this.sessionId}`;
  }

  async initialize(): Promise<void> {
    await this.storage.initialize();
  }

  async shutdown(): Promise<void> {
    await this.storage.shutdown();
  }

  async readFile(filename: string, encoding: BufferEncoding = 'utf-8'): Promise<string> {
    const content = await this.storage.read(filename, encoding);
    return typeof content === 'string' ? content : content.toString(encoding);
  }

  async writeFile(
    filename: string,
    content: string,
    encoding: BufferEncoding = 'utf-8',
    overwrite: boolean = true
  ): Promise<void> {
    if (!overwrite && (await this.storage.exists(filename))) {
      throw new Error(`File ${filename} already exists`);
    }
    await this.storage.write(filename, content, encoding);
  }

  async exists(filename: string): Promise<boolean> {
    return this.storage.exists(filename);
  }

  async isFile(filename: string): Promise<boolean> {
    return this.storage.isFile(filename);
  }

  async isDir(filename: string): Promise<boolean> {
    return this.storage.isDir(filename);
  }

  async listFiles(directory: string = ''): Promise<string[]> {
    const items = await this.storage.list(directory);
    const result: string[] = [];
    for (const item of items) {
      const fullPath = directory ? `${directory}/${item}` : item;
      if (await this.storage.isFile(fullPath)) {
        result.push(item);
      }
    }
    return result;
  }

  async getFileUrl(filename: string, expires: number = 3600): Promise<string> {
    return this.storage.getUrl(filename, expires);
  }
}

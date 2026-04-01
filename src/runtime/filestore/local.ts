/**
 * Local file system storage implementation.
 *
 * Provides file storage backed by local filesystem.
 * Suitable for single-node deployments.
 */

import * as fs from 'fs/promises';
import * as path from 'path';
import { FileStorage } from './base';

export class LocalFileStorage implements FileStorage {
  constructor(private readonly baseDir: string) {}

  async initialize(): Promise<void> {
    await fs.mkdir(this.baseDir, { recursive: true });
  }

  async shutdown(): Promise<void> {
    // No cleanup needed for local filesystem
  }

  async write(filePath: string, content: string | Buffer, encoding: BufferEncoding = 'utf-8'): Promise<void> {
    const fullPath = this.getFullPath(filePath);
    const dir = path.dirname(fullPath);
    await fs.mkdir(dir, { recursive: true });

    if (typeof content === 'string') {
      await fs.writeFile(fullPath, content, encoding);
    } else {
      await fs.writeFile(fullPath, content);
    }
  }

  async read(filePath: string, encoding: string = 'utf-8'): Promise<string | Buffer> {
    const fullPath = this.getFullPath(filePath);
    if (encoding) {
      return fs.readFile(fullPath, encoding as BufferEncoding);
    }
    return fs.readFile(fullPath);
  }

  async delete(filePath: string): Promise<void> {
    const fullPath = this.getFullPath(filePath);
    const stat = await fs.stat(fullPath);
    if (stat.isFile()) {
      await fs.unlink(fullPath);
    } else if (stat.isDirectory()) {
      await fs.rm(fullPath, { recursive: true });
    }
  }

  async exists(filePath: string): Promise<boolean> {
    try {
      await fs.stat(this.getFullPath(filePath));
      return true;
    } catch {
      return false;
    }
  }

  async isFile(filePath: string): Promise<boolean> {
    try {
      const stat = await fs.stat(this.getFullPath(filePath));
      return stat.isFile();
    } catch {
      return false;
    }
  }

  async isDir(filePath: string): Promise<boolean> {
    try {
      const stat = await fs.stat(this.getFullPath(filePath));
      return stat.isDirectory();
    } catch {
      return false;
    }
  }

  async list(dirPath: string = ''): Promise<string[]> {
    const fullPath = this.getFullPath(dirPath);
    try {
      const stat = await fs.stat(fullPath);
      if (!stat.isDirectory()) {
        return [];
      }
    } catch {
      return [];
    }

    const items = await fs.readdir(fullPath);
    return items.map(item => path.relative(fullPath, path.join(fullPath, item)).replace(/\\/g, '/'));
  }

  async getUrl(filePath: string, _expires: number = 3600): Promise<string> {
    return this.getFullPath(filePath).replace(/\\/g, '/');
  }

  private getFullPath(relativePath: string): string {
    // Normalize path to handle both forward and backward slashes
    const normalizedPath = relativePath.replace('/', path.sep).replace('\\', path.sep);
    // Remove leading separators to avoid absolute path issues
    const cleanedPath = normalizedPath.replace(/^[/\\]+/, '');
    return path.join(this.baseDir, cleanedPath);
  }
}

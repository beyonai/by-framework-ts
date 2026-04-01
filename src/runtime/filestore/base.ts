/**
 * File storage abstract interface.
 *
 * Defines the contract for file storage backends.
 */

export interface FileStorage {
  initialize(): Promise<void>;
  shutdown(): Promise<void>;
  write(path: string, content: string | Buffer, encoding?: string): Promise<void>;
  read(path: string, encoding?: string): Promise<string | Buffer>;
  delete(path: string): Promise<void>;
  exists(path: string): Promise<boolean>;
  isFile(path: string): Promise<boolean>;
  isDir(path: string): Promise<boolean>;
  list(path: string): Promise<string[]>;
  getUrl(path: string, expires?: number): Promise<string>;
}

/**
 * MinIO/S3 storage implementation.
 *
 * Provides file storage backed by MinIO or S3-compatible object storage.
 * Suitable for distributed deployments.
 *
 * Note: Requires minio package. Install with: npm install minio
 */

import { FileStorage } from './base';

export interface MinioConfig {
  bucket: string;
  endpoint: string;
  accessKey: string;
  secretKey: string;
  secure?: boolean;
  region?: string;
}

export class MinioFileStorage implements FileStorage {
  private client: any;
  private bucket: string;
  private region: string;
  private minioModule: any;
  private s3Error: any;

  constructor(config: MinioConfig) {
    // Lazy import to avoid hard dependency
    const Minio = require('minio');
    this.minioModule = Minio;
    this.s3Error = require('minio').error;
    this.bucket = config.bucket;
    this.region = config.region || 'us-east-1';
    this.client = new Minio.Client({
      endPoint: config.endpoint,
      accessKey: config.accessKey,
      secretKey: config.secretKey,
      useSSL: config.secure ?? false,
    });
  }

  async initialize(): Promise<void> {
    const exists = await this.client.bucketExists(this.bucket);
    if (!exists) {
      await this.client.makeBucket(this.bucket, this.region);
    }
  }

  async shutdown(): Promise<void> {
    // MinIO SDK doesn't require explicit shutdown
  }

  async write(path: string, content: string | Buffer, encoding: BufferEncoding = 'utf-8'): Promise<void> {
    const data = Buffer.isBuffer(content) ? content : Buffer.from(content, encoding);
    const size = data.length;
    await this.client.putObject(this.bucket, path, data, size);
  }

  async read(path: string, encoding: BufferEncoding = 'utf-8'): Promise<string | Buffer> {
    return new Promise((resolve, reject) => {
      const chunks: Buffer[] = [];
      const stream = this.client.getObject(this.bucket, path);

      stream.on('data', (chunk: Buffer) => chunks.push(chunk));
      stream.on('end', () => {
        const data = Buffer.concat(chunks);
        resolve(data.toString(encoding));
      });
      stream.on('error', reject);
    });
  }

  async delete(path: string): Promise<void> {
    try {
      await this.client.statObject(this.bucket, path);
      await this.client.removeObject(this.bucket, path);
    } catch (err: any) {
      // Ignore if file doesn't exist
      if (err.code !== 'ENOTFOUND' && err.code !== 'ObjectNotFound') {
        throw err;
      }
    }
  }

  async exists(path: string): Promise<boolean> {
    try {
      await this.client.statObject(this.bucket, path);
      return true;
    } catch {
      return false;
    }
  }

  async isFile(path: string): Promise<boolean> {
    try {
      const stat = await this.client.statObject(this.bucket, path);
      return !stat.stat?.isDir;
    } catch {
      return false;
    }
  }

  async isDir(path: string): Promise<boolean> {
    // MinIO/S3 doesn't have real directories, but paths ending with /
    // are treated as directory prefixes
    return path.endsWith('/');
  }

  async list(path: string = ''): Promise<string[]> {
    const prefix = path ? (path.endsWith('/') ? path : path + '/') : '';
    const objects = await this.client.listObjects(this.bucket, prefix, false);

    return new Promise((resolve, reject) => {
      const items: string[] = [];
      objects.on('data', (obj: any) => {
        const relativePath = prefix ? obj.name.slice(prefix.length) : obj.name;
        if (relativePath) {
          items.push(relativePath);
        }
      });
      objects.on('error', reject);
      objects.on('end', () => resolve(items));
    });
  }

  async getUrl(path: string, expires: number = 3600): Promise<string> {
    return this.client.presignedGetObject(this.bucket, path, expires);
  }
}

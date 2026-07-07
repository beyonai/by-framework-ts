import Redis, { Cluster } from 'ioredis';
import { getKeySchemaVersion } from './constants';
import { RedisConnectionError } from './exceptions';

let redisInstance: Redis | null = null;

export type RedisMode = 'standalone' | 'cluster';

export interface RedisClusterNode {
    host: string;
    port: number;
}

export interface RedisConnectionConfig {
    mode?: RedisMode;
    host?: string;
    port?: number;
    db?: number;
    username?: string;
    password?: string;
    clusterNodes?: RedisClusterNode[];
}

function parseClusterNodesFromEnv(): RedisClusterNode[] {
    const raw = process.env.REDIS_CLUSTER_NODES;
    if (!raw) {
        return [];
    }
    return raw.split(',').map((entry) => {
        const idx = entry.lastIndexOf(':');
        return { host: entry.slice(0, idx), port: parseInt(entry.slice(idx + 1), 10) };
    });
}

export function createRedis(options: RedisConnectionConfig = {}): Redis {
    const mode: RedisMode = options.mode || (process.env.REDIS_MODE as RedisMode) || 'standalone';
    const username = options.username || process.env.REDIS_USERNAME;
    const password = options.password || process.env.REDIS_PASSWORD;

    if (mode === 'cluster') {
        // Fail fast, synchronously, before any client is constructed: v1 keys
        // have no Cluster hash tags and will hit CROSSSLOT errors under Cluster.
        if (getKeySchemaVersion() !== 'v2') {
            throw new RedisConnectionError(
                'REDIS_MODE=cluster requires REDIS_KEY_SCHEMA_VERSION=v2 ' +
                    '(v1 key format has no hash tags and will hit CROSSSLOT ' +
                    'errors under Cluster). Set REDIS_KEY_SCHEMA_VERSION=v2 ' +
                    'and complete the key migration first.'
            );
        }
        const clusterNodes = options.clusterNodes || parseClusterNodesFromEnv();
        return new Cluster(clusterNodes, {
            redisOptions: { username, password },
        }) as unknown as Redis;
    }

    const host = options.host || process.env.REDIS_HOST || 'localhost';
    const port = options.port || parseInt(process.env.REDIS_PORT || '6379', 10);
    const db = options.db || parseInt(process.env.REDIS_DB || '0', 10);

    return new Redis({
        host,
        port,
        db,
        username,
        password,
        enableOfflineQueue: true,
    });
}

export function initRedis(options: RedisConnectionConfig = {}): Redis {
    if (!redisInstance) {
        redisInstance = createRedis(options);
    }
    return redisInstance;
}

export function getRedis(): Redis {
    if (!redisInstance) {
        return initRedis();
    }
    return redisInstance;
}

export async function closeRedis(): Promise<void> {
    if (redisInstance) {
        await redisInstance.quit();
        redisInstance = null;
    }
}

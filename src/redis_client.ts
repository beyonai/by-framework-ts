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

function resolveMode(options: RedisConnectionConfig): RedisMode {
    // An explicit `mode` passed by the caller always wins. Otherwise,
    // REDIS_CLUSTER_HOST is the preferred way to opt into Cluster mode: just
    // setting it is enough, no separate REDIS_MODE=cluster required.
    const raw =
        options.mode ??
        (process.env.REDIS_CLUSTER_HOST ? 'cluster' : undefined) ??
        (process.env.REDIS_MODE as RedisMode | undefined) ??
        'standalone';
    if (raw !== 'standalone' && raw !== 'cluster') {
        // A typo (e.g. 'cluser') or the reserved-but-unimplemented 'sentinel'
        // value must not silently fall through to standalone — that would
        // connect to the wrong topology instead of failing fast.
        throw new RedisConnectionError(
            `Invalid REDIS_MODE: '${raw}' (must be 'standalone' or 'cluster'; ` +
                `'sentinel' is reserved for a future phase and not implemented yet)`
        );
    }
    return raw;
}

function parseClusterNodesFromEnv(): RedisClusterNode[] {
    const raw = process.env.REDIS_CLUSTER_HOST || process.env.REDIS_CLUSTER_NODES;
    if (!raw) {
        return [];
    }
    return raw.split(',').map((entry) => {
        const idx = entry.lastIndexOf(':');
        const host = idx === -1 ? '' : entry.slice(0, idx);
        const port = idx === -1 ? NaN : parseInt(entry.slice(idx + 1), 10);
        return { host, port };
    });
}

function assertValidClusterNodes(nodes: RedisClusterNode[]): void {
    if (nodes.length === 0) {
        throw new RedisConnectionError(
            'REDIS_MODE=cluster requires at least one cluster node ' +
                "(pass clusterNodes or set REDIS_CLUSTER_NODES='host:port[,host:port...]')"
        );
    }
    for (const node of nodes) {
        if (!node.host || !Number.isFinite(node.port)) {
            throw new RedisConnectionError(
                `Invalid cluster node '${node.host}:${node.port}' (expected 'host:port' with a numeric port)`
            );
        }
    }
}

export function createRedis(options: RedisConnectionConfig = {}): Redis {
    const mode = resolveMode(options);
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
        assertValidClusterNodes(clusterNodes);
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

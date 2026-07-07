import Redis from 'ioredis';

/** The subset of ioredis.Cluster's API this module needs, checked via duck typing. */
interface ClusterLike {
    nodes(role: 'master' | 'slave' | 'all'): Redis[];
}

function asClusterLike(redis: Redis): ClusterLike | null {
    const maybeCluster = redis as unknown as ClusterLike;
    return typeof maybeCluster.nodes === 'function' ? maybeCluster : null;
}

/**
 * SCAN is a single-node command — under Cluster it only sees keys on the
 * node it was issued against, and ioredis.Cluster doesn't provide a
 * built-in aggregated scan across nodes. This iterates every master node
 * (or just the one connection, for a standalone client) and merges
 * results, so callers get the same complete result set in standalone and
 * cluster modes.
 *
 * Takes the same Redis type every other factory/business-code call site
 * uses (see redis_client.createRedis) even though the underlying instance
 * may actually be a Cluster client at runtime — detected here via duck
 * typing rather than widening the parameter type everywhere.
 */
export async function clusterScanIter(redis: Redis, pattern: string, count = 100): Promise<string[]> {
    const cluster = asClusterLike(redis);
    const nodes: Redis[] = cluster ? cluster.nodes('master') : [redis];
    const perNodeKeys = await Promise.all(nodes.map(node => scanOneNode(node, pattern, count)));
    return [...new Set(perNodeKeys.flat())];
}

async function scanOneNode(node: Redis, pattern: string, count: number): Promise<string[]> {
    const keys: string[] = [];
    let cursor = '0';
    do {
        const [nextCursor, batch] = await node.scan(cursor, 'MATCH', pattern, 'COUNT', count);
        cursor = nextCursor;
        keys.push(...batch);
    } while (cursor !== '0');
    return keys;
}

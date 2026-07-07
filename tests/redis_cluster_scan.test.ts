import { clusterScanIter } from '../src/redis_cluster_scan';

class FakeScanClient {
    public scanCalls: any[][] = [];
    constructor(private pages: string[][]) {}

    async scan(cursor: string, ...args: any[]): Promise<[string, string[]]> {
        this.scanCalls.push([cursor, ...args]);
        const idx = Number(cursor);
        const page = this.pages[idx] || [];
        const nextCursor = idx + 1 < this.pages.length ? String(idx + 1) : '0';
        return [nextCursor, page];
    }
}

class FakeClusterClient {
    constructor(private masterNodes: FakeScanClient[]) {}

    nodes(role: string): FakeScanClient[] {
        if (role !== 'master') {
            throw new Error(`expected 'master' role, got '${role}'`);
        }
        return this.masterNodes;
    }
}

describe('clusterScanIter', () => {
    test('standalone client: paginates a single connection via the cursor until it returns to 0', async () => {
        const client = new FakeScanClient([['k1', 'k2'], ['k3']]);

        const keys = await clusterScanIter(client as any, 'byai_gateway:*');

        expect(keys.sort()).toEqual(['k1', 'k2', 'k3']);
        expect(client.scanCalls).toHaveLength(2);
        expect(client.scanCalls[0]).toEqual(['0', 'MATCH', 'byai_gateway:*', 'COUNT', 100]);
    });

    test('cluster client: iterates every master node and merges results, not just one node\'s subset', async () => {
        const nodeA = new FakeScanClient([['a1', 'a2']]);
        const nodeB = new FakeScanClient([['b1']]);
        const cluster = new FakeClusterClient([nodeA, nodeB]);

        const keys = await clusterScanIter(cluster as any, 'byai_gateway:*');

        expect(keys.sort()).toEqual(['a1', 'a2', 'b1']);
        expect(nodeA.scanCalls).toHaveLength(1);
        expect(nodeB.scanCalls).toHaveLength(1);
    });

    test('cluster client: paginates each node independently before merging', async () => {
        const nodeA = new FakeScanClient([['a1'], ['a2']]);
        const nodeB = new FakeScanClient([['b1'], ['b2'], ['b3']]);
        const cluster = new FakeClusterClient([nodeA, nodeB]);

        const keys = await clusterScanIter(cluster as any, 'byai_gateway:*');

        expect(keys.sort()).toEqual(['a1', 'a2', 'b1', 'b2', 'b3']);
        expect(nodeA.scanCalls).toHaveLength(2);
        expect(nodeB.scanCalls).toHaveLength(3);
    });

    test('deduplicates keys returned more than once', async () => {
        const client = new FakeScanClient([['k1', 'k2'], ['k2', 'k3']]);

        const keys = await clusterScanIter(client as any, 'byai_gateway:*');

        expect(keys.sort()).toEqual(['k1', 'k2', 'k3']);
    });
});

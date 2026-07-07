import { Readable } from 'stream';
import { clusterScanIter } from '../src/redis_cluster_scan';

class FakeScanClient {
    public scanStreamCalls: any[] = [];
    constructor(private pages: string[][]) {}

    scanStream(options: { match?: string; count?: number } = {}): Readable {
        this.scanStreamCalls.push(options);
        const pages = this.pages;
        let i = 0;
        return new Readable({
            objectMode: true,
            read() {
                if (i < pages.length) {
                    this.push(pages[i]);
                    i++;
                } else {
                    this.push(null);
                }
            },
        });
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
    test('standalone client: consumes node.scanStream() with the given match pattern and count', async () => {
        const client = new FakeScanClient([['k1', 'k2'], ['k3']]);

        const keys = await clusterScanIter(client as any, 'byai_gateway:*');

        expect(keys.sort()).toEqual(['k1', 'k2', 'k3']);
        expect(client.scanStreamCalls).toEqual([{ match: 'byai_gateway:*', count: 100 }]);
    });

    test("cluster client: iterates every master node's scanStream() and merges results, not just one node's subset", async () => {
        const nodeA = new FakeScanClient([['a1', 'a2']]);
        const nodeB = new FakeScanClient([['b1']]);
        const cluster = new FakeClusterClient([nodeA, nodeB]);

        const keys = await clusterScanIter(cluster as any, 'byai_gateway:*');

        expect(keys.sort()).toEqual(['a1', 'a2', 'b1']);
        expect(nodeA.scanStreamCalls).toHaveLength(1);
        expect(nodeB.scanStreamCalls).toHaveLength(1);
    });

    test("cluster client: consumes every page of every node's stream before merging", async () => {
        const nodeA = new FakeScanClient([['a1'], ['a2']]);
        const nodeB = new FakeScanClient([['b1'], ['b2'], ['b3']]);
        const cluster = new FakeClusterClient([nodeA, nodeB]);

        const keys = await clusterScanIter(cluster as any, 'byai_gateway:*');

        expect(keys.sort()).toEqual(['a1', 'a2', 'b1', 'b2', 'b3']);
    });

    test('deduplicates keys returned more than once', async () => {
        const client = new FakeScanClient([['k1', 'k2'], ['k2', 'k3']]);

        const keys = await clusterScanIter(client as any, 'byai_gateway:*');

        expect(keys.sort()).toEqual(['k1', 'k2', 'k3']);
    });
});

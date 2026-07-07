const mockRedisConstructor = jest.fn();
const mockClusterConstructor = jest.fn();

jest.mock('ioredis', () => ({
    __esModule: true,
    default: jest.fn().mockImplementation((options) => {
        mockRedisConstructor(options);
        return {
            options,
            quit: jest.fn().mockResolvedValue('OK'),
        };
    }),
    Cluster: jest.fn().mockImplementation((nodes, options) => {
        mockClusterConstructor(nodes, options);
        return {
            nodes,
            options,
            quit: jest.fn().mockResolvedValue('OK'),
        };
    }),
}));

describe('redis_client', () => {
    const originalEnv = { ...process.env };

    beforeEach(() => {
        jest.resetModules();
        mockRedisConstructor.mockClear();
        mockClusterConstructor.mockClear();
        process.env = { ...originalEnv };
        delete process.env.REDIS_MODE;
        delete process.env.REDIS_KEY_SCHEMA_VERSION;
        delete process.env.REDIS_CLUSTER_NODES;
        delete process.env.REDIS_DB;
    });

    afterAll(() => {
        process.env = originalEnv;
    });

    test('createRedis passes username and password to ioredis', async () => {
        const { createRedis } = await import('../src/redis_client');

        createRedis({
            host: 'redis.example.com',
            port: 6380,
            db: 2,
            username: 'app-user',
            password: 'secret-pass',
        } as any);

        expect(mockRedisConstructor).toHaveBeenCalledWith({
            host: 'redis.example.com',
            port: 6380,
            db: 2,
            username: 'app-user',
            password: 'secret-pass',
            enableOfflineQueue: true,
        });
    });

    test('createRedis reads db from REDIS_DB env var', async () => {
        process.env.REDIS_DB = '3';
        const { createRedis } = await import('../src/redis_client');

        createRedis({ host: 'redis.example.com', port: 6380 } as any);

        expect(mockRedisConstructor).toHaveBeenCalledWith(
            expect.objectContaining({ db: 3 })
        );
    });

    test('defaults to standalone mode and builds a standard Redis client', async () => {
        const { createRedis } = await import('../src/redis_client');

        createRedis({ host: 'redis.example.com', port: 6380 } as any);

        expect(mockRedisConstructor).toHaveBeenCalled();
        expect(mockClusterConstructor).not.toHaveBeenCalled();
    });

    test('mode=cluster with default (v1) key schema version fails fast without constructing any client', async () => {
        process.env.REDIS_MODE = 'cluster';
        const { createRedis } = await import('../src/redis_client');

        expect(() =>
            createRedis({
                mode: 'cluster',
                clusterNodes: [{ host: 'unreachable-host.invalid', port: 6379 }],
            })
        ).toThrow(/REDIS_MODE=cluster requires REDIS_KEY_SCHEMA_VERSION=v2/);

        expect(mockClusterConstructor).not.toHaveBeenCalled();
        expect(mockRedisConstructor).not.toHaveBeenCalled();
    });

    test('mode=cluster with key schema v2 constructs a Cluster client', async () => {
        process.env.REDIS_MODE = 'cluster';
        process.env.REDIS_KEY_SCHEMA_VERSION = 'v2';
        const { createRedis } = await import('../src/redis_client');

        createRedis({
            mode: 'cluster',
            clusterNodes: [
                { host: 'node-a', port: 7001 },
                { host: 'node-b', port: 7002 },
            ],
        });

        expect(mockClusterConstructor).toHaveBeenCalledWith(
            [
                { host: 'node-a', port: 7001 },
                { host: 'node-b', port: 7002 },
            ],
            expect.objectContaining({ redisOptions: expect.any(Object) })
        );
        expect(mockRedisConstructor).not.toHaveBeenCalled();
    });

    test('mode=cluster reads cluster_nodes from REDIS_CLUSTER_NODES env var', async () => {
        process.env.REDIS_MODE = 'cluster';
        process.env.REDIS_KEY_SCHEMA_VERSION = 'v2';
        process.env.REDIS_CLUSTER_NODES = 'h1:6379,h2:6380,h3:6381';
        const { createRedis } = await import('../src/redis_client');

        createRedis({ mode: 'cluster' });

        expect(mockClusterConstructor).toHaveBeenCalledWith(
            [
                { host: 'h1', port: 6379 },
                { host: 'h2', port: 6380 },
                { host: 'h3', port: 6381 },
            ],
            expect.any(Object)
        );
    });
});

const mockRedisConstructor = jest.fn();

jest.mock('ioredis', () => ({
    __esModule: true,
    default: jest.fn().mockImplementation((options) => {
        mockRedisConstructor(options);
        return {
            options,
            quit: jest.fn().mockResolvedValue('OK'),
        };
    }),
}));

describe('redis_client', () => {
    beforeEach(() => {
        jest.resetModules();
        mockRedisConstructor.mockClear();
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
});

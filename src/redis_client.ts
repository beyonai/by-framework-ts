import Redis from 'ioredis';

let redisInstance: Redis | null = null;

type RedisOptions = {
    host?: string;
    port?: number;
    db?: number;
    username?: string;
    password?: string;
};

export function createRedis(options: RedisOptions = {}): Redis {
    const host = options.host || process.env.REDIS_HOST || 'localhost';
    const port = options.port || parseInt(process.env.REDIS_PORT || '6379', 10);
    const db = options.db || parseInt(process.env.REDIS_DB || '0', 10);
    const username = options.username || process.env.REDIS_USERNAME;
    const password = options.password || process.env.REDIS_PASSWORD;

    return new Redis({
        host,
        port,
        db,
        username,
        password,
        enableOfflineQueue: true,
    });
}

export function initRedis(options: RedisOptions = {}): Redis {
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

import { GatewayClient, WorkerRegistry, createRedis } from '../src';

async function main() {
    const redis = createRedis({
        host: 'localhost',
        port: 6379,
        db: 0,
        username: process.env.REDIS_USERNAME,
        password: process.env.REDIS_PASSWORD,
    });
    const registry = new WorkerRegistry(redis);
    const client = new GatewayClient(registry, redis);

    const messageId = process.argv[2] || 'msg-to-cancel';
    const sessionId = process.argv[3] || 'session-to-cancel';

    const result = await client.cancelTask({
        messageId,
        sessionId,
        reason: 'manual demo interrupt',
        requestedBy: 'ts-example',
        cancelMode: 'graceful',
    });

    console.log('Cancel result:', result);
    await redis.quit();
}

main().catch(console.error);

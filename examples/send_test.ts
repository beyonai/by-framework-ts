import { GatewayClient, createRedis, WorkerRegistry } from '../src';

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

    console.log('Sending message to demo-agent-ts...');
    const res = await client.sendMessage({
        targetAgentType: 'demo-agent-ts',
        sessionId: 'test-session-ts',
        content: 'Hello from verification script!',
        userCode: 'test-tenant'
    });

    console.log('Send result:', res);

    // Wait a bit for processing
    await new Promise(resolve => setTimeout(resolve, 2000));
    await redis.quit();
}

main().catch(console.error);

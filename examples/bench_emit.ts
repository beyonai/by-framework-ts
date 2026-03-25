import { getRedis } from '../src/redis_client';
import { AgentContext } from '../src/context';
import { QueueNames } from '../src/constants';

async function benchmark() {
    const redis = getRedis();
    const context = new AgentContext('bench-session', 'bench-trace', redis, 'bench-agent');

    console.log('Starting benchmark: 100 chunks...');
    const start = Date.now();
    for (let i = 0; i < 100; i++) {
        await context.emitChunk({ content: 'a' });
    }
    const end = Date.now();
    console.log(`Total time for 100 chunks: ${end - start}ms`);
    console.log(`Average time per chunk: ${(end - start) / 100}ms`);

    process.exit(0);
}

benchmark().catch(console.error);

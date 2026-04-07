import {
    WorkerRunner,
    WorkerRegistry,
    GatewayDataEmitter,
    createRedis
} from '../src';

/**
 * 演示原子能力的独立使用，不依赖任何基类或预设流程。
 */
async function main() {
    const redis = createRedis();
    const workerId = "atomic-demo-worker";
    const agentTypes = ["atomic-agent-type"];

    console.log("=== 原子能力演示 ===");

    // 1. 原子注册与心跳 (可选)
    const registry = new WorkerRegistry(redis);
    await registry.registerWorker(workerId, agentTypes);
    console.log("[1] 注册成功");

    // 2. 原子获取消息
    // 无需创建完整的 Worker 实例，只需提供必要元数据
    const runner = new WorkerRunner({ workerId, agentTypes }, { redisClient: redis });
    // 注意：手动模式下用户需要根据需要调用 runner.initialize() 来获取锁和设置消费组
    await runner.initialize();

    console.log("[2] 开始轮询消息...");
    const messages = await runner.poll({ block: 1000 });

    if (messages.length === 0) {
        console.log("   (未收到消息，仅做演示)");
    }

    for (const { streamName, msgId, data } of messages) {
        console.log(`[3] 处理消息: ${msgId}`);

        // 4. 原子发送数据到数据队列 (Data Stream)
        // 模拟业务产生了一些流式输出
        const emitter = new GatewayDataEmitter(redis);
        await emitter.emitChunk(data.header.sessionId, data.header.traceId || "", "收到您的请求，正在处理...\n", {
            sourceAgentType: workerId
        });
        console.log("   数据已上报到 Data Stream");

        // 3. 原子发送 ACK
        await runner.ack(streamName, msgId);
        console.log(`   消息 ${msgId} 已 ACK`);
    }

    // 释放资源
    await runner.release();
    await redis.quit();
    console.log("=== 演示结束 ===");
}

if (require.main === module) {
    main().catch(console.error);
}

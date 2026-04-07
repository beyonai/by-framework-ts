import {
    WorkerRunner,
    WorkerRegistry,
    GatewayDataEmitter,
    createRedis,
    AgentState,
    WorkerHeartbeat
} from '../src';

/**
 * 演示发布订阅 (Pub/Sub) 模式的使用。
 */
async function main() {
    const redisOpts = {
        host: 'localhost',
        port: 6379,
        db: 0,
        username: process.env.REDIS_USERNAME,
        password: process.env.REDIS_PASSWORD
    };

    const redis = createRedis(redisOpts);
    const workerId = "pubsub-demo-worker";
    const agentTypes = ["pubsub-agent-type"];

    console.log("=== Pub/Sub 模式演示 ===");

    const registry = new WorkerRegistry(redis);
    await registry.registerWorker(workerId, agentTypes);
    console.log("[1] 注册成功");

    // 1.1 启动心跳维持组件 (Standalone Heartbeat)
    const heartbeat = new WorkerHeartbeat(workerId, agentTypes, redis);
    await heartbeat.start();

    // 为 Runner 提供独立的 Redis 连接，避免轮询时的 BLOCK 指令阻塞其他操作（如 emitChunk）
    // 关键：轮询必须拥有自己的独占连接
    const runner = new WorkerRunner({ workerId, agentTypes }, {
        redisClient: createRedis(redisOpts)
    });
    const emitter = new GatewayDataEmitter(redis);

    // 2. 初始化消费组等环境
    await runner.initialize();

    // 用于追踪手动模式下的活跃任务，以便在收到取消指令时能够找到对应的信号源进行中断
    // 存储 AbortController 以及 session 相关上下文，方便在取消回调中发送状态
    const activeTasks = new Map<string, {
        controller: AbortController,
        sessionId: string,
        traceId: string
    }>();

    console.log("[2] 启动订阅...");
    const subscription = runner.subscribe(async (msg) => {
        const messageId = msg.data.header.messageId;
        const sessionId = msg.data.header.sessionId;
        const traceId = msg.data.header.traceId || "";

        // 模拟 WorkerRunner.processAndAck 的行为：注册 Execution
        const executionId = `exec-${messageId.slice(-8)}`;
        console.log(`[+] 收到消息: ${messageId}, 注册 Execution: ${executionId}`);

        // 为每个任务创建一个中止控制器，并存入上下文信息
        const controller = new AbortController();
        activeTasks.set(messageId, {
            controller,
            sessionId,
            traceId
        });

        await registry.saveExecution({
            execution_id: executionId,
            message_id: messageId,
            session_id: sessionId,
            worker_id: workerId,
            status: 'RUNNING',
            created_at: Date.now(),
            updated_at: Date.now(),
        });

        try {
            // 使用 atomic emitter 发送回复
            await emitter.emitChunk(sessionId, traceId, "Pub/Sub 模式收到任务，正在处理...", {
                sourceAgentType: workerId
            });

            // 模拟流式业务逻辑处理（在循环中输出数据并支持信号中断）
            console.log(`    任务正在执行 (循环输出数据)...`);
            for (let i = 1; i <= 20; i++) {
                // 每次循环前检查信号是否已中止
                if (controller.signal.aborted) {
                    throw new Error('Task Interrupted');
                }

                const chunkContent = `这是第 ${i} 条流式数据内容...`;
                await emitter.emitChunk(sessionId, traceId, chunkContent, {
                    sourceAgentType: workerId
                });

                console.log(`    已发送数据块 ${i}/20`);
                // 模拟更快的产生速度 (200ms)，让演示更流畅
                await new Promise(resolve => setTimeout(resolve, 200));
            }

            await emitter.emitState(sessionId, traceId, AgentState.COMPLETED, {
                sourceAgentType: workerId
            });

            // 更新状态为已完成
            await registry.markExecutionFinished(executionId, sessionId, 'COMPLETED');
            console.log(`    任务处理完毕`);
        } catch (err: any) {
            if (err.message === 'Task Interrupted') {
                console.log(`[!] 任务 ${messageId} 已中断`);
                // 发送终态 CANCELLED
                await emitter.emitState(sessionId, traceId, AgentState.CANCELLED, {
                    sourceAgentType: workerId
                });
                await registry.markExecutionFinished(executionId, sessionId, AgentState.CANCELLED);
            } else {
                console.error(`    任务执行出错:`, err);
                await registry.markExecutionFinished(executionId, sessionId, AgentState.FAILED);
            }
        } finally {
            activeTasks.delete(messageId);
            // 原子发送 ACK
            await runner.ack(msg.streamName, msg.msgId);
            console.log(`    消息已确认 (ACK)`);
        }
    });

    // 3. 启动取消订阅
    console.log("[3] 启动取消订阅...");
    const cancelSub = runner.subscribeCancel(async (cmd) => {
        console.log(`[!] 收到取消指令: ${cmd.targetMessageId}, 原因: ${cmd.reason}`);

        // 查找并触发中止信号
        const taskInfo = activeTasks.get(cmd.targetMessageId);
        if (taskInfo) {
            console.log(`    正在发送中止信号给任务: ${cmd.targetMessageId}...`);

            // 关键：优先触发 abort() 信号，不要等待状态发送完成
            // 这样业务循环能第一时间检测到 aborted 状态
            taskInfo.controller.abort();

            // 异步发送 CANCELLING 状态，不阻塞信号监听
            emitter.emitState(taskInfo.sessionId, taskInfo.traceId, AgentState.CANCELLING, {
                sourceAgentType: workerId
            }).catch(e => console.error("发送 CANCELLING 状态失败:", e));

            console.log(`    中止信号已发出`);
        } else {
            console.log(`    未找到正在运行的任务: ${cmd.targetMessageId}`);
        }
    });

    console.log("[4] 订阅已就绪，正在等待消息... (按 Ctrl+C 停止)");

    // 持续运行，直到收到中断信号
    const shutdown = async () => {
        console.log("\n[5] 正在停止订阅与资源释放...");
        subscription.stop();
        cancelSub.stop();
        heartbeat.stop();
        await runner.release();
        await redis.quit();
        console.log("=== 演示结束 ===");
        process.exit(0);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

    // 保持进程运行
    await new Promise(() => { });
}

if (require.main === module) {
    main().catch(console.error);
}

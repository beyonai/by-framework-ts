/**
 * pubsub_usage_demo.ts — 底层 Pub/Sub 模式示例
 *
 * Redis Trace 接入：
 *   runner.subscribe() 绕过了 processAndAck()，框架不会自动记录 span。
 *   需要在 subscribe 回调里手动调用 runner.spanRecorder.recordSpan()。
 *   runner.spanRecorder 已在构造时从环境变量自动初始化。
 *
 * Langfuse Trace 接入：
 *   plugin 钩子也不会自动触发，需要手动构造 AgentContext 并调用：
 *     langfusePlugin.onTaskStart(ctx)
 *     langfusePlugin.onTaskComplete / onTaskError / onTaskCancel(ctx, ...)
 *
 * 所需环境变量（Redis Trace）：
 *   BY_FRAMEWORK_OBSERVABILITY_ENABLED=true
 *   BY_FRAMEWORK_TRACE_REDIS_ENABLED=true
 *   BY_FRAMEWORK_TRACE_SAMPLE_RATE=1.0
 *
 * 所需环境变量（Langfuse，可选）：
 *   LANGFUSE_PUBLIC_KEY=pk-lf-...
 *   LANGFUSE_SECRET_KEY=sk-lf-...
 *   LANGFUSE_HOST=https://cloud.langfuse.com
 */
import {
    WorkerRunner,
    WorkerRegistry,
    GatewayDataEmitter,
    AgentContext,
    LangfusePlugin,
    createRedis,
    AgentState,
    WorkerHeartbeat,
} from '../src';
import type { TraceSpan } from '../src';

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

    // 为 Runner 提供独立的 Redis 连接，避免轮询时的 BLOCK 指令阻塞其他操作（如 emitChunk）
    // 关键：轮询必须拥有自己的独占连接
    const runner = new WorkerRunner({ workerId, agentTypes, registry }, {
        redisClient: createRedis(redisOpts)
    });
    const emitter = new GatewayDataEmitter(redis);

    // 1. 初始化消费组等环境（内部会执行 claimWorkerId 获取独占锁）
    await runner.initialize();
    console.log("[1] 注册成功并获取独占锁");

    // ── Langfuse Plugin 初始化 ──────────────────────────────────────────────
    // 若未设置 LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY，插件会静默禁用。
    const langfusePlugin = new LangfusePlugin();
    await langfusePlugin.onWorkerStartup(null as any);

    // 2. 启动心跳维持组件 (Standalone Heartbeat)
    // 必须传入同一个 registry 实例，以便复用 runner 刚刚获取的 lock token
    const heartbeat = new WorkerHeartbeat(workerId, agentTypes, redis, registry);
    await heartbeat.start();

    // 用于追踪手动模式下的活跃任务，以便在收到取消指令时能够找到对应的信号源进行中断
    // 存储 AbortController 以及 session 相关上下文，方便在取消回调中发送状态
    const activeTasks = new Map<string, {
        controller: AbortController,
        context: AgentContext,
        sessionId: string,
        traceId: string
    }>();

    console.log("[2] 启动订阅...");
    const subscription = runner.subscribe(async (msg) => {
        const messageId = msg.data.header.messageId;
        const sessionId = msg.data.header.sessionId;
        const traceId   = msg.data.header.traceId || "";

        // 模拟 WorkerRunner.processAndAck 的行为：注册 Execution
        const executionId = `exec-${messageId.slice(-8)}`;
        console.log(`[+] 收到消息: ${messageId}, 注册 Execution: ${executionId}`);

        // ── Redis Trace：记录任务开始时间 + 从 header 读取父 span ID ──
        const executionStartedAt = Date.now();
        const parentSpanId = WorkerRunner._frameworkParentSpanId(msg.data.header);

        // ── Langfuse：构造 AgentContext 供 plugin 钩子使用 ──────────────────
        // runner.spanRecorder 已从 env 自动初始化，复用它即可。
        const context = new AgentContext(
            sessionId,
            traceId,
            redis,
            agentTypes[0] ?? '',   // currentAgentType
            messageId,             // currentMessageId
            msg.data,              // currentCommand (LangfusePlugin 从 .header 读取父 obs ID)
            undefined,             // cancelSignal
            '',                    // cancelReason
            undefined,             // pluginRegistry
            executionId,           // executionId
            runner.spanRecorder,   // 复用已有的 SpanRecorder
        );
        await langfusePlugin.onTaskStart(context);

        // 为每个任务创建一个中止控制器，并存入上下文信息
        const controller = new AbortController();
        activeTasks.set(messageId, { controller, context, sessionId, traceId });

        await registry.saveExecution({
            execution_id: executionId,
            message_id:   messageId,
            session_id:   sessionId,
            worker_id:    workerId,
            status:       'RUNNING',
            created_at:   Date.now(),
            updated_at:   Date.now(),
        });

        let taskStatus: 'COMPLETED' | 'CANCELLED' | 'FAILED' = 'COMPLETED';
        let taskError: Error | undefined;
        let chunkCount = 0;

        try {
            // 使用 atomic emitter 发送回复
            await emitter.emitChunk(sessionId, traceId, "Pub/Sub 模式收到任务，正在处理...", {
                sourceAgentType: workerId
            });
            chunkCount++;

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
                chunkCount++;

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

            // ── Langfuse：正常完成 ──
            await langfusePlugin.onTaskComplete(context, { status: 'COMPLETED', chunkCount });

        } catch (err: any) {
            if (err.message === 'Task Interrupted') {
                console.log(`[!] 任务 ${messageId} 已中断`);
                taskStatus = 'CANCELLED';
                await emitter.emitState(sessionId, traceId, AgentState.CANCELLED, {
                    sourceAgentType: workerId
                });
                await registry.markExecutionFinished(executionId, sessionId, AgentState.CANCELLED);

                // ── Langfuse：任务取消 ──
                await langfusePlugin.onTaskCancel(context, null);

            } else {
                console.error(`    任务执行出错:`, err);
                taskStatus = 'FAILED';
                taskError  = err instanceof Error ? err : new Error(String(err));
                await registry.markExecutionFinished(executionId, sessionId, AgentState.FAILED);

                // ── Langfuse：任务失败 ──
                await langfusePlugin.onTaskError(context, taskError);
            }
        } finally {
            activeTasks.delete(messageId);

            // ── Redis Trace：手动记录 worker.execute span ──
            // runner.subscribe() 绕过了 processAndAck()，所以这里需要手动补写。
            // runner.spanRecorder 已在构造时从 env 自动初始化。
            const endTs = Date.now();
            const span: TraceSpan = {
                traceId,
                spanId:       `${executionId}:worker.execute`,
                parentSpanId,
                operation:    'worker.execute',
                component:    'worker',
                startTs:      executionStartedAt,
                endTs,
                status:       taskStatus,
                sessionId,
                messageId,
                executionId,
                workerId,
                targetAgentType: agentTypes[0] ?? '',
                chunkCount,
                ...(taskError && {
                    errorType:    taskError.constructor?.name || 'Error',
                    errorMessage: taskError.message,
                }),
            };
            await runner.spanRecorder.recordSpan(span);

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
        await langfusePlugin.onWorkerShutdown(null as any);  // flush pending spans
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

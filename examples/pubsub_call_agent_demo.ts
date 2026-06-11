/**
 * pubsub_call_agent_demo.ts — 底层 Pub/Sub 模式 + callAgent 示例
 *
 * 演示：在 runner.subscribe() 回调中，通过 AgentContext.callAgent()
 * 向 demo_worker.ts（agent type: demo-agent-ts）委派子任务。
 *
 * 运行前提：
 *   1. 启动 Redis：docker run -d -p 6379:6379 redis:7-alpine
 *   2. 启动子 agent（另一个终端）：npx ts-node examples/demo_worker.ts
 *   3. 启动本 orchestrator：npx ts-node examples/pubsub_call_agent_demo.ts
 *
 * Langfuse Trace 层次（跨进程自动嵌套）：
 *   trace (traceId)
 *     └── agent.workflow:pubsub-orchestrator   ← 本 orchestrator
 *           └── worker.execute
 *                 └── pubsub-orchestrator
 *                       └── client.dispatch → demo-agent-ts  ← callAgent
 *
 *   另一个 trace 节点（demo_worker 进程）：
 *   trace (同一 traceId)
 *     └── agent.workflow:demo-agent-ts         ← 嵌套在 orchestrator 的 task span 下
 *           └── worker.execute
 *                 └── demo-agent-ts
 *
 * Langfuse 环境变量（可选，不设置则静默禁用）：
 *   LANGFUSE_PUBLIC_KEY=pk-lf-...
 *   LANGFUSE_SECRET_KEY=sk-lf-...
 *   LANGFUSE_HOST=https://cloud.langfuse.com
 *
 * Redis Trace 环境变量（可选）：
 *   BY_FRAMEWORK_OBSERVABILITY_ENABLED=true
 *   BY_FRAMEWORK_TRACE_REDIS_ENABLED=true
 *   BY_FRAMEWORK_TRACE_SAMPLE_RATE=1.0
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
    QueueNames,
} from '../src';
import type { TraceSpan } from '../src';

// ── 配置 ──────────────────────────────────────────────────────────────────────

const ORCHESTRATOR_AGENT_TYPE = 'pubsub-orchestrator';
const SUB_AGENT_TYPE          = 'demo-agent-ts';   // demo_worker.ts 注册的 agent type

// ── Main ──────────────────────────────────────────────────────────────────────

async function main() {
    const redisOpts = {
        host: process.env.REDIS_HOST || 'localhost',
        port: Number(process.env.REDIS_PORT || 6379),
        db: 0,
        username: process.env.REDIS_USERNAME,
        password: process.env.REDIS_PASSWORD,
    };

    const redis     = createRedis(redisOpts);
    const workerId  = 'pubsub-orchestrator-01';
    const agentTypes = [ORCHESTRATOR_AGENT_TYPE];

    console.log('=== Pub/Sub + callAgent 演示 ===');

    const registry = new WorkerRegistry(redis);

    // 轮询连接必须独占，避免 XREAD BLOCK 阻塞 emitChunk 等操作
    const runner = new WorkerRunner({ workerId, agentTypes, registry }, {
        redisClient: createRedis(redisOpts),
    });
    const emitter = new GatewayDataEmitter(redis);

    await runner.initialize();
    console.log('[1] 注册成功并获取独占锁');

    // ── Langfuse Plugin 初始化 ────────────────────────────────────────────────
    const langfusePlugin = new LangfusePlugin();
    await langfusePlugin.onWorkerStartup(null as any);

    const heartbeat = new WorkerHeartbeat(workerId, agentTypes, redis, registry);
    await heartbeat.start();

    // 活跃任务表：用于取消信号路由
    const activeTasks = new Map<string, {
        controller: AbortController;
        context: AgentContext;
        sessionId: string;
        traceId: string;
    }>();

    // ── 主订阅：接收 orchestrator 任务 ───────────────────────────────────────
    console.log('[2] 启动订阅...');
    const subscription = runner.subscribe(async (msg) => {
        const messageId = msg.data.header.messageId;
        const sessionId = msg.data.header.sessionId;
        const traceId   = msg.data.header.traceId || '';
        const content   = (msg.data as any).content ?? '(empty)';

        const executionId    = `exec-${messageId.slice(-8)}`;
        const executionStart = Date.now();
        const parentSpanId   = WorkerRunner._frameworkParentSpanId(msg.data.header);

        console.log(`[+] 收到消息: ${messageId} | content: ${String(content).slice(0, 60)}`);

        // ── 构建 AgentContext（复用 runner.spanRecorder，供 callAgent + Langfuse 使用）──
        const context = new AgentContext(
            sessionId,
            traceId,
            redis,
            ORCHESTRATOR_AGENT_TYPE,
            messageId,
            msg.data,
            undefined,
            '',
            undefined,
            executionId,
            runner.spanRecorder,
        );

        // ── Langfuse：任务开始 ─────────────────────────────────────────────────
        // onTaskStart 会：
        //   1. 在 Langfuse 创建 workflow / worker.execute / agent.task 三层 span
        //   2. 将 agent.task span ID 写入 context.traceParentObservationId
        //   3. callAgent() 会自动把它放入子命令的 langfuseParentObservationId，
        //      让子 agent 的 workflow span 正确嵌套在本 task span 下
        await langfusePlugin.onTaskStart(context);

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

        try {
            // ── Step 1：通知客户端开始处理 ──────────────────────────────────────
            await emitter.emitChunk(sessionId, traceId,
                `[Orchestrator] 收到任务，准备委派给 ${SUB_AGENT_TYPE}...`, {
                    sourceAgentType: ORCHESTRATOR_AGENT_TYPE,
                }
            );

            if (controller.signal.aborted) throw new Error('Task Interrupted');

            // ── Step 2：callAgent — 调用 demo_worker.ts ──────────────────────
            // waitForReply: false → 消防式委派，子 agent 异步处理，
            //   子 agent 的输出块（emitChunk）会写入同一 sessionId 的 data stream，
            //   客户端无需更换订阅即可收到。
            //
            // waitForReply: true  → 挂起当前执行，等待子 agent 返回 ResumeCommand。
            //   在高层 GatewayWorker 模式（demo_worker.ts 方式）下框架自动处理；
            //   在底层 pub/sub 模式下需要手动在 subscribe 回调里识别并路由 ResumeCommand，
            //   见文件末尾的 "Resume 处理" 注释。
            console.log(`    [callAgent] → ${SUB_AGENT_TYPE}, waitForReply=false`);

            const callResult = await context.callAgent({
                targetAgentType: SUB_AGENT_TYPE,
                content:         String(content),
                waitForReply:    false,
                // 透传给子 agent 的额外参数
                payload: { source: ORCHESTRATOR_AGENT_TYPE },
                // langfuseParentObservationId 由框架自动从 context.traceParentObservationId 写入，
                // 无需手动传递
            });

            if (callResult.status === 'FAILED') {
                throw new Error(callResult.error || `callAgent to ${SUB_AGENT_TYPE} failed`);
            }

            console.log(`    [callAgent] 委派成功: messageId=${callResult.messageId}, status=${callResult.status}`);

            // ── Step 3：告知客户端委派完成，子 agent 正在处理 ─────────────────
            await emitter.emitChunk(sessionId, traceId,
                `[Orchestrator] 任务已委派 (subMsgId=${callResult.messageId})，子 agent 正在处理中...`, {
                    sourceAgentType: ORCHESTRATOR_AGENT_TYPE,
                }
            );

            // ── Step 4：子 agent 的输出块将自动流入同一 data stream ─────────────
            // 读取 data stream 的示例（可选，用于日志或二次加工）：
            //
            //   const streamKey = `byai_gateway:session:${sessionId}:data_stream`;
            //   const events = await redis.xread('COUNT', 50, 'STREAMS', streamKey, '$');
            //
            // 本演示不阻塞等待，子 agent 异步完成后客户端自然收到其输出块。

            await emitter.emitState(sessionId, traceId, AgentState.COMPLETED, {
                sourceAgentType: ORCHESTRATOR_AGENT_TYPE,
            });

            await registry.markExecutionFinished(executionId, sessionId, 'COMPLETED');
            console.log('    [orchestrator] 完成，子任务运行中');

            // ── Langfuse：正常完成 ──────────────────────────────────────────────
            await langfusePlugin.onTaskComplete(context, {
                status:  'COMPLETED',
                subMsgId: callResult.messageId,
            });

        } catch (err: any) {
            if (err.message === 'Task Interrupted') {
                console.log(`[!] 任务 ${messageId} 已中断`);
                taskStatus = 'CANCELLED';
                await emitter.emitState(sessionId, traceId, AgentState.CANCELLED, {
                    sourceAgentType: ORCHESTRATOR_AGENT_TYPE,
                });
                await registry.markExecutionFinished(executionId, sessionId, AgentState.CANCELLED);
                await langfusePlugin.onTaskCancel(context, null);

            } else {
                console.error('    [orchestrator] 出错:', err);
                taskStatus = 'FAILED';
                taskError  = err instanceof Error ? err : new Error(String(err));
                await registry.markExecutionFinished(executionId, sessionId, AgentState.FAILED);
                await langfusePlugin.onTaskError(context, taskError);
            }
        } finally {
            activeTasks.delete(messageId);

            // ── Redis Trace：手动记录 worker.execute span ────────────────────────
            const endTs = Date.now();
            const span: TraceSpan = {
                traceId,
                spanId:          `${executionId}:worker.execute`,
                parentSpanId,
                operation:       'worker.execute',
                component:       'worker',
                startTs:         executionStart,
                endTs,
                status:          taskStatus,
                sessionId,
                messageId,
                executionId,
                workerId,
                targetAgentType: ORCHESTRATOR_AGENT_TYPE,
                ...(taskError && {
                    errorType:    taskError.constructor?.name || 'Error',
                    errorMessage: taskError.message,
                }),
            };
            await runner.spanRecorder.recordSpan(span);

            await runner.ack(msg.streamName, msg.msgId);
            console.log('    [orchestrator] ACK 已确认');
        }
    });

    // ── 取消订阅 ─────────────────────────────────────────────────────────────
    console.log('[3] 启动取消订阅...');
    const cancelSub = runner.subscribeCancel(async (cmd) => {
        console.log(`[!] 收到取消指令: ${cmd.targetMessageId}`);
        const taskInfo = activeTasks.get(cmd.targetMessageId);
        if (taskInfo) {
            taskInfo.controller.abort();
            emitter.emitState(taskInfo.sessionId, taskInfo.traceId, AgentState.CANCELLING, {
                sourceAgentType: ORCHESTRATOR_AGENT_TYPE,
            }).catch(e => console.error('发送 CANCELLING 状态失败:', e));
        }
    });

    console.log('[4] 就绪，等待消息... (Ctrl+C 停止)');
    console.log(`    本 orchestrator agent type: ${ORCHESTRATOR_AGENT_TYPE}`);
    console.log(`    子 agent type:              ${SUB_AGENT_TYPE}`);

    const shutdown = async () => {
        console.log('\n[5] 正在停止...');
        subscription.stop();
        cancelSub.stop();
        heartbeat.stop();
        await runner.release();
        await langfusePlugin.onWorkerShutdown(null as any);
        await redis.quit();
        console.log('=== 演示结束 ===');
        process.exit(0);
    };

    process.on('SIGINT', shutdown);
    process.on('SIGTERM', shutdown);

    await new Promise(() => { });
}

/*
 * ── waitForReply: true 在 pub/sub 模式下的 Resume 处理说明 ──────────────────
 *
 * 若要使用 waitForReply: true，子 agent 完成后会将 ResumeCommand 推入
 * orchestrator 的 per-worker 流（byai_gateway:ctrl:worker:{workerId}）。
 *
 * 在底层 pub/sub 模式下，需要在 subscribe 回调里区分命令类型并手动恢复：
 *
 *   import { ResumeCommand } from '../src';
 *
 *   runner.subscribe(async (msg) => {
 *       if (msg.data instanceof ResumeCommand) {
 *           // 从 activeTasks 找到挂起的 generator/callback，继续执行
 *           const resumed = suspendedTasks.get(msg.data.header.parentMessageId);
 *           if (resumed) resumed(msg.data.replyData);
 *           return;
 *       }
 *       // 正常 AskAgentCommand 处理...
 *   });
 *
 * 对于 waitForReply: true 的完整支持，推荐改用高层 GatewayWorker 模式
 * （参见 demo_worker.ts），框架会自动处理 suspend/resume 流程。
 */

if (require.main === module) {
    main().catch(console.error);
}

/**
 * demo_worker.ts — 高层 API 示例
 *
 * Trace 接入方式：WorkerRunner 在 processAndAck() 内部自动记录 worker.execute span，
 * 无需任何代码改动，只需设置以下环境变量即可开启：
 *
 *   BY_FRAMEWORK_OBSERVABILITY_ENABLED=true
 *   BY_FRAMEWORK_TRACE_REDIS_ENABLED=true
 *   BY_FRAMEWORK_TRACE_SAMPLE_RATE=1.0        # 采样率 0.0~1.0
 *   BY_FRAMEWORK_TRACE_TTL_SECONDS=900        # span 保留时长（秒）
 *
 * Span 写入 Redis：
 *   byai_gateway:trace:{traceId}:spans        — span JSON 列表
 *   byai_gateway:trace:{traceId}:meta         — trace 元信息
 *
 * 自定义 SpanRecorder（可选）：
 *   const recorder = new SpanRecorder(redis, { exporters: [myExporter] });
 *   const runner = new WorkerRunner(worker, { spanRecorder: recorder });
 */
import {
    GatewayWorker,
    AskAgentCommand,
    AgentContext,
    WorkerRunner,
    autoRegisterLangfusePlugin
} from '../src';

class DemoWorker extends GatewayWorker {
    getAgentTypes(): string[] {
        return ['demo-agent-ts'];
    }

    async processCommand(command: AskAgentCommand, context: AgentContext): Promise<any> {
        console.log(`[${this.workerId}] Processing message: ${command.content}`);
        // context.traceId / context.executionId 可在业务逻辑中读取，用于关联外部系统
        console.log(`[${this.workerId}] traceId=${context.traceId} executionId=${context.executionId}`);

        // Discovery Demo
        const activeWorkers = await context.getActiveWorkers();
        console.log(`[${this.workerId}] Active workers in cluster: ${Object.keys(activeWorkers).join(', ')}`);

        const text = `Echo from TypeScript SDK: ${command.content}. I am processing your request.`;

        for (const char of text) {
            await context.emitChunk({ content: char });
            await new Promise((resolve) => setTimeout(resolve, 50));
        }

        return {
            status: 'done',
            reply: 'Message processed by TS SDK',
        };
    }
}

async function main() {
    // 启动前一行注册
    autoRegisterLangfusePlugin();
    const worker = new DemoWorker('worker-ts-01');
    // WorkerRunner 自动从环境变量创建 SpanRecorder：
    //   runner.spanRecorder — 可在外部读取已写入的 recorder 实例
    const runner = new WorkerRunner(worker);

    await runner.start({ handleSignals: true });
}

main().catch(console.error);

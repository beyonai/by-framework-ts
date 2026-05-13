# @byclaw/by-framework

Gateway Message Queue 的 TypeScript SDK，提供基于 Redis Stream 的任务分发、Worker 运行时、流式数据上报、任务取消、执行状态追踪、会话历史、工作区隔离、插件和服务发现等能力。

## 核心架构

SDK 当前以 `agent_type` 为主要路由维度：

- **控制流**：`GatewayClient` 将 `AskAgentCommand` / `ResumeCommand` / `CancelTaskCommand` 写入 Redis Stream。普通任务进入 `byai_gateway:ctrl:agent_type:{agentType}`，定向控制命令进入 `byai_gateway:ctrl:worker:{workerId}`。
- **执行流**：`WorkerRunner` 为 Worker 创建消费组、抢占 `workerId` 在线租约、启动心跳，并通过 `runner.start()` 自动轮询、处理、ACK、更新执行状态。
- **数据流**：`AgentContext` / `GatewayDataEmitter` 将流式回答、思考状态、文件产物、用户输入请求写入 `byai_gateway:session:{sessionId}:data_stream`。
- **注册与状态**：`WorkerRegistry` 维护 Worker 能力、在线租约，以及按 `sessionId` 聚合的 execution 记录和 `messageId -> executionId` 映射。
- **扩展能力**：包含插件注册、拦截器、会话历史、工作区、HookSandbox、文件存储和服务发现模块。

## 模块结构

- `src/client.ts`：业务端发起任务、发送自定义命令、取消任务。
- `src/worker.ts`：`GatewayWorker` 抽象基类与 `AnonymousWorker`。
- `src/runner.ts`：Worker 生命周期、消费组、自动处理、取消控制流。
- `src/context.ts`：Worker 处理任务时使用的 `AgentContext`，负责推流、调用下游 Agent、取消检查等。
- `src/emitter.ts`：独立数据流上报器 `GatewayDataEmitter`。
- `src/registry.ts`：Worker 在线状态和 execution 生命周期记录。
- `src/protocol/`：命令、事件、状态、响应、消息协议类型。
- `src/extensions/`：插件和 Agent 配置。
- `src/runtime/`：历史、文件存储、运行态辅助模块。
- `src/discovery/`：服务发现客户端和注册器。
- `examples/`：本地运行示例。
- `tests/`：Jest 测试。

`dist/` 是构建产物，不要手工编辑。

## 安装

环境要求：Node.js 18+

```bash
npm install @byclaw/by-framework
```

本仓库开发命令：

```bash
npm run build
npm test
npm run dev
```

## 快速开始

### 创建 Worker

推荐让 `WorkerRunner.start()` 托管 Worker 生命周期。它会初始化 Stream、启动心跳、处理任务、自动 ACK，并在任务结束时更新 execution 状态。

```typescript
import {
    AgentContext,
    AgentState,
    AskAgentCommand,
    GatewayWorker,
    WorkerRunner,
} from '@byclaw/by-framework';

class DemoWorker extends GatewayWorker {
    getAgentTypes(): string[] {
        return ['demo-agent-ts'];
    }

    async processCommand(command: AskAgentCommand, context: AgentContext): Promise<any> {
        await context.emitState('processing');
        await context.emitChunk(`Echo: ${command.content}`);

        return {
            status: AgentState.COMPLETED,
            content: `Echo: ${command.content}`,
            replyData: { ok: true },
        };
    }
}

const worker = new DemoWorker('worker-ts-01');
const runner = new WorkerRunner(worker, {
    maxConcurrency: 50,
    fetchCount: 10,
});

await runner.start({ handleSignals: true });
```

也可以用工厂方法创建回调式 Worker：

```typescript
import { AgentState, createRedis, createWorkerRunner } from '@byclaw/by-framework';

const redis = createRedis({ host: 'localhost', port: 6379, db: 0 });

const runner = createWorkerRunner({
    workerId: 'worker-ts-01',
    agentTypes: ['demo-agent-ts'],
    redisClient: redis,
    onTask: async (command, context) => {
        await context.emitChunk(`Received: ${JSON.stringify(command.toDict())}`);
        return { status: AgentState.COMPLETED, replyData: { ok: true } };
    },
});

await runner.start({ handleSignals: true });
```

### 发送任务

`sendMessage` 默认要求目标 `agentType` 当前有在线 Worker。若需要“先入队、后启动 Worker”的调试模式，可传 `requireOnlineWorker: false`。

```typescript
import { GatewayClient, WorkerRegistry, createRedis } from '@byclaw/by-framework';

const redis = createRedis({ host: 'localhost', port: 6379, db: 0 });
const registry = new WorkerRegistry(redis);
const client = new GatewayClient(registry, redis);

const res = await client.sendMessage({
    targetAgentType: 'demo-agent-ts',
    sessionId: 'test-session',
    content: 'Hello!',
    userCode: 'test-tenant',
    metadata: { requestId: 'req-1' },
});

console.log(res);
await redis.quit();
```

`content` 支持字符串、`BaiYingMessage` 或 `BaiYingMessage[]`。复杂消息会序列化为协议中的 `body.content`。

### 取消任务

取消逻辑以 `sessionId + messageId` 查找 execution。若任务已被 Worker 领取，会向该 Worker 的控制流发送 `CancelTaskCommand`；若还在队列中，会将 execution 标记为 `CANCELLING`，后续 Worker 领取时会跳过业务处理并标记为 `CANCELLED`。

```typescript
const cancelRes = await client.cancelTask({
    messageId: res.message_id,
    sessionId: 'test-session',
    reason: 'user aborted',
    requestedBy: 'frontend',
    cancelMode: 'graceful',
});
```

Worker 业务代码应在长任务中周期性检查取消信号：

```typescript
for (const item of items) {
    await context.checkCancelled();
    await doWork(item);
}
```

### 调用下游 Agent

在 Worker 内可以通过 `AgentContext` 继续派发任务：

```typescript
const child = await context.callAgent({
    targetAgentType: 'child-agent',
    content: 'Please continue this task',
    waitForReply: true,
    payload: { priority: 'normal' },
});
```

也支持 Scatter-Gather：

```typescript
const group = await context.dispatchGroup({
    tasks: [
        { targetAgentType: 'agent-a', content: 'task A' },
        { targetAgentType: 'agent-b', content: 'task B' },
    ],
    waitForReply: true,
});

const results = await context.collectGroupResults(group.taskGroupId, 30);
```

## 核心 API

### Redis

```typescript
createRedis(options?: {
    host?: string;
    port?: number;
    db?: number;
    username?: string;
    password?: string;
})
```

默认读取：

| 变量名 | 说明 |
|--------|------|
| `REDIS_HOST` | Redis 主机，默认 `localhost` |
| `REDIS_PORT` | Redis 端口，默认 `6379` |
| `REDIS_DATABASE` | Redis DB，默认 `0` |
| `REDIS_USERNAME` | Redis 用户名，可选 |
| `REDIS_PASSWORD` | Redis 密码，可选 |

`runWorker` 还会读取：

| 变量名 | 说明 |
|--------|------|
| `BYAI_WORKER_CONCURRENCY` | Worker 最大并发，默认 `50` |
| `BYAI_WORKER_FETCH_COUNT` | 每次读取任务数，默认 `10` |
| `BYAI_REDIS_MAX_CONNECTIONS` | 与 Python SDK 对齐的连接数配置语义 |

### GatewayClient

```typescript
new GatewayClient(
    registry?: WorkerRegistry,
    redisClient?: Redis,
    interceptors?: GatewayInterceptor[]
)
```

#### `client.sendMessage(params)`

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| `targetAgentType` | `string` | 是 | 目标 Agent 类型 |
| `sessionId` | `string` | 是 | 会话 ID |
| `content` | `string \| BaiYingMessage \| BaiYingMessage[]` | 是 | 消息内容 |
| `sourceAgentType` | `string` | 否 | 来源 Agent 类型，设置后用于 Agent 间回调 |
| `traceId` | `string` | 否 | 追踪 ID，默认自动生成 |
| `userCode` | `string` | 否 | 租户或用户编码 |
| `userName` | `string` | 否 | 用户名 |
| `actionType` | `ActionType` | 否 | 默认 `ActionType.ASK_AGENT`，也可为 `ActionType.RESUME` |
| `extraPayload` | `Record<string, unknown>` | 否 | 协议 `body.extra_payload` |
| `metadata` | `Record<string, unknown>` | 否 | 协议 `header.metadata` |
| `parentMessageId` | `string` | 否 | 父消息 ID |
| `messageId` | `string` | 否 | 消息 ID，默认自动生成 |
| `targetWorkerId` | `string` | 否 | 直接投递到指定 Worker 控制流 |
| `requireOnlineWorker` | `boolean` | 否 | 是否要求在线 Worker，默认 `true` |

返回：

```typescript
interface SendMessageResponse {
    success: boolean;
    message_id: string;
    trace_id: string;
    target_worker_id: string;
    timestamp: number;
    status: string;
    error?: string;
    error_code?: string;
}
```

#### `client.sendCommand(command, streamName?)`

发送 `BaseCommand` 或自定义命令。未传 `streamName` 时按 `command.header.targetAgentType` 投递到 `agent_type` 控制流。

#### `client.cancelTask(params)`

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| `messageId` | `string` | 是 | 要取消的消息 ID |
| `sessionId` | `string` | 是 | 会话 ID |
| `reason` | `string` | 否 | 取消原因 |
| `targetAgentType` | `string` | 否 | 目标 Agent 类型，通常可省略 |
| `requestedBy` | `string` | 否 | 请求方，默认 `client` |
| `cancelMode` | `'graceful' \| 'force'` | 否 | 默认 `graceful` |

返回：

```typescript
interface CancelTaskResponse {
    success: boolean;
    message_id: string;
    execution_id: string;
    worker_id: string;
    status: string;
    timestamp: number;
    error?: string;
    cancelled_count?: number;
}
```

### WorkerRunner

```typescript
new WorkerRunner(
    workerOrOptions:
        | GatewayWorker
        | { workerId: string; agentTypes: string[]; registry?: WorkerRegistry },
    options?: {
        redisClient?: Redis;
        groupName?: string;
        maxConcurrency?: number;
        fetchCount?: number;
    }
)
```

常用方法：

| 方法 | 说明 |
|------|------|
| `initialize()` | 抢占 Worker ID、创建 Stream 消费组、启动心跳和控制流循环 |
| `start({ handleSignals? })` | 推荐入口，自动初始化、轮询、处理、ACK、释放资源 |
| `stop()` | 停止主循环和控制流循环 |
| `release()` | 停止心跳、释放 Worker ID、关闭 Runner 拥有的 Redis 连接 |
| `poll({ count?, block? })` | 手动从 agent type 控制流读取消息 |
| `processAndAck(streamName, msgId, data)` | 执行业务处理并 ACK，同时维护 execution 状态 |
| `ack(streamName, msgId)` | 手动 ACK |
| `runControlOnce(block?)` | 手动读取一次 Worker 控制流 |
| `subscribe(handler, options?)` | 低层订阅模式，需要调用者处理和 ACK |
| `subscribeCancel(handler, options?)` | 注册取消指令回调 |

多数业务应使用 `start()`；只有需要完全自定义消费循环时才使用 `poll()` / `subscribe()`。

### GatewayWorker

继承 `GatewayWorker` 时需要实现：

```typescript
abstract getAgentTypes(): ReadonlyArray<string>;
abstract processCommand(command: GatewayCommand, context: AgentContext): Promise<ProcessCommandResult>;
```

`processCommand` 可以返回：

- `AgentTaskResult`
- `{ status, content, replyData, metadata, extraPayload }`
- 任意 JSON 可序列化对象，此时会作为 `replyData`
- `AgentState` 字符串

Worker 基类会自动处理：

- 保存用户消息到历史记录。
- 创建 `AgentContext`。
- 注入会话历史到命令的 `extraPayload.history`。
- 捕获取消并返回 `CANCELLED`。
- 任务完成时发送 `FINAL_ANSWER` 和必要的 `APP_STREAM_RESPONSE`。
- 有 `sourceAgentType` 时将结果封装为 `ResumeCommand` 回调给上游 Agent。

### AgentContext

| 方法 | 说明 |
|------|------|
| `emitChunk(event, eventType?)` | 推送流式回答，默认 `answerDelta` |
| `emitState(event, eventType?)` | 推送状态或思考日志，默认 `reasoningLogDelta` |
| `emitArtifact(event, eventType?)` | 推送文件产物 |
| `askUser(event)` | 向用户请求输入，并将当前任务标记为等待用户 |
| `callAgent(params)` | 调用单个下游 Agent |
| `dispatchGroup(params)` | 并发派发多个下游任务 |
| `collectGroupResults(taskGroupId, timeout?)` | 收集任务组结果 |
| `checkCancelled()` | 若任务已取消则抛出 `TaskCancelledError` |
| `isCancelRequested()` | 查询取消信号 |
| `updateExecutionState(status)` | 更新当前 execution 状态 |
| `getActiveWorkers()` | 获取在线 Worker |
| `callTool(name, kwargs?)` | 调用插件 Agent 配置中的工具 |

### GatewayDataEmitter

```typescript
new GatewayDataEmitter(redisClient?: Redis, params?: {
    sourceAgentType?: string;
    dataStreamName?: string;
})
```

| 方法 | 默认事件 | 默认内容类型 | 说明 |
|------|----------|--------------|------|
| `emitEvent(params)` | 自定义 | 自定义 | 底层事件上报 |
| `emitChunk(sessionId, traceId, event, options?)` | `EventType.ANSWER_DELTA` | `SseMessageType.text` (`1002`) | 文本或结构化 chunk |
| `emitState(sessionId, traceId, event, options?)` | `EventType.REASONING_LOG_DELTA` | `SseReasonMessageType.think_title` (`3003`) | 状态或思考日志 |
| `emitArtifact(sessionId, traceId, event, options?)` | `EventType.REASONING_LOG_DELTA` | `SseReasonMessageType.task_create_file` (`3010`) | 文件 URL |
| `askUser(sessionId, traceId, event, options?)` | `EventType.REASONING_LOG_DELTA` | `SseReasonMessageType.task_user_input` (`3013`) | 用户输入表单 |

所有事件默认写入 `QueueNames.session_data_stream(sessionId)`，构造时传入 `dataStreamName` 可固定写入指定流。

### WorkerRegistry

Worker 在线状态：

| 方法 | 说明 |
|------|------|
| `registerWorker(workerId, agentTypes)` | 注册 Worker 能力并发送一次心跳 |
| `registerWorkerMembership(workerId, agentTypes)` | 只注册能力，不改变在线租约 |
| `heartbeatWorker(workerId, leaseTtlSeconds?)` | 刷新在线租约 |
| `unregisterWorker(workerId)` | 移除在线状态和能力成员关系 |
| `markWorkerInactive(workerId, token?)` | 移除在线租约 |
| `unregisterWorkerMembership(workerId)` | 移除能力成员关系 |
| `claimWorkerId(workerId, ttlSeconds?)` | 抢占 Worker ID，防止重复启动 |
| `refreshWorkerIdLock(workerId, ttlSeconds?)` | 刷新抢占锁 |
| `releaseWorkerId(workerId, token?)` | 释放抢占锁 |
| `isWorkerOnline(workerId)` | 判断 Worker 是否在线 |
| `hasOnlineAgentType(agentType)` | 获取某类 Agent 的在线 Worker |
| `getOnlineWorkers(agentType)` | 获取在线 Worker 列表 |
| `getTargetWorker(agentType)` | 随机获取一个在线 Worker |
| `getAllWorkers()` | 获取当前在线 Worker 信息 |

Execution 状态：

| 方法 | 说明 |
|------|------|
| `initializeExecution(execution)` | 初始化 execution，包含时间线字段 |
| `saveExecution(execution)` | 保存或覆盖 execution |
| `getExecution(executionId, sessionId)` | 查询 execution |
| `getExecutionByMessageId(messageId, sessionId)` | 通过消息 ID 查询 execution |
| `getAllSessionExecutions(sessionId)` | 查询会话下全部 execution |
| `updateExecutionStatus(executionId, sessionId, status, extra?)` | 更新状态和时间线 |
| `updateExecutionStatusByMessage(messageId, sessionId, status)` | 按消息 ID 更新状态 |
| `markExecutionCancelling(executionId, sessionId, reason)` | 标记取消中 |
| `markCancelRequested(executionId, sessionId, reason?)` | 只设置取消请求，不改状态 |
| `markExecutionFinished(executionId, sessionId, status)` | 标记终态并写入完成时间 |

## 协议枚举

```typescript
enum ActionType {
    ASK_AGENT = "ASK_AGENT",
    RESUME = "RESUME",
    CANCEL_TASK = "CANCEL_TASK",
    ASK_USER = "ASK_USER",
}

enum AgentState {
    STARTING = "STARTING",
    CANCELLING = "CANCELLING",
    CANCELLED = "CANCELLED",
    COMPLETED = "COMPLETED",
    FAILED = "FAILED",
    RESUMED = "RESUMED",
    WAITING_AGENT = "WAITING_AGENT",
    WAITING_USER = "WAITING_USER",
    QUEUED = "QUEUED",
    CALLING_AGENT = "CALLING_AGENT",
}

enum EventType {
    ANSWER_DELTA = "answerDelta",
    REASONING_LOG_DELTA = "reasoningLogDelta",
    REASONING_LOG_START = "reasoningLogStart",
    REASONING_LOG_END = "reasoningLogEnd",
    APP_STREAM_RESPONSE = "appStreamResponse",
    FINAL_ANSWER = "finalAnswer",
    TASK_CREATE = "taskCreate",
    STEP_COMPLETE = "stepComplete",
    TASK_STOP = "taskStop",
}
```

## 相关示例

- [examples/demo_worker.ts](examples/demo_worker.ts)：继承 `GatewayWorker` 的 Worker 示例。
- [examples/send_test.ts](examples/send_test.ts)：发送任务示例。
- [examples/pubsub_usage_demo.ts](examples/pubsub_usage_demo.ts)：低层 Pub/Sub 风格示例。
- [examples/cancel_task_demo.ts](examples/cancel_task_demo.ts)：取消任务示例。
- [examples/custom_command_demo.ts](examples/custom_command_demo.ts)：自定义 Command 示例。
- [examples/atomic_usage_demo.ts](examples/atomic_usage_demo.ts)：原子化使用示例。

## 开发提示

- 修改消息路由、Redis 行为、Worker 生命周期或取消逻辑后，请运行 `npm test`。
- 协议字段保持 snake_case，例如 `target_agent_type`、`message_id`、`extra_payload`。
- SDK 源码使用 strict TypeScript、CommonJS 输出、4 空格缩进、单引号和分号。

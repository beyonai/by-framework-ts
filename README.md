# gateway_sdk_ts

TypeScript SDK for Gateway Worker communication, supporting both atomic request-response and Pub/Sub messaging patterns.

## 核心架构

系统采用事件驱动与“控制流-数据流分离”的双流设计，高度解耦：

- **接入层 (业务门户)**: 使用 `GatewayClient` 向 Redis Input MQ 投递基于 `session_id` 和 `target_agent_type` 的结构化控制指令。
- **调度缓冲 (Input MQ)**: 利用 Redis Stream 实现 Worker 集群的无状态订阅消费、平滑削峰和基于能力 (Capabilities) 的分组路由。
- **执行层 (GatewayWorker)**: 包含 `SuperAssistantWorker` 等实现。Agent通过 `Runner` 主动拉取任务（Pull模型）并进行自驱动处理。每次执行挂载独立的隔离工作空间环境 (基于 ContextVar 注入拦截 `builtins.open` 的动态沙箱 `HookSandbox`)。
- **输出层 (Data MQ)**: 结果数据异步推入数据流 MQ，支持页面SSE流式打字机推送、数据库持久化和链路追踪监听并行消费。

## 模块结构

- `gateway_sdk.protocol`: 核心协议模型 (`AskAgentCommand`, `ResumeCommand`, `CancelTaskCommand`, `DataMessage`)
- `gateway_sdk.registry`: Worker 状态和能力注册中心 (`WorkerRegistry`)
- `gateway_sdk.client`: 业务端消息发起客户端 (`GatewayClient`)
- `gateway_sdk.context`: 运行期下发给 Agent 的通讯与推流上下文代理 (`AgentContext`)
- `gateway_sdk.workspace`: 临时与持久化隔离文件工作区管理 (`WorkspaceManager`)
- `gateway_sdk.sandbox`: 通过 AST 层劫持与路径限制的轻量代码沙箱 (`HookSandbox`)
- `gateway_sdk.worker`: Agent 开发脚手架与抽象基类 (`GatewayWorker`)
- `gateway_sdk.runner`: 执行组守护进程 (`WorkerRunner`) 

## 安装

**环境要求：** Node.js >= 18.x

```bash
npm install byclaw-gateway-sdk
```

---

## 使用示例

### 发送消息

```typescript
import { GatewayClient, createRedis, WorkerRegistry } from 'byclaw-gateway-sdk';

const redis = createRedis({ host: 'localhost', port: 6379, db: 0 });
const registry = new WorkerRegistry(redis);
const client = new GatewayClient(registry, redis);

const res = await client.sendMessage({
    targetAgentType: 'capability-a',
    sessionId: 'test-session',
    content: 'Hello!',
    tenantId: 'test-tenant'
});
```

### Pub/Sub 模式

```typescript
import { WorkerRunner, WorkerRegistry, GatewayDataEmitter, createRedis, AgentState, EventType } from 'byclaw-gateway-sdk';

const redis = createRedis({ host: 'localhost', port: 6379, db: 0 });
const registry = new WorkerRegistry(redis);
const workerId = 'my-worker';
const capabilities = ['capability-a'];
await registry.registerWorker(workerId, capabilities);

const runner = new WorkerRunner(
    { workerId, capabilities },
    { redisClient: redis }
);
const emitter = new GatewayDataEmitter(redis);

await runner.initialize();

const subscription = runner.subscribe(async (msg) => {
    const { sessionId, traceId } = msg.data.header;
    await emitter.emitChunk(sessionId, traceId, '处理中...', {
        contentType: SseMessageType.text,
    });

    // 处理完必须发送一条eventType为APP_STREAM_RESPONSE的事件
    await emitter.emitState(sessionId, traceId, AgentState.COMPLETED, {
        eventType: EventType.APP_STREAM_RESPONSE
    });
    await runner.ack(msg.streamName, msg.msgId);
});

const cancelSub = runner.subscribeCancel((cmd) => {
    console.log('收到取消指令:', cmd.targetMessageId, cmd.reason);
});

// 持续运行，直到收到中断信号
const shutdown = async () => {
    console.log("\n[5] 正在停止订阅与资源释放...");
    subscription.stop();
    cancelSub.stop();
    await runner.release();
    await redis.quit();
    console.log("=== 演示结束 ===");
    process.exit(0);
};

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
```

---

## 环境变量

| 变量名 | 说明 |
|--------|------|
| `REDIS_USERNAME` | Redis 用户名（可选） |
| `REDIS_PASSWORD` | Redis 密码（可选） |

---

## 相关文件

- 发送消息示例：[examples/send_test.ts](examples/send_test.ts)
- Pub/Sub 示例：[examples/pubsub_usage_demo.ts](examples/pubsub_usage_demo.ts)
- 取消任务示例：[examples/cancel_task_demo.ts](examples/cancel_task_demo.ts)
- 自定义 Command 示例：[examples/custom_command_demo.ts](examples/custom_command_demo.ts)


## 核心 API

---

### createRedis(options?)

创建 Redis 客户端实例。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| options.host | `string` | 否 | Redis 主机，默认 `localhost` |
| options.port | `number` | 否 | Redis 端口，默认 `6379` |
| options.db | `number` | 否 | Redis 数据库编号，默认 `0` |
| options.username | `string` | 否 | Redis 用户名 |
| options.password | `string` | 否 | Redis 密码 |

**返回：** `Redis` - ioredis 客户端实例

---

### GatewayClient

向 Worker 发送消息的客户端。

**构造函数：**

```typescript
new GatewayClient(registry?: WorkerRegistry, redisClient?: Redis)
```

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| registry | `WorkerRegistry` | 否 | Worker 注册表，默认新建 |
| redisClient | `Redis` | 否 | Redis 客户端，默认使用全局实例 |

---

#### client.sendMessage(params)

发送消息给指定类型的 Worker 并等待响应。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| params.targetAgentType | `string` | 是 | 目标 Worker 类型 |
| params.sessionId | `string` | 是 | 会话 ID |
| params.content | `string \| BaiYingMessage \| BaiYingMessage[]` | 是 | 消息内容 |
| params.sourceAgentId | `string` | 否 | 源 Agent ID |
| params.traceId | `string` | 否 | 追踪 ID，自动生成 |
| params.tenantId | `string` | 否 | 租户 ID |
| params.actionType | `ActionType` | 否 | 动作类型 |
| params.payload | `Record<string, any>` | 否 | 额外载荷 |
| params.parentMessageId | `string` | 否 | 父消息 ID |
| params.messageId | `string` | 否 | 消息 ID，自动生成 |

**返回：** `Promise<SendMessageResponse>`

```typescript
interface SendMessageResponse {
    success: boolean;
    status: string;
    message_id: string;
    trace_id: string;
    target_worker_id: string;
    timestamp: number;
}
```

---

#### client.cancelTask(params)

取消指定任务。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| params.messageId | `string` | 是 | 消息 ID |
| params.sessionId | `string` | 是 | 会话 ID |
| params.reason | `string` | 否 | 取消原因 |
| params.targetAgentType | `string` | 否 | 目标 Agent 类型 |
| params.requestedBy | `string` | 否 | 请求方 |
| params.cancelMode | `'graceful' \| 'force'` | 否 | 取消模式，默认 `graceful` |

**返回：** `Promise<CancelTaskResponse>`

```typescript
interface CancelTaskResponse {
    success: boolean;
    message_id: string;
    execution_id: string;
    worker_id: string;
    status: 'NOT_FOUND' | 'ALREADY_FINISHED' | 'CANCEL_REQUESTED';
    timestamp: number;
    error?: string;
}
```

---

### WorkerRegistry

管理 Worker 注册和执行状态。

**构造函数：**

```typescript
new WorkerRegistry(redisClient?: Redis)
```

---

#### registry.registerWorker(workerId, capabilities)

注册一个 Worker 及其能力。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| workerId | `string` | 是 | Worker 唯一标识 |
| capabilities | `string[]` | 是 | Worker 具备的能力列表 |

**返回：** `Promise<void>`

---

#### registry.unregisterWorker(workerId)

注销一个 Worker。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| workerId | `string` | 是 | Worker 唯一标识 |

**返回：** `Promise<void>`

---

#### registry.getTargetWorker(agentId)

根据 Agent 类型获取一个可用的 Worker ID（随机）。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| agentId | `string` | 是 | Agent 类型标识 |

**返回：** `Promise<string \| null>`

---

#### registry.saveExecution(execution)

保存执行记录。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| execution | `Record<string, any>` | 是 | 执行记录对象 |

**返回：** `Promise<void>`

---

#### registry.getExecution(executionId)

根据执行 ID 获取执行记录。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| executionId | `string` | 是 | 执行记录 ID |

**返回：** `Promise<Record<string, any> \| null>`

---

#### registry.getExecutionByMessageId(messageId)

根据消息 ID 获取执行记录。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| messageId | `string` | 是 | 消息 ID |

**返回：** `Promise<Record<string, any> \| null>`

---

#### registry.markExecutionCancelling(executionId, reason)

标记执行状态为 CANCELLING。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| executionId | `string` | 是 | 执行记录 ID |
| reason | `string` | 是 | 取消原因 |

**返回：** `Promise<void>`

---

#### registry.markExecutionFinished(executionId, status)

标记执行已完成。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| executionId | `string` | 是 | 执行记录 ID |
| status | `string` | 是 | 最终状态，如 `COMPLETED`、`FAILED`、`CANCELLED` |

**返回：** `Promise<void>`

---

### WorkerRunner

订阅消息队列、处理消息、发送 ACK。

**构造函数：**

```typescript
new WorkerRunner(
    workerOrOptions: GatewayWorker | { workerId: string; capabilities: string[]; registry?: WorkerRegistry },
    options: {
        redisClient?: Redis;
        groupName?: string;
        maxConcurrency?: number;
    } = {}
)
```

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| workerOrOptions.workerId | `string` | 是 | Worker 唯一标识 |
| workerOrOptions.capabilities | `string[]` | 是 | Worker 能力列表 |
| workerOrOptions.registry | `WorkerRegistry` | 否 | 注册表实例 |
| options.redisClient | `Redis` | 否 | Redis 客户端（建议传入独立连接避免轮询阻塞） |
| options.groupName | `string` | 否 | 消费组名称 |
| options.maxConcurrency | `number` | 否 | 最大并发数 |

---

#### runner.initialize()

初始化环境：抢占 Worker ID 锁、设置 Stream、消费组、启动心跳。

**返回：** `Promise<void>`

---

#### runner.release()

优雅释放资源：停止心跳、释放锁。

**返回：** `Promise<void>`

---

#### runner.subscribe(handler, options?)

订阅任务消息，异步接收并处理。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| handler | `(message: { streamName: string; msgId: string; data: GatewayCommand }) => Promise<void> \| void` | 是 | 消息处理回调 |
| options.pollInterval | `number` | 否 | 轮询间隔（毫秒），默认 `1000` |

**返回：** `{ stop: () => void }` - 调用 `stop()` 停止订阅

**message 对象结构：**

```typescript
{
    streamName: string;  // 流名称
    msgId: string;       // 消息 ID
    data: GatewayCommand // 命令对象（AskAgentCommand / ResumeCommand / CancelTaskCommand 等）
}
```

---

#### runner.subscribeCancel(handler, options?)

订阅取消指令。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| handler | `(command: CancelTaskCommand) => Promise<void> \| void` | 是 | 取消指令处理回调 |
| options.pollInterval | `number` | 否 | 轮询间隔（毫秒），默认 `1000` |

**返回：** `{ stop: () => void }` - 调用 `stop()` 停止订阅

**CancelTaskCommand 属性：**

```typescript
{
    targetMessageId: string;     // 目标消息 ID
    targetExecutionId: string;   // 目标执行 ID
    workerId: string;            // Worker ID
    reason: string;              // 取消原因
    requestedBy: string;         // 请求方
    cancelMode: 'graceful' | 'force';  // 取消模式
}
```

---

#### runner.ack(streamName, msgId)

手动确认消息。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| streamName | `string` | 是 | 流名称 |
| msgId | `string` | 是 | 消息 ID |

**返回：** `Promise<void>`

---

#### runner.poll(options?)

手动轮询消息（与 subscribe 二选一）。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| options.count | `number` | 否 | 每次轮询最大消息数，默认 `10` |
| options.block | `number` | 否 | 阻塞时间（毫秒），默认 `2000` |

**返回：** `Promise<{ streamName: string; msgId: string; data: GatewayCommand }[]>`

---

### GatewayDataEmitter

向客户端推送流式数据和状态。

**构造函数：**

```typescript
new GatewayDataEmitter(redisClient?: Redis)
```

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| redisClient | `Redis` | 否 | Redis 客户端，默认使用全局实例 |

---

#### emitter.emitChunk(sessionId, traceId, event, options?)

推送流式数据块。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| sessionId | `string` | 是 | 会话 ID |
| traceId | `string` | 是 | 追踪 ID |
| event | `StreamChunkEvent \| string` | 是 | 数据内容（字符串或对象） |
| options.sourceAgentId | `string` | 否 | 源 Agent ID |
| options.eventType | `EventType` | 否 | 事件类型 |
| options.contentType | `string` | 否 | 消息类型，默认`SseMessageType.text` |
| options.metadata | `Record<string, any>` | 否 | 额外元数据 |

**返回：** `Promise<void>`

---

#### emitter.emitState(sessionId, traceId, event, options?)

推送任务状态变更。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| sessionId | `string` | 是 | 会话 ID |
| traceId | `string` | 是 | 追踪 ID |
| event | `StateChangeEvent \| string` | 是 | 状态值（字符串或对象） |
| options.sourceAgentId | `string` | 否 | 源 Agent ID |
| options.eventType | `EventType` | 否 | 事件类型 |
| options.contentType | `string` | 否 | 消息类型，默认`SseMessageType.text` |
| options.metadata | `Record<string, any>` | 否 | 额外元数据 |

**返回：** `Promise<void>`

---

#### emitter.emitArtifact(sessionId, traceId, event, options?)

推送产物（如文件）信息。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| sessionId | `string` | 是 | 会话 ID |
| traceId | `string` | 是 | 追踪 ID |
| event | `ArtifactEvent \| string` | 是 | 产物 URL（字符串或对象） |
| options.sourceAgentId | `string` | 否 | 源 Agent ID |
| options.metadata | `Record<string, any>` | 否 | 额外元数据 |

**返回：** `Promise<void>`

---

#### emitter.askUser(sessionId, traceId, event, options?)

向用户请求输入。

**参数：**

| 参数名 | 类型 | 必填 | 说明 |
|--------|------|------|------|
| sessionId | `string` | 是 | 会话 ID |
| traceId | `string` | 是 | 追踪 ID |
| event | `AskUserEvent \| string` | 是 | 提示内容（字符串或对象） |
| options.sourceAgentId | `string` | 否 | 源 Agent ID |
| options.metadata | `Record<string, any>` | 否 | 额外元数据 |

**返回：** `Promise<void>`

---

### AgentState

任务状态枚举。

```typescript
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
```

事件类型枚举。
```typescript
enum EventType {
    // 兼容原生 SseResponseEvent
    ANSWER_DELTA = "answerDelta",
    REASONING_LOG_DELTA = "reasoningLogDelta",
    REASONING_LOG_START = "reasoningLogStart",
    REASONING_LOG_END = "reasoningLogEnd",
    APP_STREAM_RESPONSE = "appStreamResponse",
    TASK_CREATE = "taskCreate",
    STEP_COMPLETE = "stepComplete",
    TASK_STOP = "taskStop",
}
```

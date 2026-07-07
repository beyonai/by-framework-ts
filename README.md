# By-Framework for TypeScript

<div align="center">

[![NPM](https://img.shields.io/npm/v/@byclaw/by-framework?color=blue)](https://www.npmjs.com/package/@byclaw/by-framework)
[![Node](https://img.shields.io/badge/node-18+-green.svg)](https://nodejs.org/)
[![Redis](https://img.shields.io/badge/redis-7.0+-red.svg)](https://redis.io/)
[![License](https://img.shields.io/badge/license-Apache_2.0-green.svg)](LICENSE)

</div>

<div align="center">

[**English**](README.md) | [**中文**](README_zh.md)

**Important Links:** [Documentation](https://beyonai.github.io/by-framework-docs) · [Java Version](https://beyonai.github.io/by-framework-java) · [Python Version](https://beyonai.github.io/by-framework-python)

</div>

---

**By-Framework (TypeScript SDK)** is a distributed, high-performance Agent scheduling engine built on Redis Streams, purpose-built for multi-agent systems.

## Challenges in Traditional Architecture

Traditional AI application architectures often face three critical challenges when dealing with Agent scenarios:

- **Full-link Synchronous Blocking $\rightarrow$ Forced "Manual Monitoring"** — Strong coupling between frontend and backend means tasks are interrupted if the page is closed. Users cannot switch devices or tasks, making workflows fragile to network fluctuations or interruptions.
- **Inability to Support Long-running Tasks $\rightarrow$ System "Constant Accompaniment"** — For reasoning tasks taking minutes or hours, callers must block threads and wait. This leads to gateway timeouts and massive waste of idle compute resources.
- **Inter-Agent Orchestration Recovery Dilemma** — In complex cascaded calls, if a timeout or interruption occurs, it's nearly impossible to accurately resume state. Developers are forced to build extremely complex persistent state machines.

## The By-Framework Solution

![Architecture Overview](./assets/img/architecture_en.png)

By-Framework addresses these issues through an asynchronous architecture with **separated Control and Data Planes**:

- **Instruction Asynchrony**: The APP sends control instructions to the **Control Queue** via the **Gateway Client**. Being asynchronous, the APP never blocks, and backend threads are released immediately.
- **Agent Cluster Consumption**: A distributed cluster of **Agents** competitively consumes messages from the control queue. Logical routing (Agent Type) provides native load balancing and elastic scaling.
- **Data Stream Feedback**: During execution, Agents asynchronously push chunks, state changes, and artifacts to the **Data Queue**. The APP listens via the **Gateway Client** for progress, natively supporting ultra-long tasks.
- **Native Orchestration & Resumption**: When an Agent needs to call another Agent, it sends a new instruction to the **Control Queue**. This message-based mechanism allows tasks to release resources while waiting and resume context precisely upon receiving a reply.

## Highlights

- 🚀 **Async & Event-Driven** — Control and data on separate Redis Streams; scale Workers without touching the delivery path
- 🧩 **Type Safety** — Full TypeScript support for robust distributed communication and superior developer experience
- 🔌 **Plugin System** — Hot-reloadable plugins with lifecycle hooks, tools, prompts, and sub-agent configs
- 🤝 **Inter-Agent Orchestration** — Built-in `callAgent`, `scatter-gather` fan-out, and user-in-the-loop patterns
- 🛡️ **Production-Ready** — Competitive consumption, graceful shutdown, message persistence, and execution state tracking


---

## Core Architecture

The SDK currently uses `agent_type` as the primary routing dimension:

- **Control Flow**: `GatewayClient` writes `AskAgentCommand` / `ResumeCommand` / `CancelTaskCommand` to Redis Streams. Standard tasks enter `byai_gateway:ctrl:agent_type:{agentType}`, while direct control commands enter `byai_gateway:ctrl:worker:{workerId}`.
- **Execution Flow**: `WorkerRunner` creates consumer groups for Workers, claims `workerId` online leases, starts heartbeats, and automatically polls, processes, ACKs, and updates execution states via `runner.start()`.
- **Data Flow**: `AgentContext` / `GatewayDataEmitter` writes streaming answers, reasoning logs, artifacts, and user input requests to `byai_gateway:session:{sessionId}:data_stream`.
- **Registry & State**: `WorkerRegistry` maintains Worker capabilities, online leases, and execution records aggregated by `sessionId` (including `messageId -> executionId` mapping).
- **Extensions**: Includes modules for plugin registration, interceptors, session history, workspaces, HookSandbox, file storage, and service discovery.

## Module Structure

- `src/client.ts`: Business-side task initiation, custom commands, and task cancellation.
- `src/worker.ts`: `GatewayWorker` abstract base class and `AnonymousWorker`.
- `src/runner.ts`: Worker lifecycle, consumer groups, auto-processing, and control flow cancellation.
- `src/context.ts`: `AgentContext` used by Workers for streaming, calling downstream agents, cancellation checks, etc.
- `src/emitter.ts`: Independent `GatewayDataEmitter` for data stream reporting.
- `src/registry.ts`: Worker online status and execution lifecycle tracking.
- `src/protocol/`: Command, event, state, response, and message protocol types.
- `src/extensions/`: Plugins and Agent configurations.
- `src/runtime/`: History, file storage, and runtime helper modules.
- `src/discovery/`: Service discovery client and registry.
- `examples/`: Local execution examples.
- `tests/`: Jest tests.

`dist/` contains build artifacts; do not edit manually.

## Installation

Prerequisites: Node.js 18+

```bash
npm install @byclaw/by-framework
```

Development commands:

```bash
npm run build
npm test
npm run dev
```

## Quick Start

### Creating a Worker

It is recommended to use `WorkerRunner.start()` to manage the Worker lifecycle. It initializes streams, starts heartbeats, processes tasks, automatically ACKs, and updates execution status upon task completion.

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

### Sending a Task

By default, `sendMessage` requires the target `agentType` to have at least one online Worker. If you need a "queue first, start Worker later" debug mode, set `requireOnlineWorker: false`.

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

### Cancelling a Task

Cancellation logic finds the execution using `sessionId + messageId`. If the task has been claimed by a Worker, a `CancelTaskCommand` is sent to that Worker's control flow. If it is still in the queue, the execution is marked as `CANCELLING`, and subsequent Workers will skip processing and mark it as `CANCELLED`.

```typescript
const cancelRes = await client.cancelTask({
    messageId: res.message_id,
    sessionId: 'test-session',
    reason: 'user aborted',
    requestedBy: 'frontend',
    cancelMode: 'graceful',
});
```

Worker business logic should periodically check for the cancellation signal in long-running tasks:

```typescript
for (const item of items) {
    await context.checkCancelled();
    await doWork(item);
}
```

### Calling Downstream Agents

Inside a Worker, you can dispatch further tasks via `AgentContext`:

```typescript
const child = await context.callAgent({
    targetAgentType: 'child-agent',
    content: 'Please continue this task',
    waitForReply: true,
    payload: { priority: 'normal' },
});
```

Scatter-Gather is also supported:

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

## Core API

### Redis

```typescript
createRedis(options?: {
    mode?: 'standalone' | 'cluster';
    host?: string;
    port?: number;
    db?: number;
    username?: string;
    password?: string;
    clusterNodes?: { host: string; port: number }[];
})
```

Default environment variables:

| Variable | Description |
|----------|-------------|
| `REDIS_MODE` | `standalone` (default) or `cluster` |
| `REDIS_HOST` | Redis host, default `localhost` |
| `REDIS_PORT` | Redis port, default `6379` |
| `REDIS_DB` | Redis DB index, default `0` |
| `REDIS_USERNAME` | Redis username, optional |
| `REDIS_PASSWORD` | Redis password, optional |
| `REDIS_CLUSTER_NODES` | Comma-separated `host:port` list, used when `mode=cluster` |
| `REDIS_KEY_SCHEMA_VERSION` | `v1` (default, unprefixed keys) or `v2` (hash-tagged keys); `mode=cluster` requires `v2` |

`runWorker` also reads:

| Variable | Description |
|----------|-------------|
| `BYAI_WORKER_CONCURRENCY` | Max worker concurrency, default `50` |
| `BYAI_WORKER_FETCH_COUNT` | Batch size for fetching tasks, default `10` |
| `BYAI_REDIS_MAX_CONNECTIONS` | Redis connection pool config (aligned with Python SDK) |

### GatewayClient

```typescript
new GatewayClient(
    registry?: WorkerRegistry,
    redisClient?: Redis,
    interceptors?: GatewayInterceptor[]
)
```

#### `client.sendMessage(params)`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `targetAgentType` | `string` | Yes | Target agent type |
| `sessionId` | `string` | Yes | Session ID |
| `content` | `string \| BaiYingMessage \| BaiYingMessage[]` | Yes | Message content |
| `sourceAgentType` | `string` | No | Source agent type (for inter-agent callbacks) |
| `traceId` | `string` | No | Trace ID (auto-generated) |
| `userCode` | `string` | No | Tenant or user code |
| `userName` | `string` | No | User name |
| `actionType` | `ActionType` | No | Default `ActionType.ASK_AGENT`; can be `ActionType.RESUME` |
| `extraPayload` | `Record<string, unknown>` | No | Protocol `body.extra_payload` |
| `metadata` | `Record<string, unknown>` | No | Protocol `header.metadata` |
| `parentMessageId` | `string` | No | Parent message ID |
| `messageId` | `string` | No | Message ID (auto-generated) |
| `targetWorkerId` | `string` | No | Direct delivery to a specific Worker |
| `requireOnlineWorker` | `boolean` | No | Require an online worker (default `true`) |

Returns:

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

Sends a `BaseCommand` or custom command. If `streamName` is omitted, it routes to the `agent_type` stream based on `command.header.targetAgentType`.

#### `client.cancelTask(params)`

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `messageId` | `string` | Yes | ID of the message to cancel |
| `sessionId` | `string` | Yes | Session ID |
| `reason` | `string` | No | Reason for cancellation |
| `targetAgentType` | `string` | No | Target agent type (usually optional) |
| `requestedBy` | `string` | No | Requester (default `client`) |
| `cancelMode` | `'graceful' \| 'force'` | No | Default `graceful` |

Returns:

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

Common methods:

| Method | Description |
|--------|-------------|
| `initialize()` | Claims Worker ID, creates consumer group, starts heartbeats and control loop |
| `start({ handleSignals? })` | Recommended entry point: auto-init, poll, process, ACK, and cleanup |
| `stop()` | Stops the main loop and control flow loop |
| `release()` | Stops heartbeats, releases Worker ID, and closes Runner-owned Redis connections |
| `poll({ count?, block? })` | Manual read from agent-type control stream |
| `processAndAck(streamName, msgId, data)` | Executes business logic, ACKs, and maintains execution state |
| `ack(streamName, msgId)` | Manual ACK |
| `runControlOnce(block?)` | Manual single read from Worker control stream |

Most applications should use `start()`.

### GatewayWorker

Implement the following when inheriting from `GatewayWorker`:

```typescript
abstract getAgentTypes(): ReadonlyArray<string>;
abstract processCommand(command: GatewayCommand, context: AgentContext): Promise<ProcessCommandResult>;
```

`processCommand` can return:
- `AgentTaskResult`
- `{ status, content, replyData, metadata, extraPayload }`
- Any JSON-serializable object (used as `replyData`)
- An `AgentState` string

The base class automatically handles:
- Saving user messages to history.
- Creating `AgentContext`.
- Injecting session history into `extraPayload.history`.
- Catching cancellation and returning `CANCELLED`.
- Emitting `FINAL_ANSWER` and `APP_STREAM_RESPONSE` upon completion.
- Wrapping results in a `ResumeCommand` for upstream agents if `sourceAgentType` exists.

### AgentContext

| Method | Description |
|--------|-------------|
| `emitChunk(event, eventType?)` | Push streaming answer, default `answerDelta` |
| `emitState(event, eventType?)` | Push state or reasoning log, default `reasoningLogDelta` |
| `emitArtifact(event, eventType?)` | Push file artifacts |
| `askUser(event)` | Request user input and mark task as waiting |
| `callAgent(params)` | Call a downstream Agent |
| `dispatchGroup(params)` | Dispatch multiple downstream tasks in parallel |
| `collectGroupResults(taskGroupId, timeout?)` | Collect results for a task group |
| `checkCancelled()` | Throws `TaskCancelledError` if task was cancelled |
| `isCancelRequested()` | Check for cancellation signal |
| `updateExecutionState(status)` | Update current execution state |

### GatewayDataEmitter

```typescript
new GatewayDataEmitter(redisClient?: Redis, params?: {
    sourceAgentType?: string;
    dataStreamName?: string;
})
```

All events are written to `QueueNames.session_data_stream(sessionId)` by default.

### WorkerRegistry

Worker Status:
| Method | Description |
|--------|-------------|
| `registerWorker(workerId, agentTypes)` | Register capabilities and send initial heartbeat |
| `heartbeatWorker(workerId, leaseTtlSeconds?)` | Refresh online lease |
| `unregisterWorker(workerId)` | Remove online status and membership |
| `claimWorkerId(workerId, ttlSeconds?)` | Mutex lock for Worker ID |
| `isWorkerOnline(workerId)` | Check if a worker is online |
| `getTargetWorker(agentType)` | Get a random online worker for an agent type |

Execution State:
| Method | Description |
|--------|-------------|
| `initializeExecution(execution)` | Init execution with timeline |
| `getExecution(executionId, sessionId)` | Query execution state |
| `updateExecutionStatus(executionId, sessionId, status, extra?)` | Update status and timeline |
| `markExecutionFinished(executionId, sessionId, status)` | Mark final state with end time |

## Protocol Enums

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

## Related Examples

- [examples/demo_worker.ts](examples/demo_worker.ts): GatewayWorker implementation example.
- [examples/send_test.ts](examples/send_test.ts): Task initiation example.
- [examples/cancel_task_demo.ts](examples/cancel_task_demo.ts): Cancellation example.

## Development Tips

- Run `npm test` after modifying routing, Redis behavior, or lifecycle logic.
- Keep protocol fields in `snake_case` (e.g., `target_agent_type`, `message_id`).
- Source code uses strict TypeScript, CommonJS output, 4-space indentation, single quotes, and semicolons.

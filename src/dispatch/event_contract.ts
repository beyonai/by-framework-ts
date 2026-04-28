/**
 * Event-driven gateway contract (publish path and outbound streams).
 *
 * **Control plane (inbound to workers)**  
 * - Stream name: `QueueNames.ctrl_stream(agentType)` → `byai_gateway:ctrl:agent_type:${agentType}`  
 * - Each entry: field **`data`**, value **`JSON.stringify(command.toDict())`** where `command` is typically
 *   `AskAgentCommand` with `action_type === ASK_AGENT`.  
 * - This is a **fire-and-forget append** to Redis Streams; the publisher does not `XREAD` for a reply here.
 *
 * **Session plane (outbound events)**  
 * - Stream name: `QueueNames.session_data_stream(sessionId)` → `byai_gateway:session:${sessionId}:data_stream`  
 * - Payloads are JSON objects consumed by gateways/UI; they include **`event_type`** (`EventType`)
 *   and correlation fields such as **`trace_id`**, **`session_id`**, **`message_id`** / `source_agent_type`
 *   as emitted by `GatewayDataEmitter`.
 *
 * **`wait_for_reply`** in `AskAgentCommand` body is a **protocol hint** for consumers, not an SDK-level
 * blocking read on Redis.
 *
 * @packageDocumentation
 */

export {};

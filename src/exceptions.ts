/**
 * Gateway SDK Exception Definitions
 *
 * All business exceptions are defined in this module.
 * Usage of raw `throw new Error()` is prohibited in business code.
 */

export class GatewaySDKError extends Error {
  cause: Error | null;
  code: string;

  constructor(message: string, cause?: Error | null) {
    super(message);
    this.name = 'GatewaySDKError';
    this.cause = cause ?? null;
    this.code = this.constructor.name;
  }
}

// === Redis Related Exceptions ===

export class RedisConnectionError extends GatewaySDKError {
  constructor(message: string = 'Failed to connect to Redis', cause?: Error | null) {
    super(message, cause);
    this.name = 'RedisConnectionError';
  }
}

export class StreamGroupExistsError extends GatewaySDKError {
  groupName: string;
  streamName: string;

  constructor(groupName: string, streamName: string) {
    super(`Consumer group '${groupName}' already exists in stream '${streamName}'`);
    this.name = 'StreamGroupExistsError';
    this.groupName = groupName;
    this.streamName = streamName;
  }
}

// === Execution Related Exceptions ===

export class ExecutionNotFoundError extends GatewaySDKError {
  executionId: string;
  sessionId: string;

  constructor(executionId: string, sessionId: string = '') {
    let msg = `Execution not found: ${executionId}`;
    if (sessionId) {
      msg += ` (session: ${sessionId})`;
    }
    super(msg);
    this.name = 'ExecutionNotFoundError';
    this.executionId = executionId;
    this.sessionId = sessionId;
  }
}

export class ExecutionDataError extends GatewaySDKError {
  executionId: string;

  constructor(executionId: string, cause?: Error | null) {
    super(`Failed to parse execution data for ${executionId}`, cause);
    this.name = 'ExecutionDataError';
    this.executionId = executionId;
  }
}

export class SessionMismatchError extends GatewaySDKError {
  messageId: string;
  expectedSession: string;
  actualSession: string;

  constructor(messageId: string, expectedSession: string, actualSession: string) {
    super(
      `Session mismatch for message ${messageId}: expected ${expectedSession}, got ${actualSession}`
    );
    this.name = 'SessionMismatchError';
    this.messageId = messageId;
    this.expectedSession = expectedSession;
    this.actualSession = actualSession;
  }
}

export class TerminalStateError extends GatewaySDKError {
  executionId: string;
  currentStatus: string;

  constructor(executionId: string, currentStatus: string) {
    super(`Execution ${executionId} is already in terminal state: ${currentStatus}`);
    this.name = 'TerminalStateError';
    this.executionId = executionId;
    this.currentStatus = currentStatus;
  }
}

// === Message Handling Exceptions ===

export class UnsupportedCommandError extends GatewaySDKError {
  commandType: string;

  constructor(commandType: string) {
    super(`Unsupported command type: ${commandType}`);
    this.name = 'UnsupportedCommandError';
    this.commandType = commandType;
  }
}

export class MessageParseError extends GatewaySDKError {
  messageId: string;

  constructor(messageId: string = '', cause?: Error | null) {
    let msg = 'Failed to parse message';
    if (messageId) {
      msg += `: ${messageId}`;
    }
    super(msg, cause);
    this.name = 'MessageParseError';
    this.messageId = messageId;
  }
}

export class MessageDataNotFoundError extends GatewaySDKError {
  messageId: string;

  constructor(messageId: string = '') {
    let msg = 'Message data not found';
    if (messageId) {
      msg += `: ${messageId}`;
    }
    super(msg);
    this.name = 'MessageDataNotFoundError';
    this.messageId = messageId;
  }
}

// === Worker Related Exceptions ===

export class WorkerNotFoundError extends GatewaySDKError {
  agentType: string;

  constructor(agentType: string) {
    super(`No worker found for agent type: ${agentType}`);
    this.name = 'WorkerNotFoundError';
    this.agentType = agentType;
  }
}

export class WorkerLockError extends GatewaySDKError {
  workerId: string;

  constructor(workerId: string) {
    super(`Worker ID already in use: ${workerId}`);
    this.name = 'WorkerLockError';
    this.workerId = workerId;
  }
}

export class WorkerRegistryNotSetError extends GatewaySDKError {
  operation: string;

  constructor(operation: string) {
    super(`GatewayClient requires a WorkerRegistry to ${operation}`);
    this.name = 'WorkerRegistryNotSetError';
    this.operation = operation;
  }
}

// === Command Validation Exceptions ===

export class CommandValidationError extends GatewaySDKError {
  commandType: string;
  reason: string;

  constructor(commandType: string, reason: string) {
    super(`Validation failed for ${commandType}: ${reason}`);
    this.name = 'CommandValidationError';
    this.commandType = commandType;
    this.reason = reason;
  }
}

// === Suspended-Caller Liveness Exceptions ===

/**
 * A wait-index ZSET member could not be decoded (see src/liveness/wait_index.ts).
 *
 * The member encoding is a cross-SDK wire contract, so this is the shape a
 * Python/TS/Java encoding drift surfaces as: the member on the wire does not
 * round-trip through this SDK's decoder. `reason` therefore has to stay
 * specific ("dangling escape" vs. a wrong field count) — it is the only
 * evidence available for telling an encoder bug from a corrupted key.
 *
 * The wait sweeper catches this per entry and drops just that member, so one
 * bad member cannot poison a whole shard.
 */
export class WaitIndexMemberError extends GatewaySDKError {
  member: string;
  reason: string;

  constructor(member: string, reason: string) {
    super(`Invalid wait-index member (${reason}): ${JSON.stringify(member)}`);
    this.name = 'WaitIndexMemberError';
    this.member = member;
    this.reason = reason;
  }
}

// === HTTP Related Exceptions ===

export class HttpClientError extends GatewaySDKError {
  statusCode?: number;
  url: string;

  constructor(message: string, url: string, statusCode?: number, cause?: Error | null) {
    super(message, cause);
    this.name = 'HttpClientError';
    this.url = url;
    this.statusCode = statusCode;
  }
}

export class HttpRequestError extends GatewaySDKError {
  url: string;

  constructor(message: string, url: string, cause?: Error | null) {
    super(message, cause);
    this.name = 'HttpRequestError';
    this.url = url;
  }
}

// === Service Discovery Exceptions ===

export class DiscoveryHttpClientError extends HttpClientError {
  constructor(message: string) {
    super(message, '');
    this.name = 'DiscoveryHttpClientError';
  }
}

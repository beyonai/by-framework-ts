/**
 * Gateway SDK Exception Definitions
 *
 * All business exceptions are defined in this module.
 * Usage of raw `throw new Error()` is prohibited in business code.
 */

export class GatewaySDKError extends Error {
  cause: Error | null;

  constructor(message: string, cause?: Error | null) {
    super(message);
    this.name = 'GatewaySDKError';
    this.cause = cause ?? null;
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

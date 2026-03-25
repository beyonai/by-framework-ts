import { ActionType } from './action_type';
import { MessageHeader } from './message_header';

function hasContent(value: string | ReadonlyArray<unknown>): boolean {
    if (typeof value === 'string') {
        return value.trim() !== '';
    }
    return Array.isArray(value) && value.length > 0;
}

export abstract class BaseCommand {
    static actionType: string;

    abstract readonly actionType: string;
    constructor(public readonly header: MessageHeader) {}

    abstract toDict(): Readonly<Record<string, unknown>>;

    toRedisPayload(): Readonly<Record<string, string>> {
        return { data: JSON.stringify(this.toDict()) };
    }

    injectRuntimePayload(_payload: Readonly<Record<string, unknown>>): void {}

    static fromDict(_data: Readonly<Record<string, unknown>>): BaseCommand {
        throw new Error('BaseCommand.fromDict must be implemented');
    }
}

export class AskAgentCommand extends BaseCommand {
    static actionType = ActionType.ASK_AGENT;
    readonly actionType = ActionType.ASK_AGENT;

    constructor(
        public readonly header: MessageHeader,
        public readonly content: string | ReadonlyArray<unknown>,
        public readonly waitForReply: boolean = false,
        public readonly extraPayload: Readonly<Record<string, unknown>> = {}
    ) {
        super(header);
        if (!hasContent(content)) {
            throw new Error('AskAgentCommand requires non-empty content');
        }
    }

    toDict(): Readonly<Record<string, unknown>> {
        const body: Record<string, unknown> = {
            content: this.content,
            wait_for_reply: this.waitForReply,
        };
        if (Object.keys(this.extraPayload).length > 0) {
            body.extra_payload = { ...this.extraPayload };
        }
        return {
            action_type: this.actionType,
            header: this.header.toDict(),
            body,
        };
    }

    injectRuntimePayload(payload: Readonly<Record<string, unknown>>): void {
        Object.assign(this.extraPayload, payload);
    }

    static fromDict(data: Readonly<Record<string, unknown>>): AskAgentCommand {
        const body = { ...(data.body as Record<string, unknown> || {}) };
        return new AskAgentCommand(
            MessageHeader.fromDict(data.header as Record<string, unknown>),
            (body.content as string | ReadonlyArray<unknown>) || '',
            Boolean(body.wait_for_reply),
            { ...(body.extra_payload as Record<string, unknown> || {}) }
        );
    }
}

export class ResumeCommand extends BaseCommand {
    static actionType = ActionType.RESUME;
    readonly actionType = ActionType.RESUME;

    constructor(
        public readonly header: MessageHeader,
        public readonly content: string | ReadonlyArray<unknown> = '',
        public readonly status: string = '',
        public readonly replyData: unknown = null,
        public readonly extraPayload: Readonly<Record<string, unknown>> = {}
    ) {
        super(header);
        if (!status && !hasContent(content)) {
            throw new Error('ResumeCommand requires status or content');
        }
    }

    toDict(): Readonly<Record<string, unknown>> {
        const body: Record<string, unknown> = {
            content: this.content,
            status: this.status,
            reply_data: this.replyData,
        };
        if (Object.keys(this.extraPayload).length > 0) {
            body.extra_payload = { ...this.extraPayload };
        }
        return {
            action_type: this.actionType,
            header: this.header.toDict(),
            body,
        };
    }

    injectRuntimePayload(payload: Readonly<Record<string, unknown>>): void {
        Object.assign(this.extraPayload, payload);
    }

    static fromDict(data: Readonly<Record<string, unknown>>): ResumeCommand {
        const body = { ...(data.body as Record<string, unknown> || {}) };
        return new ResumeCommand(
            MessageHeader.fromDict(data.header as Record<string, unknown>),
            (body.content as string | ReadonlyArray<unknown>) || '',
            (body.status as string) || '',
            body.reply_data,
            { ...(body.extra_payload as Record<string, unknown> || {}) }
        );
    }
}

export class CancelTaskCommand extends BaseCommand {
    static actionType = ActionType.CANCEL_TASK;
    readonly actionType = ActionType.CANCEL_TASK;

    constructor(
        public readonly header: MessageHeader,
        public readonly targetMessageId: string,
        public readonly targetExecutionId: string = '',
        public readonly targetWorkerId: string = '',
        public readonly reason: string = '',
        public readonly requestedBy: string = '',
        public readonly cancelMode: 'graceful' | 'force' = 'graceful'
    ) {
        super(header);
        if (!targetMessageId) {
            throw new Error('CancelTaskCommand requires targetMessageId');
        }
        if (!['graceful', 'force'].includes(cancelMode)) {
            throw new Error('CancelTaskCommand cancelMode must be graceful or force');
        }
    }

    toDict(): Readonly<Record<string, unknown>> {
        return {
            action_type: this.actionType,
            header: this.header.toDict(),
            body: {
                target_message_id: this.targetMessageId,
                target_execution_id: this.targetExecutionId,
                target_worker_id: this.targetWorkerId,
                reason: this.reason,
                requested_by: this.requestedBy,
                cancel_mode: this.cancelMode,
            },
        };
    }

    static fromDict(data: Readonly<Record<string, unknown>>): CancelTaskCommand {
        const body = { ...(data.body as Record<string, unknown> || {}) };
        return new CancelTaskCommand(
            MessageHeader.fromDict(data.header as Record<string, unknown>),
            (body.target_message_id as string) || '',
            (body.target_execution_id as string) || '',
            (body.target_worker_id as string) || '',
            (body.reason as string) || '',
            (body.requested_by as string) || '',
            (body.cancel_mode as 'graceful' | 'force') || 'graceful'
        );
    }
}

export type GatewayCommand = BaseCommand;
export type CommandConstructor<T extends BaseCommand = BaseCommand> = {
    actionType: string;
    fromDict(data: Readonly<Record<string, unknown>>): T;
};

const COMMAND_REGISTRY = new Map<string, CommandConstructor>();

export function registerCommand<T extends BaseCommand>(commandCtor: CommandConstructor<T>): CommandConstructor<T> {
    if (!commandCtor.actionType) {
        throw new Error('Command constructor must define static actionType');
    }
    COMMAND_REGISTRY.set(commandCtor.actionType, commandCtor);
    return commandCtor;
}

export function unregisterCommand(actionType: string): void {
    COMMAND_REGISTRY.delete(actionType);
}

export function getRegisteredCommand(actionType: string): CommandConstructor | undefined {
    return COMMAND_REGISTRY.get(actionType);
}

export function commandFromDict(data: Readonly<Record<string, unknown>>): GatewayCommand {
    const commandCtor = getRegisteredCommand(String(data.action_type));
    if (!commandCtor) {
        throw new Error(`Unsupported action_type: ${String(data.action_type)}`);
    }
    return commandCtor.fromDict(data);
}

registerCommand(AskAgentCommand);
registerCommand(ResumeCommand);
registerCommand(CancelTaskCommand);

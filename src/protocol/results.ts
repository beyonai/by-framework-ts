import { AgentState } from './agent_state';

export type JsonValue =
    | string
    | number
    | boolean
    | null
    | ReadonlyArray<JsonValue>
    | { readonly [key: string]: JsonValue };

export type WireContent = string | ReadonlyArray<Readonly<Record<string, JsonValue>>>;

export class AgentTaskResult {
    public readonly status: string;
    public readonly content: WireContent;
    public readonly replyData: JsonValue;
    public readonly metadata: Readonly<Record<string, JsonValue>>;
    public readonly extraPayload: Readonly<Record<string, JsonValue>>;

    constructor(options: {
        readonly status?: string;
        readonly content?: WireContent;
        readonly replyData?: JsonValue;
        readonly metadata?: Readonly<Record<string, JsonValue>>;
        readonly extraPayload?: Readonly<Record<string, JsonValue>>;
    } = {}) {
        this.status = options.status ?? AgentState.COMPLETED;
        this.content = options.content ?? '';
        this.replyData = options.replyData ?? null;
        this.metadata = options.metadata ?? {};
        this.extraPayload = options.extraPayload ?? {};
    }
}

export type ProcessCommandResult = AgentTaskResult | JsonValue;

const RESULT_FIELDS = new Set([
    'status',
    'content',
    'replyData',
    'reply_data',
    'metadata',
    'extraPayload',
    'extra_payload',
]);

export function ensureJsonSerializable(value: unknown, path = 'value'): JsonValue {
    if (
        value === null
        || typeof value === 'string'
        || typeof value === 'number'
        || typeof value === 'boolean'
    ) {
        return value;
    }
    if (Array.isArray(value)) {
        return value.map((item, index) => ensureJsonSerializable(item, `${path}[${index}]`));
    }
    if (isPlainRecord(value)) {
        const serialized: Record<string, JsonValue> = {};
        for (const [key, item] of Object.entries(value)) {
            serialized[key] = ensureJsonSerializable(item, `${path}.${key}`);
        }
        return serialized;
    }
    throw new TypeError(
        `processCommand return value must be JSON serializable; got ${typeName(value)} at ${path}`
    );
}

export function normalizeProcessResult(result: unknown): AgentTaskResult {
    if (result instanceof AgentTaskResult) {
        return new AgentTaskResult({
            status: result.status,
            content: ensureWireContent(result.content),
            replyData: ensureJsonSerializable(result.replyData, 'replyData'),
            metadata: ensureJsonObject(result.metadata, 'metadata'),
            extraPayload: ensureJsonObject(result.extraPayload, 'extraPayload'),
        });
    }

    if (typeof result === 'string' && Object.values(AgentState).includes(result as AgentState)) {
        return new AgentTaskResult({ status: result });
    }

    if (isPlainRecord(result)) {
        const metadata = 'metadata' in result
            ? ensureJsonObject(result.metadata, 'metadata')
            : {};
        const keys = Object.keys(result);
        const isStructured = (
            'replyData' in result
            || 'reply_data' in result
            || 'content' in result
            || (keys.length > 0 && keys.every((key) => RESULT_FIELDS.has(key)))
        );
        if (isStructured) {
            return new AgentTaskResult({
                status: typeof result.status === 'string' ? result.status : AgentState.COMPLETED,
                content: ensureWireContent(result.content ?? ''),
                replyData: ensureJsonSerializable(result.replyData ?? result.reply_data ?? null, 'replyData'),
                metadata,
                extraPayload: ensureJsonObject(
                    result.extraPayload ?? result.extra_payload ?? {},
                    'extraPayload'
                ),
            });
        }
        return new AgentTaskResult({
            status: typeof result.status === 'string' ? result.status : AgentState.COMPLETED,
            replyData: ensureJsonSerializable(result, 'replyData'),
            metadata,
        });
    }

    return new AgentTaskResult({
        replyData: ensureJsonSerializable(result, 'replyData'),
    });
}

function ensureWireContent(value: unknown, path = 'content'): WireContent {
    if (typeof value === 'string') {
        return value;
    }
    if (Array.isArray(value)) {
        const serialized = ensureJsonSerializable(value, path);
        if (Array.isArray(serialized) && serialized.every(isPlainRecord)) {
            return serialized as ReadonlyArray<Readonly<Record<string, JsonValue>>>;
        }
    }
    throw new TypeError(
        `processCommand return content must be a string or array of records; got ${typeName(value)} at ${path}`
    );
}

function ensureJsonObject(value: unknown, path: string): Readonly<Record<string, JsonValue>> {
    const serialized = ensureJsonSerializable(value, path);
    if (!isPlainRecord(serialized)) {
        throw new TypeError(
            `processCommand return metadata fields must be JSON objects; got ${typeName(value)} at ${path}`
        );
    }
    return serialized;
}

function isPlainRecord(value: unknown): value is Record<string, unknown> {
    if (value === null || typeof value !== 'object') {
        return false;
    }
    const prototype = Object.getPrototypeOf(value);
    return prototype === Object.prototype || prototype === null;
}

function typeName(value: unknown): string {
    if (value === null) {
        return 'null';
    }
    if (value === undefined) {
        return 'undefined';
    }
    if (typeof value === 'object' && value.constructor?.name) {
        return value.constructor.name;
    }
    return typeof value;
}

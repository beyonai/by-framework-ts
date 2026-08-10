import { ContentCodec, WireContent } from './content_codec';
import { BaiYingMessage, MessageContent, MessageFile, Resource } from './message';

function isWireMessage(item: unknown): item is Record<string, unknown> {
    return typeof item === 'object' && item !== null && 'role' in item && 'content' in item;
}

function isMessageContent(payload: unknown): payload is Record<string, unknown> {
    return typeof payload === 'object' && payload !== null && !Array.isArray(payload);
}

function serializeMessage(message: BaiYingMessage): Record<string, unknown> {
    const content = message.content;
    if (isMessageContent(content)) {
        const mc = content as MessageContent;
        return {
            role: message.role,
            content: {
                text: mc.text,
                files: mc.files ?? [],
                resources: mc.resources ?? [],
            },
        };
    }
    return { role: message.role, content };
}

function deserializeMessage(item: Record<string, unknown>): BaiYingMessage {
    const role = item.role as BaiYingMessage['role'];
    const payload = item.content;
    if (isMessageContent(payload)) {
        const content: MessageContent = {
            text: (payload.text as string) ?? '',
            files: (payload.files as MessageFile[]) ?? [],
            resources: (payload.resources as Resource[]) ?? [],
        };
        return { role, content };
    }
    return { role, content: payload as string };
}

/** Convert BaiYing domain objects into protocol-safe wire payloads. */
export function serializeByaiContent(content: unknown): WireContent {
    if (typeof content === 'string') {
        return content;
    }
    if (Array.isArray(content)) {
        return content.map((item) => (isWireMessage(item) ? serializeMessage(item as unknown as BaiYingMessage) : item)) as WireContent;
    }
    if (isWireMessage(content)) {
        return [serializeMessage(content as unknown as BaiYingMessage)] as WireContent;
    }
    return content as WireContent;
}

/** Convert protocol wire payloads into BaiYing domain objects when applicable. */
export function deserializeByaiContent(content: unknown): unknown {
    if (typeof content === 'string') {
        return content;
    }
    if (!Array.isArray(content) || content.length === 0) {
        return content;
    }
    if (!content.every((item) => isWireMessage(item))) {
        return content;
    }

    const messages = content.map((item) => deserializeMessage(item as Record<string, unknown>));
    return messages.length === 1 ? messages[0] : messages;
}

export class ByaiContentCodec implements ContentCodec {
    serialize(content: unknown): WireContent {
        return serializeByaiContent(content);
    }

    deserialize(content: WireContent): unknown {
        return deserializeByaiContent(content);
    }
}

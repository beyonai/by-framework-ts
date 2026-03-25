export enum BaiYingMessageRole {
    USER = 'user',
    ASSISTANT = 'assistant',
    SYSTEM = 'system',
    TOOL_CALL = 'tool-call',
    TOOL_RESPONSE = 'tool-response',
    RESPONSE_TO_SUB_AGENT = 'response-to-sub-agent',
}

export interface MessageFile {
    fileId: number;
    fileUrl: string;
    fileType: 'image' | 'file' | 'video' | 'audio';
    fileName: string;
}

export interface Resource {
    resourceId: string;
    resourceName: string;
    resourceType: string;
    id?: string;
    path?: string;
    resourceDesc?: string;
    resourceMetaData?: Record<string, any>;
}

export interface MessageContent {
    text: string;
    files?: MessageFile[];
    resources?: Resource[];
}

export interface BaiYingMessage {
    role: BaiYingMessageRole | string;
    content: string | MessageContent;
}

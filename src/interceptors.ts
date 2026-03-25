import { BaiYingMessage } from './protocol/message';

export interface GatewayInterceptor {
    beforeSend?(params: Record<string, any>): Record<string, any> | Promise<Record<string, any>>;
    afterSend?(response: Record<string, any>): Record<string, any> | Promise<Record<string, any>>;
}

export class ByaiMessageInterceptor implements GatewayInterceptor {
    beforeSend(params: Record<string, any>): Record<string, any> {
        const content = params.content;
        if (typeof content === 'string') {
            return params;
        }

        const messages: BaiYingMessage[] = Array.isArray(content) ? content : [content];
        return {
            ...params,
            content: messages.map((msg) => ({
                role: msg.role,
                content: msg.content,
            })),
        };
    }
}


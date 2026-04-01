import { BaiYingMessage } from './protocol/message';

export interface GatewayInterceptor {
    beforeSend?(params: Record<string, any>): Record<string, any> | Promise<Record<string, any>>;
    afterSend?(response: Record<string, any>): Record<string, any> | Promise<Record<string, any>>;
}

export class ByaiMessageInterceptor implements GatewayInterceptor {
    beforeSend(params: Record<string, any>): Record<string, any> {
        const content = params.content;
        if (content === undefined || content === null) {
            return params;
        }

        return {
            ...params,
            content: this.formatContent(content),
        };
    }

    private formatContent(content: any): any {
        if (typeof content === 'string') {
            return content;
        }

        // Consistent list-based processing
        const inputList: any[] = Array.isArray(content) ? content : [content];
        const formattedContent: any[] = [];

        for (const m of inputList) {
            if (typeof m === 'object' && m !== null) {
                // Handle dict format
                if (!('role' in m) && !('content' in m)) {
                    formattedContent.push(m);
                    continue;
                }

                const role = m.role;
                const msgContent = m.content;

                // Handle specialized MessageContent objects
                if (msgContent && typeof msgContent === 'object') {
                    formattedContent.push({
                        role: role,
                        content: {
                            text: (msgContent as any).text || '',
                            files: this.extractObjects((msgContent as any).files),
                            resources: this.extractObjects((msgContent as any).resources),
                        },
                    });
                } else {
                    formattedContent.push({ role: role, content: msgContent });
                }
            } else {
                formattedContent.push(m);
            }
        }

        return formattedContent;
    }

    private extractObjects(items: any[] | undefined): any[] {
        if (!items || !Array.isArray(items)) {
            return [];
        }
        return items.map((item) => (typeof item === 'object' && item !== null ? item : {}));
    }
}

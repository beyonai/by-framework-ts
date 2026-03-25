import { Redis } from 'ioredis';
import { GatewayClient } from './client';
import { GatewayInterceptor } from './interceptors';
import { SendMessageResponse } from './protocol/responses';
import { WorkerRegistry } from './registry';

export class ByaiGatewayClient extends GatewayClient {
    private readonly interceptor?: GatewayInterceptor;

    public constructor(interceptor?: GatewayInterceptor, registry?: WorkerRegistry, redisClient?: Redis) {
        super(registry, redisClient);
        this.interceptor = interceptor;
    }

    async sendMessage(params: Parameters<GatewayClient['sendMessage']>[0]): Promise<SendMessageResponse> {
        let request: Record<string, unknown> = params as unknown as Record<string, unknown>;
        if (this.interceptor?.beforeSend) {
            request = await this.interceptor.beforeSend(request as unknown as Record<string, unknown>) as unknown as Record<string, unknown>;
        }
        let response: SendMessageResponse = await super.sendMessage(request as unknown as Parameters<GatewayClient['sendMessage']>[0]);
        if (this.interceptor?.afterSend) {
            response = (await this.interceptor.afterSend(response)) as SendMessageResponse;
        }
        return response;
    }
}

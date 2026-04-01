import { Redis } from 'ioredis';
import { GatewayClient } from './client';
import { GatewayInterceptor, ByaiMessageInterceptor } from './interceptors';
import { SendMessageResponse } from './protocol/responses';
import { WorkerRegistry } from './registry';

export class ByaiGatewayClient extends GatewayClient {
    public constructor(interceptors?: GatewayInterceptor[], registry?: WorkerRegistry, redisClient?: Redis) {
        // 1. Start with the ByaiMessageInterceptor
        const defaultInterceptors: GatewayInterceptor[] = [new ByaiMessageInterceptor()];

        // 2. Append any additional user-provided interceptors
        if (interceptors && interceptors.length > 0) {
            defaultInterceptors.push(...interceptors);
        }

        // 3. Initialize the base GatewayClient with interceptors
        super(registry, redisClient, defaultInterceptors);
    }

    async sendMessage(params: Parameters<GatewayClient['sendMessage']>[0]): Promise<SendMessageResponse> {
        let request: Record<string, unknown> = params as unknown as Record<string, unknown>;

        // Get all interceptors (from parent class)
        const interceptors = (this as any).interceptors as GatewayInterceptor[];

        // Run all interceptors beforeSend
        for (const interceptor of interceptors) {
            if (interceptor.beforeSend) {
                request = await interceptor.beforeSend(request) as Record<string, unknown>;
            }
        }

        let response: SendMessageResponse = await super.sendMessage(request as unknown as Parameters<GatewayClient['sendMessage']>[0]);

        // Run all interceptors afterSend
        for (const interceptor of interceptors) {
            if (interceptor.afterSend) {
                response = await interceptor.afterSend(response) as SendMessageResponse;
            }
        }

        return response;
    }
}

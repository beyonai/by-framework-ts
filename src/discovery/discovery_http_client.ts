/**
 * HTTP client with service discovery and node-switching retries.
 *
 * Mirrors by_framework.util.discovery_http_client.DiscoveryHttpClient from the Python SDK.
 * Resolves service names to physical addresses dynamically via DiscoveryClient
 * and handles load balancing with automatic node-switching on failures.
 */

import { DiscoveryClient } from './discovery_client';
import { ServiceInstance } from './service_instance';
import { ByHttpClient, HttpResponse, RetryConfig, calculateDelay } from '../http_client';
import { DiscoveryHttpClientError, HttpClientError, HttpRequestError } from '../exceptions';

/**
 * HTTP client that integrates with Service Discovery.
 *
 * Resolves service names to physical addresses dynamically and handles load balancing.
 * Supports automatically switching to a different node upon request failures.
 *
 * @example
 * ```typescript
 * const discoveryClient = new DiscoveryClient(redis);
 * const client = new DiscoveryHttpClient({ discoveryClient });
 *
 * const response = await client.get('user-service', '/api/users/123');
 * if (response.isSuccess) {
 *     console.log(response.data);
 * }
 * ```
 */
export class DiscoveryHttpClient {
    public readonly discoveryClient: DiscoveryClient;
    public readonly httpClient: ByHttpClient;
    public readonly retryConfig: RetryConfig;
    public readonly ownsHttpClient: boolean;

    constructor(params: {
        discoveryClient: DiscoveryClient;
        httpClient?: ByHttpClient;
        retryConfig?: RetryConfig;
    }) {
        const { discoveryClient, httpClient, retryConfig } = params;
        this.discoveryClient = discoveryClient;
        // We enforce RetryConfig.noRetry() on the underlying ByHttpClient
        // if we create it, so retries stay discovery-aware.
        this.ownsHttpClient = httpClient === undefined;
        this.httpClient = httpClient ?? new ByHttpClient({ retryConfig: RetryConfig.noRetry() });
        this.retryConfig = retryConfig ?? new RetryConfig();
    }

    /**
     * Resolve a service instance and perform an HTTP request with retries.
     */
    private async requestWithDiscovery(
        method: string,
        serviceName: string,
        path: string,
        params?: {
            headers?: Record<string, string>;
            queryParams?: Record<string, string>;
            json?: Record<string, unknown>;
            data?: Record<string, unknown>;
            retryCount?: number;
            excludeInstanceIds?: Set<string>;
        }
    ): Promise<HttpResponse> {
        const {
            headers,
            queryParams,
            json,
            data,
            retryCount = 0,
            excludeInstanceIds: rawExcludeInstanceIds,
        } = params || {};
        const excludeInstanceIds = rawExcludeInstanceIds ?? new Set<string>();

        // 1. Discover a healthy instance
        const instance = await this.discoveryClient.discover(serviceName);
        if (!instance) {
            throw new DiscoveryHttpClientError(
                `No available instances for service: ${serviceName}`
            );
        }

        // 2. Construct the absolute URL
        const normalizedPath = path.replace(/^\/+/, '');
        const absoluteUrl = `http://${instance.host}:${instance.port}/${normalizedPath}`;
        const attempt = retryCount + 1;

        let lastError: Error | null = null;

        // 3. Perform the request
        try {
            const response = await this.httpClient.request(method, absoluteUrl, {
                headers,
                queryParams,
                json,
                data,
            });

            // If success or not a retryable status code, return directly
            if (
                response.isSuccess ||
                !this.retryConfig.retryOnStatusCodes.includes(response.statusCode)
            ) {
                return response;
            }

            console.warn(
                `[${method.toUpperCase()}] ${absoluteUrl} -> ${response.statusCode}, switching node and retrying...`
            );

        } catch (error: unknown) {
            if (error instanceof HttpRequestError) {
                // The internal client runs with noRetry(), so network
                // failures surface immediately and can trigger node switching.
                lastError = error;
                console.warn(
                    `[${method.toUpperCase()}] ${absoluteUrl} network error (attempt ${attempt}): ${error.message}`
                );
            } else if (error instanceof HttpClientError) {
                lastError = error;
                console.warn(
                    `[${method.toUpperCase()}] ${absoluteUrl} HTTP error (attempt ${attempt}): ${error.message}`
                );
            } else if (error instanceof Error) {
                lastError = error;
                console.warn(
                    `[${method.toUpperCase()}] ${absoluteUrl} error (attempt ${attempt}): ${error.message}`
                );
            } else {
                lastError = new Error(String(error));
                console.warn(
                    `[${method.toUpperCase()}] ${absoluteUrl} unknown error (attempt ${attempt}): ${error}`
                );
            }
        }

        // 4. Handle retry
        if (attempt < this.retryConfig.maxAttempts) {
            excludeInstanceIds.add(instance.id);
            const delay = calculateDelay(attempt, this.retryConfig);
            console.warn(
                `Node-switching retry in ${delay.toFixed(1)}s for service ${serviceName}`
            );
            await new Promise(resolve => setTimeout(resolve, delay * 1000));
            return this.requestWithDiscovery(method, serviceName, path, {
                headers,
                queryParams,
                json,
                data,
                retryCount: attempt,
                excludeInstanceIds,
            });
        }

        if (lastError) {
            throw new DiscoveryHttpClientError(
                `Service request failed after ${this.retryConfig.maxAttempts} attempts: ${lastError}`
            );
        }

        throw new DiscoveryHttpClientError(
            `Service request failed after ${this.retryConfig.maxAttempts} attempts.`
        );
    }

    /**
     * Download a file from a discovered service instance.
     */
    async download(
        serviceName: string,
        path: string,
        destination: string,
        params?: {
            headers?: Record<string, string>;
            queryParams?: Record<string, string>;
            retryCount?: number;
        }
    ): Promise<HttpResponse> {
        const { headers, queryParams, retryCount = 0 } = params || {};

        const instance = await this.discoveryClient.discover(serviceName);
        if (!instance) {
            throw new DiscoveryHttpClientError(
                `No available instances for service: ${serviceName}`
            );
        }

        const normalizedPath = path.replace(/^\/+/, '');
        const absoluteUrl = `http://${instance.host}:${instance.port}/${normalizedPath}`;
        const attempt = retryCount + 1;
        let lastError: Error | null = null;

        try {
            const response = await this.httpClient.download(absoluteUrl, destination, {
                headers,
                queryParams,
            });

            if (
                response.isSuccess ||
                !this.retryConfig.retryOnStatusCodes.includes(response.statusCode)
            ) {
                return response;
            }

            console.warn(
                `[DOWNLOAD] ${absoluteUrl} -> ${response.statusCode}, switching node and retrying...`
            );

        } catch (error: unknown) {
            if (error instanceof HttpRequestError) {
                lastError = error;
                console.warn(
                    `[DOWNLOAD] ${absoluteUrl} network error (attempt ${attempt}): ${error.message}`
                );
            } else if (error instanceof Error) {
                lastError = error;
                console.warn(
                    `[DOWNLOAD] ${absoluteUrl} error (attempt ${attempt}): ${error.message}`
                );
            } else {
                lastError = new Error(String(error));
            }
        }

        if (attempt < this.retryConfig.maxAttempts) {
            const delay = calculateDelay(attempt, this.retryConfig);
            console.warn(
                `Node-switching retry in ${delay.toFixed(1)}s for service ${serviceName}`
            );
            await new Promise(resolve => setTimeout(resolve, delay * 1000));
            return this.download(serviceName, path, destination, {
                headers,
                queryParams,
                retryCount: attempt,
            });
        }

        if (lastError) {
            throw new DiscoveryHttpClientError(
                `Service download failed after ${this.retryConfig.maxAttempts} attempts: ${lastError}`
            );
        }

        throw new DiscoveryHttpClientError(
            `Service download failed after ${this.retryConfig.maxAttempts} attempts.`
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // File Upload Methods (with service discovery)
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Upload a file using multipart/form-data with service discovery.
     */
    async upload(
        serviceName: string,
        path: string,
        filePath: string,
        params?: {
            headers?: Record<string, string>;
            formFields?: Record<string, string>;
            retryCount?: number;
        }
    ): Promise<HttpResponse> {
        return this.uploadMultiple(serviceName, path, [filePath], params);
    }

    /**
     * Upload multiple files using multipart/form-data with service discovery.
     */
    async uploadMultiple(
        serviceName: string,
        path: string,
        filePaths: string[],
        params?: {
            headers?: Record<string, string>;
            formFields?: Record<string, string>;
            retryCount?: number;
        }
    ): Promise<HttpResponse> {
        const { headers, formFields, retryCount = 0 } = params || {};

        const instance = await this.discoveryClient.discover(serviceName);
        if (!instance) {
            throw new DiscoveryHttpClientError(
                `No available instances for service: ${serviceName}`
            );
        }

        const normalizedPath = path.replace(/^\/+/, '');
        const absoluteUrl = `http://${instance.host}:${instance.port}/${normalizedPath}`;
        const attempt = retryCount + 1;
        let lastError: Error | null = null;

        try {
            const response = await this.httpClient.uploadMultiple(absoluteUrl, filePaths, {
                headers,
                formFields,
            });

            if (
                response.isSuccess ||
                !this.retryConfig.retryOnStatusCodes.includes(response.statusCode)
            ) {
                return response;
            }

            console.warn(
                `[UPLOAD] ${absoluteUrl} -> ${response.statusCode}, switching node and retrying...`
            );

        } catch (error: unknown) {
            if (error instanceof HttpRequestError) {
                lastError = error;
                console.warn(
                    `[UPLOAD] ${absoluteUrl} network error (attempt ${attempt}): ${error.message}`
                );
            } else if (error instanceof Error) {
                lastError = error;
                console.warn(
                    `[UPLOAD] ${absoluteUrl} error (attempt ${attempt}): ${error.message}`
                );
            } else {
                lastError = new Error(String(error));
            }
        }

        if (attempt < this.retryConfig.maxAttempts) {
            const delay = calculateDelay(attempt, this.retryConfig);
            console.warn(
                `Node-switching retry in ${delay.toFixed(1)}s for service ${serviceName}`
            );
            await new Promise(resolve => setTimeout(resolve, delay * 1000));
            return this.uploadMultiple(serviceName, path, filePaths, {
                headers,
                formFields,
                retryCount: attempt,
            });
        }

        if (lastError) {
            throw new DiscoveryHttpClientError(
                `Service upload failed after ${this.retryConfig.maxAttempts} attempts: ${lastError}`
            );
        }

        throw new DiscoveryHttpClientError(
            `Service upload failed after ${this.retryConfig.maxAttempts} attempts.`
        );
    }

    /**
     * Upload a file from bytes using multipart/form-data with service discovery.
     */
    async uploadWithStream(
        serviceName: string,
        path: string,
        fileName: string,
        content: Buffer,
        params?: {
            contentType?: string;
            headers?: Record<string, string>;
            formFields?: Record<string, string>;
            retryCount?: number;
        }
    ): Promise<HttpResponse> {
        const { contentType, headers, formFields, retryCount = 0 } = params || {};

        const instance = await this.discoveryClient.discover(serviceName);
        if (!instance) {
            throw new DiscoveryHttpClientError(
                `No available instances for service: ${serviceName}`
            );
        }

        const normalizedPath = path.replace(/^\/+/, '');
        const absoluteUrl = `http://${instance.host}:${instance.port}/${normalizedPath}`;
        const attempt = retryCount + 1;
        let lastError: Error | null = null;

        try {
            const response = await this.httpClient.uploadWithStream(absoluteUrl, fileName, content, {
                contentType,
                headers,
                formFields,
            });

            if (
                response.isSuccess ||
                !this.retryConfig.retryOnStatusCodes.includes(response.statusCode)
            ) {
                return response;
            }

            console.warn(
                `[UPLOAD] ${absoluteUrl} -> ${response.statusCode}, switching node and retrying...`
            );

        } catch (error: unknown) {
            if (error instanceof HttpRequestError) {
                lastError = error;
                console.warn(
                    `[UPLOAD] ${absoluteUrl} network error (attempt ${attempt}): ${error.message}`
                );
            } else if (error instanceof Error) {
                lastError = error;
                console.warn(
                    `[UPLOAD] ${absoluteUrl} error (attempt ${attempt}): ${error.message}`
                );
            } else {
                lastError = new Error(String(error));
            }
        }

        if (attempt < this.retryConfig.maxAttempts) {
            const delay = calculateDelay(attempt, this.retryConfig);
            console.warn(
                `Node-switching retry in ${delay.toFixed(1)}s for service ${serviceName}`
            );
            await new Promise(resolve => setTimeout(resolve, delay * 1000));
            return this.uploadWithStream(serviceName, path, fileName, content, {
                contentType,
                headers,
                formFields,
                retryCount: attempt,
            });
        }

        if (lastError) {
            throw new DiscoveryHttpClientError(
                `Service upload failed after ${this.retryConfig.maxAttempts} attempts: ${lastError}`
            );
        }

        throw new DiscoveryHttpClientError(
            `Service upload failed after ${this.retryConfig.maxAttempts} attempts.`
        );
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Convenience methods
    // ─────────────────────────────────────────────────────────────────────────

    async get(
        serviceName: string,
        path: string,
        params?: { headers?: Record<string, string>; queryParams?: Record<string, string> }
    ): Promise<HttpResponse> {
        return this.requestWithDiscovery('GET', serviceName, path, params);
    }

    async post(
        serviceName: string,
        path: string,
        params?: {
            headers?: Record<string, string>;
            json?: Record<string, unknown>;
            data?: Record<string, unknown>;
        }
    ): Promise<HttpResponse> {
        return this.requestWithDiscovery('POST', serviceName, path, params);
    }

    async put(
        serviceName: string,
        path: string,
        params?: {
            headers?: Record<string, string>;
            json?: Record<string, unknown>;
            data?: Record<string, unknown>;
        }
    ): Promise<HttpResponse> {
        return this.requestWithDiscovery('PUT', serviceName, path, params);
    }

    async patch(
        serviceName: string,
        path: string,
        params?: {
            headers?: Record<string, string>;
            json?: Record<string, unknown>;
            data?: Record<string, unknown>;
        }
    ): Promise<HttpResponse> {
        return this.requestWithDiscovery('PATCH', serviceName, path, params);
    }

    async delete(
        serviceName: string,
        path: string,
        params?: { headers?: Record<string, string>; queryParams?: Record<string, string> }
    ): Promise<HttpResponse> {
        return this.requestWithDiscovery('DELETE', serviceName, path, params);
    }
}
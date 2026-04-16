/**
 * HTTP client with retry, timeout, authentication, and error handling.
 *
 * Mirrors by_framework.util.http_client.ByHttpClient from the Python SDK,
 * using Node.js native fetch API for zero additional dependencies.
 */

import * as fs from 'fs';
import * as path from 'path';
import { HttpClientError, HttpRequestError } from './exceptions';

// ─────────────────────────────────────────────────────────────────────────────
// Authentication
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Abstract base class for authentication strategies.
 */
export abstract class Auth {
    /**
     * Apply authentication to the outgoing request headers.
     */
    abstract apply(headers: Record<string, string>): void;
}

/**
 * No authentication.
 */
export class NoAuth extends Auth {
    apply(_headers: Record<string, string>): void {
        // No-op
    }
}

/**
 * API key authentication (header or query param).
 */
export class ApiKeyAuth extends Auth {
    constructor(
        public readonly key: string,
        public readonly value: string,
        private readonly inHeader: boolean = true,
        private readonly prefix: string = ''
    ) {
        super();
    }

    apply(headers: Record<string, string>): void {
        if (this.inHeader) {
            const headerValue = this.prefix ? `${this.prefix} ${this.value}` : this.value;
            headers[this.key] = headerValue;
        }
        // Note: query param auth is handled separately in request construction
    }
}

/**
 * Bearer token authentication (JWT, OAuth2 tokens).
 */
export class BearerAuth extends Auth {
    constructor(public readonly token: string) {
        super();
    }

    apply(headers: Record<string, string>): void {
        headers['Authorization'] = `Bearer ${this.token}`;
    }
}

/**
 * Basic authentication (username/password).
 */
export class BasicAuth extends Auth {
    constructor(
        public readonly username: string,
        public readonly password: string
    ) {
        super();
    }

    apply(headers: Record<string, string>): void {
        const credentials = `${this.username}:${this.password}`;
        const encoded = Buffer.from(credentials).toString('base64');
        headers['Authorization'] = `Basic ${encoded}`;
    }
}

/**
 * Resolve auth parameter to an Auth instance.
 */
export function resolveAuth(auth: Auth | string | null | undefined): Auth {
    if (auth === null || auth === undefined) {
        return new NoAuth();
    }
    if (auth instanceof Auth) {
        return auth;
    }
    if (typeof auth === 'string') {
        return new BearerAuth(auth);
    }
    throw new TypeError(`Unsupported auth type: ${typeof auth}`);
}

// ─────────────────────────────────────────────────────────────────────────────
// Response & Configuration
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Wrapper for HTTP response with typed data.
 */
export class HttpResponse {
    constructor(
        public readonly statusCode: number,
        public readonly headers: Record<string, string>,
        public readonly data: unknown,
        public readonly isSuccess: boolean
    ) {}
}

/**
 * Configuration for retry behavior.
 */
export class RetryConfig {
    constructor(
        public readonly maxAttempts: number = 3,
        public readonly initialDelay: number = 0.5,
        public readonly maxDelay: number = 30.0,
        public readonly backoffMultiplier: number = 2.0,
        public readonly retryOnStatusCodes: number[] = [429, 500, 502, 503, 504]
    ) {}

    /**
     * Create a config that disables retries.
     */
    static noRetry(): RetryConfig {
        return new RetryConfig(1, 0.5, 30.0, 2.0, []);
    }
}

/**
 * Calculate delay for given attempt using exponential backoff.
 */
export function calculateDelay(attempt: number, config: RetryConfig): number {
    const delay = config.initialDelay * Math.pow(config.backoffMultiplier, attempt - 1);
    return Math.min(delay, config.maxDelay);
}

/**
 * Sleep for a given number of milliseconds.
 */
function sleep(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

// ─────────────────────────────────────────────────────────────────────────────
// ByHttpClient
// ─────────────────────────────────────────────────────────────────────────────

/**
 * HTTP client with automatic retry, timeout, and authentication support.
 *
 * Features:
 * - Configurable retry with exponential backoff
 * - Automatic timeout handling
 * - Structured error responses
 * - Request/response logging (via console.warn on failures)
 * - Pluggable authentication (API Key, Bearer, Basic)
 *
 * @example
 * ```typescript
 * const client = new ByHttpClient({ baseUrl: 'https://api.example.com' });
 * const response = await client.get('/users/123');
 * if (response.isSuccess) {
 *     console.log(response.data);
 * }
 * await client.close();
 * ```
 */
export class ByHttpClient {
    private readonly baseUrl: string;
    private readonly defaultHeaders: Record<string, string>;
    private readonly auth: Auth;
    private readonly timeout: number;
    private readonly retryConfig: RetryConfig;

    constructor(params: {
        baseUrl?: string;
        auth?: Auth | string | null;
        timeout?: number;
        headers?: Record<string, string>;
        retryConfig?: RetryConfig;
    }) {
        const { baseUrl = '', auth = null, timeout = 30.0, headers, retryConfig } = params;
        this.baseUrl = baseUrl.replace(/\/+$/, '');
        this.defaultHeaders = { ...(headers || {}) };
        this.auth = resolveAuth(auth);
        this.timeout = timeout;
        this.retryConfig = retryConfig ?? new RetryConfig();
    }

    /**
     * Execute HTTP request with retry logic.
     */
    async request(
        method: string,
        url: string,
        params?: {
            headers?: Record<string, string>;
            queryParams?: Record<string, string>;
            json?: Record<string, unknown>;
            data?: Record<string, unknown>;
            retryCount?: number;
        }
    ): Promise<HttpResponse> {
        const {
            headers: requestHeaders,
            queryParams,
            json,
            data,
            retryCount = 0,
        } = params || {};

        const attempt = retryCount + 1;
        const fullUrl = this.buildUrl(url, queryParams);
        const mergedHeaders = { ...this.defaultHeaders };
        this.auth.apply(mergedHeaders);
        if (requestHeaders) {
            Object.assign(mergedHeaders, requestHeaders);
        }

        // Set content-type for JSON body
        if (json && !mergedHeaders['Content-Type']) {
            mergedHeaders['Content-Type'] = 'application/json';
        }

        const body = json ? JSON.stringify(json) : data ? new URLSearchParams(data as Record<string, string>).toString() : undefined;
        if (data && !mergedHeaders['Content-Type']) {
            mergedHeaders['Content-Type'] = 'application/x-www-form-urlencoded';
        }

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout * 1000);

        let lastError: Error | null = null;

        try {
            const fetchOptions: RequestInit = {
                method,
                headers: mergedHeaders,
                signal: controller.signal,
            };
            if (body !== undefined) {
                fetchOptions.body = body;
            }

            const response = await fetch(fullUrl, fetchOptions);
            clearTimeout(timeoutId);

            const parsedResponse = await this.parseResponse(response);

            if (parsedResponse.isSuccess) {
                return parsedResponse;
            }

            // Check if we should retry on this status code
            if (
                this.retryConfig.retryOnStatusCodes.includes(parsedResponse.statusCode) &&
                attempt < this.retryConfig.maxAttempts
            ) {
                const delay = calculateDelay(attempt, this.retryConfig);
                console.warn(
                    `[${method.toUpperCase()}] ${fullUrl} -> ${parsedResponse.statusCode}, retrying in ${delay.toFixed(1)}s`
                );
                await sleep(delay * 1000);
                return this.request(method, url, {
                    headers: requestHeaders,
                    queryParams,
                    json,
                    data,
                    retryCount: attempt,
                });
            }

            return parsedResponse;

        } catch (error: unknown) {
            clearTimeout(timeoutId);

            if (error instanceof DOMException && error.name === 'AbortError') {
                lastError = new Error(`Request timeout after ${this.timeout}s`);
            } else if (error instanceof TypeError) {
                // Network error from fetch
                lastError = error as Error;
            } else if (error instanceof Error) {
                lastError = error;
            } else {
                lastError = new Error(String(error));
            }

            console.warn(
                `[${method.toUpperCase()}] ${fullUrl} network error (attempt ${attempt}): ${lastError.message}`
            );
        }

        // Retry on network errors
        if (attempt < this.retryConfig.maxAttempts) {
            const delay = calculateDelay(attempt, this.retryConfig);
            console.warn(
                `Retrying in ${delay.toFixed(1)}s after ${lastError!.constructor.name}`
            );
            await sleep(delay * 1000);
            return this.request(method, url, {
                headers: requestHeaders,
                queryParams,
                json,
                data,
                retryCount: attempt,
            });
        }

        throw new HttpRequestError(
            `Request failed after ${this.retryConfig.maxAttempts} attempts: ${lastError}`,
            fullUrl,
            lastError
        );
    }

    /**
     * Download a remote file to a local destination.
     */
    async download(
        url: string,
        destination: string,
        params?: {
            headers?: Record<string, string>;
            queryParams?: Record<string, string>;
            retryCount?: number;
        }
    ): Promise<HttpResponse> {
        const { headers: requestHeaders, queryParams, retryCount = 0 } = params || {};

        const attempt = retryCount + 1;
        const fullUrl = this.buildUrl(url, queryParams);
        const mergedHeaders = { ...this.defaultHeaders };
        this.auth.apply(mergedHeaders);
        if (requestHeaders) {
            Object.assign(mergedHeaders, requestHeaders);
        }

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout * 1000);

        let lastError: Error | null = null;

        try {
            const response = await fetch(fullUrl, {
                method: 'GET',
                headers: mergedHeaders,
                signal: controller.signal,
            });
            clearTimeout(timeoutId);

            if (response.ok) {
                const dir = path.dirname(destination);
                if (!fs.existsSync(dir)) {
                    fs.mkdirSync(dir, { recursive: true });
                }

                const arrayBuffer = await response.arrayBuffer();
                fs.writeFileSync(destination, Buffer.from(arrayBuffer));

                const responseHeaders = this.extractHeaders(response);

                return new HttpResponse(
                    response.status,
                    responseHeaders,
                    destination,
                    true
                );
            }

            // Non-success response
            const responseHeaders = this.extractHeaders(response);
            const textBody = await response.text();

            if (
                this.retryConfig.retryOnStatusCodes.includes(response.status) &&
                attempt < this.retryConfig.maxAttempts
            ) {
                const delay = calculateDelay(attempt, this.retryConfig);
                console.warn(
                    `[DOWNLOAD] ${fullUrl} -> ${response.status}, retrying download in ${delay.toFixed(1)}s`
                );
                await sleep(delay * 1000);
                return this.download(url, destination, {
                    headers: requestHeaders,
                    queryParams,
                    retryCount: attempt,
                });
            }

            return new HttpResponse(
                response.status,
                responseHeaders,
                textBody,
                false
            );

        } catch (error: unknown) {
            clearTimeout(timeoutId);

            if (error instanceof DOMException && error.name === 'AbortError') {
                lastError = new Error(`Request timeout after ${this.timeout}s`);
            } else if (error instanceof TypeError) {
                lastError = error as Error;
            } else if (error instanceof Error) {
                lastError = error;
            } else {
                lastError = new Error(String(error));
            }

            console.warn(
                `[DOWNLOAD] ${fullUrl} network error (attempt ${attempt}): ${lastError.message}`
            );
        }

        // Retry on network errors
        if (attempt < this.retryConfig.maxAttempts) {
            const delay = calculateDelay(attempt, this.retryConfig);
            console.warn(
                `Retrying download in ${delay.toFixed(1)}s after ${lastError!.constructor.name}`
            );
            await sleep(delay * 1000);
            return this.download(url, destination, {
                headers: requestHeaders,
                queryParams,
                retryCount: attempt,
            });
        }

        throw new HttpRequestError(
            `Download failed after ${this.retryConfig.maxAttempts} attempts: ${lastError}`,
            fullUrl,
            lastError
        );
    }

    /**
     * Build full URL from base URL, path, and query params.
     */
    private buildUrl(urlPath: string, queryParams?: Record<string, string>): string {
        const normalizedPath = urlPath.startsWith('http') ? urlPath : `${this.baseUrl}/${urlPath.replace(/^\/+/, '')}`;

        if (!queryParams || Object.keys(queryParams).length === 0) {
            return normalizedPath;
        }

        const url = new URL(normalizedPath);
        for (const [key, value] of Object.entries(queryParams)) {
            url.searchParams.append(key, value);
        }
        return url.toString();
    }

    /**
     * Parse fetch Response into HttpResponse.
     */
    private async parseResponse(response: Response): Promise<HttpResponse> {
        const headers = this.extractHeaders(response);
        const contentType = response.headers.get('content-type') || '';

        let data: unknown;
        if (contentType.includes('application/json')) {
            try {
                data = await response.json();
            } catch {
                data = await response.text();
            }
        } else {
            data = await response.text();
        }

        return new HttpResponse(
            response.status,
            headers,
            data,
            response.ok
        );
    }

    /**
     * Extract headers from a fetch Response into a plain object.
     */
    private extractHeaders(response: Response): Record<string, string> {
        const headers: Record<string, string> = {};
        response.headers.forEach((value, key) => {
            headers[key] = value;
        });
        return headers;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Convenience methods
    // ─────────────────────────────────────────────────────────────────────────

    async get(
        url: string,
        params?: { headers?: Record<string, string>; queryParams?: Record<string, string> }
    ): Promise<HttpResponse> {
        return this.request('GET', url, params);
    }

    async post(
        url: string,
        params?: {
            headers?: Record<string, string>;
            json?: Record<string, unknown>;
            data?: Record<string, unknown>;
        }
    ): Promise<HttpResponse> {
        return this.request('POST', url, params);
    }

    async put(
        url: string,
        params?: {
            headers?: Record<string, string>;
            json?: Record<string, unknown>;
            data?: Record<string, unknown>;
        }
    ): Promise<HttpResponse> {
        return this.request('PUT', url, params);
    }

    async patch(
        url: string,
        params?: {
            headers?: Record<string, string>;
            json?: Record<string, unknown>;
            data?: Record<string, unknown>;
        }
    ): Promise<HttpResponse> {
        return this.request('PATCH', url, params);
    }

    async delete(
        url: string,
        params?: { headers?: Record<string, string>; queryParams?: Record<string, string> }
    ): Promise<HttpResponse> {
        return this.request('DELETE', url, params);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // File Upload Methods
    // ─────────────────────────────────────────────────────────────────────────

    /**
     * Upload a file using multipart/form-data.
     */
    async upload(
        url: string,
        filePath: string,
        params?: {
            headers?: Record<string, string>;
            formFields?: Record<string, string>;
        }
    ): Promise<HttpResponse> {
        return this.uploadMultiple(url, [filePath], params);
    }

    /**
     * Upload multiple files using multipart/form-data.
     */
    async uploadMultiple(
        url: string,
        filePaths: string[],
        params?: {
            headers?: Record<string, string>;
            formFields?: Record<string, string>;
        }
    ): Promise<HttpResponse> {
        const { headers: requestHeaders, formFields } = params || {};

        const fullUrl = this.buildUrl(url);
        const mergedHeaders = { ...this.defaultHeaders };
        this.auth.apply(mergedHeaders);
        if (requestHeaders) {
            Object.assign(mergedHeaders, requestHeaders);
        }

        // Build multipart form data
        const formData = new FormData();
        if (formFields) {
            for (const [key, value] of Object.entries(formFields)) {
                formData.append(key, value);
            }
        }
        for (const filePath of filePaths) {
            const fileContent = fs.readFileSync(filePath);
            const fileName = path.basename(filePath);
            const blob = new Blob([fileContent]);
            formData.append('file', blob, fileName);
        }

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout * 1000);

        try {
            const response = await fetch(fullUrl, {
                method: 'POST',
                headers: mergedHeaders,
                body: formData,
                signal: controller.signal,
            });
            clearTimeout(timeoutId);

            return await this.parseResponse(response);

        } catch (error: unknown) {
            clearTimeout(timeoutId);

            let errorMessage: string;
            if (error instanceof DOMException && error.name === 'AbortError') {
                errorMessage = `Upload timeout after ${this.timeout}s`;
            } else if (error instanceof TypeError) {
                errorMessage = `Upload connection error: ${error.message}`;
            } else if (error instanceof Error) {
                errorMessage = `Upload error: ${error.message}`;
            } else {
                errorMessage = `Upload unknown error: ${String(error)}`;
            }

            console.warn(`[POST] ${fullUrl} error: ${errorMessage}`);
            throw new HttpRequestError(errorMessage, fullUrl, error instanceof Error ? error : new Error(String(error)));
        }
    }

    /**
     * Upload a file from bytes using multipart/form-data.
     */
    async uploadWithStream(
        url: string,
        fileName: string,
        content: Buffer,
        params?: {
            contentType?: string;
            headers?: Record<string, string>;
            formFields?: Record<string, string>;
        }
    ): Promise<HttpResponse> {
        const { contentType = 'application/octet-stream', headers: requestHeaders, formFields } = params || {};

        const fullUrl = this.buildUrl(url);
        const mergedHeaders = { ...this.defaultHeaders };
        this.auth.apply(mergedHeaders);
        if (requestHeaders) {
            Object.assign(mergedHeaders, requestHeaders);
        }
        // Let the browser set Content-Type with boundary for multipart
        delete mergedHeaders['Content-Type'];

        const formData = new FormData();
        if (formFields) {
            for (const [key, value] of Object.entries(formFields)) {
                formData.append(key, value);
            }
        }
        const blob = new Blob([content], { type: contentType });
        formData.append('file', blob, fileName);

        const controller = new AbortController();
        const timeoutId = setTimeout(() => controller.abort(), this.timeout * 1000);

        try {
            const response = await fetch(fullUrl, {
                method: 'POST',
                headers: mergedHeaders,
                body: formData,
                signal: controller.signal,
            });
            clearTimeout(timeoutId);

            return await this.parseResponse(response);

        } catch (error: unknown) {
            clearTimeout(timeoutId);

            let errorMessage: string;
            if (error instanceof DOMException && error.name === 'AbortError') {
                errorMessage = `Upload timeout after ${this.timeout}s`;
            } else if (error instanceof TypeError) {
                errorMessage = `Upload connection error: ${error.message}`;
            } else if (error instanceof Error) {
                errorMessage = `Upload error: ${error.message}`;
            } else {
                errorMessage = `Upload unknown error: ${String(error)}`;
            }

            console.warn(`[POST] ${fullUrl} error: ${errorMessage}`);
            throw new HttpRequestError(errorMessage, fullUrl, error instanceof Error ? error : new Error(String(error)));
        }
    }
}
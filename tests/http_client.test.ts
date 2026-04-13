/**
 * Unit tests for ByHttpClient, HttpResponse, RetryConfig, Auth classes.
 */

import {
    ByHttpClient,
    HttpResponse,
    RetryConfig,
    Auth,
    NoAuth,
    ApiKeyAuth,
    BearerAuth,
    BasicAuth,
    resolveAuth,
    calculateDelay,
} from '../src/http_client';
import { HttpRequestError, HttpClientError } from '../src/exceptions';

// ─────────────────────────────────────────────────────────────────────────────
// Mock fetch globally
// ─────────────────────────────────────────────────────────────────────────────

const mockFetch = jest.fn();
global.fetch = mockFetch;

// ─────────────────────────────────────────────────────────────────────────────
// HttpResponse
// ─────────────────────────────────────────────────────────────────────────────

describe('HttpResponse', () => {
    it('should create response with correct properties', () => {
        const response = new HttpResponse(200, { 'content-type': 'application/json' }, { id: 1 }, true);
        expect(response.statusCode).toBe(200);
        expect(response.headers).toEqual({ 'content-type': 'application/json' });
        expect(response.data).toEqual({ id: 1 });
        expect(response.isSuccess).toBe(true);
    });

    it('should mark non-2xx as not successful', () => {
        const response = new HttpResponse(404, {}, 'Not found', false);
        expect(response.isSuccess).toBe(false);
        expect(response.statusCode).toBe(404);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// RetryConfig
// ─────────────────────────────────────────────────────────────────────────────

describe('RetryConfig', () => {
    it('should have sensible defaults', () => {
        const config = new RetryConfig();
        expect(config.maxAttempts).toBe(3);
        expect(config.initialDelay).toBe(0.5);
        expect(config.maxDelay).toBe(30.0);
        expect(config.backoffMultiplier).toBe(2.0);
        expect(config.retryOnStatusCodes).toEqual([429, 500, 502, 503, 504]);
    });

    it('noRetry should disable retries', () => {
        const config = RetryConfig.noRetry();
        expect(config.maxAttempts).toBe(1);
        expect(config.retryOnStatusCodes).toEqual([]);
    });

    it('should allow custom configuration', () => {
        const config = new RetryConfig(5, 1.0, 60.0, 3.0, [500, 502]);
        expect(config.maxAttempts).toBe(5);
        expect(config.initialDelay).toBe(1.0);
        expect(config.maxDelay).toBe(60.0);
        expect(config.backoffMultiplier).toBe(3.0);
        expect(config.retryOnStatusCodes).toEqual([500, 502]);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// calculateDelay
// ─────────────────────────────────────────────────────────────────────────────

describe('calculateDelay', () => {
    it('should calculate exponential backoff', () => {
        const config = new RetryConfig(3, 0.5, 30.0, 2.0);
        expect(calculateDelay(1, config)).toBe(0.5);   // 0.5 * 2^0
        expect(calculateDelay(2, config)).toBe(1.0);   // 0.5 * 2^1
        expect(calculateDelay(3, config)).toBe(2.0);   // 0.5 * 2^2
    });

    it('should cap at maxDelay', () => {
        const config = new RetryConfig(10, 0.5, 2.0, 2.0);
        expect(calculateDelay(1, config)).toBe(0.5);
        expect(calculateDelay(2, config)).toBe(1.0);
        expect(calculateDelay(3, config)).toBe(2.0); // capped
        expect(calculateDelay(4, config)).toBe(2.0); // still capped
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// Auth classes
// ─────────────────────────────────────────────────────────────────────────────

describe('Auth classes', () => {
    describe('NoAuth', () => {
        it('should not modify headers', () => {
            const auth = new NoAuth();
            const headers: Record<string, string> = {};
            auth.apply(headers);
            expect(headers).toEqual({});
        });
    });

    describe('ApiKeyAuth', () => {
        it('should add API key to headers', () => {
            const auth = new ApiKeyAuth('X-API-Key', 'my-key');
            const headers: Record<string, string> = {};
            auth.apply(headers);
            expect(headers['X-API-Key']).toBe('my-key');
        });

        it('should add API key with prefix', () => {
            const auth = new ApiKeyAuth('Authorization', 'my-key', true, 'ApiKey');
            const headers: Record<string, string> = {};
            auth.apply(headers);
            expect(headers['Authorization']).toBe('ApiKey my-key');
        });

        it('should add API key without prefix', () => {
            const auth = new ApiKeyAuth('X-API-Key', 'my-key', true, '');
            const headers: Record<string, string> = {};
            auth.apply(headers);
            expect(headers['X-API-Key']).toBe('my-key');
        });
    });

    describe('BearerAuth', () => {
        it('should add Bearer token to headers', () => {
            const auth = new BearerAuth('my-jwt-token');
            const headers: Record<string, string> = {};
            auth.apply(headers);
            expect(headers['Authorization']).toBe('Bearer my-jwt-token');
        });
    });

    describe('BasicAuth', () => {
        it('should add Basic auth to headers', () => {
            const auth = new BasicAuth('user', 'pass');
            const headers: Record<string, string> = {};
            auth.apply(headers);
            expect(headers['Authorization']).toBe('Basic dXNlcjpwYXNz');
        });
    });

    describe('resolveAuth', () => {
        it('should return NoAuth for null/undefined', () => {
            expect(resolveAuth(null)).toBeInstanceOf(NoAuth);
            expect(resolveAuth(undefined)).toBeInstanceOf(NoAuth);
        });

        it('should return BearerAuth for string', () => {
            const auth = resolveAuth('my-token');
            expect(auth).toBeInstanceOf(BearerAuth);
            expect((auth as BearerAuth).token).toBe('my-token');
        });

        it('should return Auth instance as-is', () => {
            const original = new ApiKeyAuth('key', 'value');
            expect(resolveAuth(original)).toBe(original);
        });

        it('should throw TypeError for unsupported type', () => {
            expect(() => resolveAuth(123 as any)).toThrow(TypeError);
        });
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// ByHttpClient
// ─────────────────────────────────────────────────────────────────────────────

describe('ByHttpClient', () => {
    let client: ByHttpClient;

    beforeEach(() => {
        mockFetch.mockReset();
        client = new ByHttpClient({ baseUrl: 'https://api.example.com' });
    });

    describe('request', () => {
        it('should make a successful GET request', async () => {
            mockFetch.mockResolvedValueOnce({
                ok: true,
                status: 200,
                headers: new Headers({ 'content-type': 'application/json' }),
                json: async () => ({ id: 1 }),
                text: async () => '',
                arrayBuffer: async () => new ArrayBuffer(0),
            });

            const response = await client.get('/users/123');
            expect(response.isSuccess).toBe(true);
            expect(response.statusCode).toBe(200);
            expect(response.data).toEqual({ id: 1 });
        });

        it('should make a successful POST request with JSON body', async () => {
            mockFetch.mockResolvedValueOnce({
                ok: true,
                status: 201,
                headers: new Headers({ 'content-type': 'application/json' }),
                json: async () => ({ id: 2, name: 'test' }),
                text: async () => '',
                arrayBuffer: async () => new ArrayBuffer(0),
            });

            const response = await client.post('/users', {
                json: { name: 'test' },
            });
            expect(response.isSuccess).toBe(true);
            expect(response.statusCode).toBe(201);

            // Verify the request included JSON content-type
            const fetchCall = mockFetch.mock.calls[0];
            expect(fetchCall[1].headers['Content-Type']).toBe('application/json');
            expect(fetchCall[1].body).toBe(JSON.stringify({ name: 'test' }));
        });

        it('should return non-success response for 4xx status codes', async () => {
            mockFetch.mockResolvedValueOnce({
                ok: false,
                status: 404,
                headers: new Headers({ 'content-type': 'text/plain' }),
                json: async () => { throw new Error('not json'); },
                text: async () => 'Not found',
                arrayBuffer: async () => new ArrayBuffer(0),
            });

            const response = await client.get('/nonexistent');
            expect(response.isSuccess).toBe(false);
            expect(response.statusCode).toBe(404);
            expect(response.data).toBe('Not found');
        });

        it('should retry on retryable status codes', async () => {
            // First call: 503 (retryable)
            mockFetch.mockResolvedValueOnce({
                ok: false,
                status: 503,
                headers: new Headers({ 'content-type': 'text/plain' }),
                json: async () => { throw new Error(); },
                text: async () => 'Service Unavailable',
                arrayBuffer: async () => new ArrayBuffer(0),
            });

            // Second call: 200 (success)
            mockFetch.mockResolvedValueOnce({
                ok: true,
                status: 200,
                headers: new Headers({ 'content-type': 'application/json' }),
                json: async () => ({ ok: true }),
                text: async () => '',
                arrayBuffer: async () => new ArrayBuffer(0),
            });

            const retryConfig = new RetryConfig(3, 0.01, 1.0, 2.0, [503]);
            const clientWithRetry = new ByHttpClient({
                baseUrl: 'https://api.example.com',
                retryConfig,
            });

            const response = await clientWithRetry.get('/flaky');
            expect(response.isSuccess).toBe(true);
            expect(mockFetch).toHaveBeenCalledTimes(2);
        });

        it('should throw HttpRequestError after exhausting retries on network errors', async () => {
            mockFetch.mockRejectedValue(new TypeError('Network error'));

            const retryConfig = new RetryConfig(2, 0.01, 1.0, 2.0);
            const clientWithRetry = new ByHttpClient({
                baseUrl: 'https://api.example.com',
                retryConfig,
            });

            await expect(clientWithRetry.get('/fail')).rejects.toThrow(HttpRequestError);
        });

        it('should not retry on non-retryable status codes', async () => {
            mockFetch.mockResolvedValueOnce({
                ok: false,
                status: 404,
                headers: new Headers({ 'content-type': 'text/plain' }),
                json: async () => { throw new Error(); },
                text: async () => 'Not found',
                arrayBuffer: async () => new ArrayBuffer(0),
            });

            const response = await client.get('/notfound');
            expect(response.isSuccess).toBe(false);
            expect(response.statusCode).toBe(404);
            expect(mockFetch).toHaveBeenCalledTimes(1);
        });

        it('should apply auth headers', async () => {
            mockFetch.mockResolvedValueOnce({
                ok: true,
                status: 200,
                headers: new Headers({ 'content-type': 'application/json' }),
                json: async () => ({}),
                text: async () => '',
                arrayBuffer: async () => new ArrayBuffer(0),
            });

            const authClient = new ByHttpClient({
                baseUrl: 'https://api.example.com',
                auth: new BearerAuth('my-token'),
            });
            await authClient.get('/protected');

            const fetchCall = mockFetch.mock.calls[0];
            expect(fetchCall[1].headers['Authorization']).toBe('Bearer my-token');
        });

        it('should merge custom headers with default headers', async () => {
            mockFetch.mockResolvedValueOnce({
                ok: true,
                status: 200,
                headers: new Headers({ 'content-type': 'application/json' }),
                json: async () => ({}),
                text: async () => '',
                arrayBuffer: async () => new ArrayBuffer(0),
            });

            const clientWithHeaders = new ByHttpClient({
                baseUrl: 'https://api.example.com',
                headers: { 'X-Custom': 'value' },
            });
            await clientWithHeaders.get('/test', { headers: { 'X-Request': 'req' } });

            const fetchCall = mockFetch.mock.calls[0];
            expect(fetchCall[1].headers['X-Custom']).toBe('value');
            expect(fetchCall[1].headers['X-Request']).toBe('req');
        });

        it('should build URL with query params', async () => {
            mockFetch.mockResolvedValueOnce({
                ok: true,
                status: 200,
                headers: new Headers({ 'content-type': 'application/json' }),
                json: async () => ({}),
                text: async () => '',
                arrayBuffer: async () => new ArrayBuffer(0),
            });

            await client.get('/search', { queryParams: { q: 'test', page: '1' } });

            const fetchCall = mockFetch.mock.calls[0];
            const url = fetchCall[0] as string;
            expect(url).toContain('q=test');
            expect(url).toContain('page=1');
        });
    });

    describe('convenience methods', () => {
        const mockResponse = {
            ok: true,
            status: 200,
            headers: new Headers({ 'content-type': 'application/json' }),
            json: async () => ({ success: true }),
            text: async () => '',
            arrayBuffer: async () => new ArrayBuffer(0),
        };

        it('should send PUT request', async () => {
            mockFetch.mockResolvedValueOnce(mockResponse);
            await client.put('/users/1', { json: { name: 'updated' } });
            const fetchCall = mockFetch.mock.calls[0];
            expect(fetchCall[1].method).toBe('PUT');
        });

        it('should send PATCH request', async () => {
            mockFetch.mockResolvedValueOnce(mockResponse);
            await client.patch('/users/1', { json: { name: 'patched' } });
            const fetchCall = mockFetch.mock.calls[0];
            expect(fetchCall[1].method).toBe('PATCH');
        });

        it('should send DELETE request', async () => {
            mockFetch.mockResolvedValueOnce(mockResponse);
            await client.delete('/users/1');
            const fetchCall = mockFetch.mock.calls[0];
            expect(fetchCall[1].method).toBe('DELETE');
        });
    });
});
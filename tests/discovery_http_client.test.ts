/**
 * Unit tests for DiscoveryHttpClient.
 */

import { DiscoveryHttpClient } from '../src/discovery/discovery_http_client';
import { DiscoveryClient } from '../src/discovery/discovery_client';
import { ServiceInstance } from '../src/discovery/service_instance';
import { ByHttpClient, HttpResponse, RetryConfig } from '../src/http_client';
import { DiscoveryHttpClientError, HttpClientError, HttpRequestError } from '../src/exceptions';

// ─────────────────────────────────────────────────────────────────────────────
// Mock fetch globally
// ─────────────────────────────────────────────────────────────────────────────

const mockFetch = jest.fn();
global.fetch = mockFetch;

// ─────────────────────────────────────────────────────────────────────────────
// Helper to create mock DiscoveryClient
// ─────────────────────────────────────────────────────────────────────────────

function createMockDiscoveryClient(instance: ServiceInstance | null): DiscoveryClient {
    return {
        discover: jest.fn().mockResolvedValue(instance),
        getInstances: jest.fn(),
        watch: jest.fn(),
        unwatch: jest.fn(),
        close: jest.fn(),
        getCacheStats: jest.fn(),
    } as unknown as DiscoveryClient;
}

function createSuccessResponse(data: unknown, statusCode = 200): HttpResponse {
    return new HttpResponse(statusCode, { 'content-type': 'application/json' }, data, 200 <= statusCode && statusCode < 300);
}

// ─────────────────────────────────────────────────────────────────────────────
// DiscoveryHttpClientError
// ─────────────────────────────────────────────────────────────────────────────

describe('DiscoveryHttpClientError', () => {
    it('should create error with message', () => {
        const error = new DiscoveryHttpClientError('No instances found');
        expect(error.message).toBe('No instances found');
        expect(error.name).toBe('DiscoveryHttpClientError');
        expect(error).toBeInstanceOf(HttpClientError);
    });
});

// ─────────────────────────────────────────────────────────────────────────────
// DiscoveryHttpClient
// ─────────────────────────────────────────────────────────────────────────────

describe('DiscoveryHttpClient', () => {
    let mockDiscoveryClient: DiscoveryClient;
    let instance: ServiceInstance;

    beforeEach(() => {
        mockFetch.mockReset();
        instance = new ServiceInstance('user-service:abc123', '10.0.0.1', 8080, 1, {});

        mockDiscoveryClient = createMockDiscoveryClient(instance);
    });

    describe('constructor', () => {
        it('should create internal ByHttpClient with noRetry if none provided', () => {
            const client = new DiscoveryHttpClient({ discoveryClient: mockDiscoveryClient });
            expect(client.ownsHttpClient).toBe(true);
            expect(client.retryConfig).toBeInstanceOf(RetryConfig);
        });

        it('should accept custom httpClient and retryConfig', () => {
            const httpClient = new ByHttpClient({ baseUrl: '' });
            const retryConfig = new RetryConfig(5, 1.0, 30.0, 2.0, [500]);
            const client = new DiscoveryHttpClient({
                discoveryClient: mockDiscoveryClient,
                httpClient,
                retryConfig,
            });
            expect(client.ownsHttpClient).toBe(false);
            expect(client.httpClient).toBe(httpClient);
            expect(client.retryConfig).toBe(retryConfig);
        });
    });

    describe('GET', () => {
        it('should discover service and make GET request', async () => {
            mockFetch.mockResolvedValueOnce({
                ok: true,
                status: 200,
                headers: new Headers({ 'content-type': 'application/json' }),
                json: async () => ({ id: 1, name: 'user' }),
                text: async () => '',
                arrayBuffer: async () => new ArrayBuffer(0),
            });

            const client = new DiscoveryHttpClient({ discoveryClient: mockDiscoveryClient });
            const response = await client.get('user-service', '/api/users/1');

            expect(response.isSuccess).toBe(true);
            expect(response.data).toEqual({ id: 1, name: 'user' });
            expect(mockDiscoveryClient.discover).toHaveBeenCalledWith('user-service');

            const fetchCall = mockFetch.mock.calls[0];
            expect(fetchCall[0]).toBe('http://10.0.0.1:8080/api/users/1');
            expect(fetchCall[1].method).toBe('GET');
        });

        it('should throw DiscoveryHttpClientError when no instances found', async () => {
            const emptyDiscoveryClient = createMockDiscoveryClient(null);
            const client = new DiscoveryHttpClient({ discoveryClient: emptyDiscoveryClient });

            await expect(client.get('missing-service', '/path')).rejects.toThrow(DiscoveryHttpClientError);
            await expect(client.get('missing-service', '/path')).rejects.toThrow('No available instances for service: missing-service');
        });

        it('should strip leading slashes from path', async () => {
            mockFetch.mockResolvedValueOnce({
                ok: true,
                status: 200,
                headers: new Headers({ 'content-type': 'application/json' }),
                json: async () => ({}),
                text: async () => '',
                arrayBuffer: async () => new ArrayBuffer(0),
            });

            const client = new DiscoveryHttpClient({ discoveryClient: mockDiscoveryClient });
            await client.get('user-service', '///api/users/1');

            const fetchCall = mockFetch.mock.calls[0];
            expect(fetchCall[0]).toBe('http://10.0.0.1:8080/api/users/1');
        });
    });

    describe('POST', () => {
        it('should send POST request with JSON body', async () => {
            mockFetch.mockResolvedValueOnce({
                ok: true,
                status: 201,
                headers: new Headers({ 'content-type': 'application/json' }),
                json: async () => ({ id: 2 }),
                text: async () => '',
                arrayBuffer: async () => new ArrayBuffer(0),
            });

            const client = new DiscoveryHttpClient({ discoveryClient: mockDiscoveryClient });
            const response = await client.post('user-service', '/api/users', {
                json: { name: 'new_user' },
            });

            expect(response.isSuccess).toBe(true);
            expect(response.statusCode).toBe(201);

            const fetchCall = mockFetch.mock.calls[0];
            expect(fetchCall[1].method).toBe('POST');
            expect(fetchCall[1].body).toBe(JSON.stringify({ name: 'new_user' }));
        });
    });

    describe('node-switching retry', () => {
        it('should retry on retryable status codes by rediscovering', async () => {
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
            const client = new DiscoveryHttpClient({
                discoveryClient: mockDiscoveryClient,
                retryConfig,
            });

            const response = await client.get('user-service', '/api/users');
            expect(response.isSuccess).toBe(true);
            expect(mockDiscoveryClient.discover).toHaveBeenCalledTimes(2);
        });

        it('should retry on network errors', async () => {
            // First call: network error
            mockFetch.mockRejectedValueOnce(new TypeError('Network error'));

            // Second call: success
            mockFetch.mockResolvedValueOnce({
                ok: true,
                status: 200,
                headers: new Headers({ 'content-type': 'application/json' }),
                json: async () => ({ ok: true }),
                text: async () => '',
                arrayBuffer: async () => new ArrayBuffer(0),
            });

            const retryConfig = new RetryConfig(3, 0.01, 1.0, 2.0);
            const client = new DiscoveryHttpClient({
                discoveryClient: mockDiscoveryClient,
                retryConfig,
            });

            const response = await client.get('user-service', '/api/users');
            expect(response.isSuccess).toBe(true);
        });

        it('should throw DiscoveryHttpClientError after exhausting retries', async () => {
            mockFetch.mockRejectedValue(new TypeError('Network error'));

            const retryConfig = new RetryConfig(2, 0.01, 1.0, 2.0);
            const client = new DiscoveryHttpClient({
                discoveryClient: mockDiscoveryClient,
                retryConfig,
            });

            await expect(client.get('user-service', '/api/users')).rejects.toThrow(DiscoveryHttpClientError);
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

            const client = new DiscoveryHttpClient({ discoveryClient: mockDiscoveryClient });
            const response = await client.get('user-service', '/api/missing');

            expect(response.isSuccess).toBe(false);
            expect(response.statusCode).toBe(404);
            expect(mockFetch).toHaveBeenCalledTimes(1);
        });
    });

    describe('PUT, PATCH, DELETE', () => {
        const mockResponse = {
            ok: true,
            status: 200,
            headers: new Headers({ 'content-type': 'application/json' }),
            json: async () => ({}),
            text: async () => '',
            arrayBuffer: async () => new ArrayBuffer(0),
        };

        it('should send PUT request', async () => {
            mockFetch.mockResolvedValueOnce(mockResponse);
            const client = new DiscoveryHttpClient({ discoveryClient: mockDiscoveryClient });
            await client.put('user-service', '/api/users/1', { json: { name: 'updated' } });
            expect(mockFetch.mock.calls[0][1].method).toBe('PUT');
        });

        it('should send PATCH request', async () => {
            mockFetch.mockResolvedValueOnce(mockResponse);
            const client = new DiscoveryHttpClient({ discoveryClient: mockDiscoveryClient });
            await client.patch('user-service', '/api/users/1', { json: { name: 'patched' } });
            expect(mockFetch.mock.calls[0][1].method).toBe('PATCH');
        });

        it('should send DELETE request', async () => {
            mockFetch.mockResolvedValueOnce(mockResponse);
            const client = new DiscoveryHttpClient({ discoveryClient: mockDiscoveryClient });
            await client.delete('user-service', '/api/users/1');
            expect(mockFetch.mock.calls[0][1].method).toBe('DELETE');
        });
    });
});
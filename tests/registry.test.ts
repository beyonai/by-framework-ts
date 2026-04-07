import { WorkerRegistry } from '../src/registry';

class MockRedis {
    private zsets = new Map<string, Map<string, number>>();
    private sets = new Map<string, Set<string>>();
    private strings = new Map<string, string>();
    private hashes = new Map<string, Record<string, string>>();

    async zadd(key: string, score: string, member: string): Promise<number> {
        if (!this.zsets.has(key)) {
            this.zsets.set(key, new Map());
        }
        this.zsets.get(key)!.set(member, Number(score));
        return 1;
    }

    async zscore(key: string, member: string): Promise<string | null> {
        const score = this.zsets.get(key)?.get(member);
        return score === undefined ? null : String(score);
    }

    async zrange(key: string, _start: number, _end: number): Promise<string[]> {
        return [...(this.zsets.get(key)?.keys() || [])];
    }

    async zrem(key: string, member: string): Promise<number> {
        return this.zsets.get(key)?.delete(member) ? 1 : 0;
    }

    async sadd(key: string, member: string): Promise<number> {
        if (!this.sets.has(key)) {
            this.sets.set(key, new Set());
        }
        this.sets.get(key)!.add(member);
        return 1;
    }

    async smembers(key: string): Promise<string[]> {
        return [...(this.sets.get(key) || [])];
    }

    async srem(key: string, member: string): Promise<number> {
        return this.sets.get(key)?.delete(member) ? 1 : 0;
    }

    async del(key: string): Promise<number> {
        let deleted = 0;
        if (this.sets.delete(key)) deleted += 1;
        if (this.strings.delete(key)) deleted += 1;
        if (this.zsets.delete(key)) deleted += 1;
        if (this.hashes.delete(key)) deleted += 1;
        return deleted;
    }

    async set(key: string, value: string, ...args: any[]): Promise<'OK' | null> {
        const useNx = args.includes('NX');
        if (useNx && this.strings.has(key)) {
            return null;
        }
        this.strings.set(key, value);
        return 'OK';
    }

    async get(key: string): Promise<string | null> {
        return this.strings.get(key) ?? null;
    }

    async expire(_key: string, _seconds: number): Promise<number> {
        return 1;
    }

    async hset(key: string, field: string | Record<string, string>, value?: string): Promise<number> {
        if (!this.hashes.has(key)) {
            this.hashes.set(key, {});
        }
        const hash = this.hashes.get(key)!;
        if (typeof field === 'string') {
            hash[field] = value!;
            return 1;
        } else {
            Object.assign(hash, field);
            return Object.keys(field).length;
        }
    }

    async hget(key: string, field: string): Promise<string | null> {
        return this.hashes.get(key)?.[field] ?? null;
    }

    async hgetall(key: string): Promise<Record<string, string>> {
        return { ...(this.hashes.get(key) || {}) };
    }

    pipeline() {
        const self = this;
        const commands: any[] = [];
        const pipe = {
            hset: (key: string, field: string, value: string) => {
                commands.push(() => self.hset(key, field, value));
                return pipe;
            },
            expire: (key: string, seconds: number) => {
                commands.push(() => self.expire(key, seconds));
                return pipe;
            },
            exec: async () => {
                for (const cmd of commands) await cmd();
                return [];
            }
        };
        return pipe;
    }
}

describe('WorkerRegistry', () => {
    let redis: MockRedis;
    let registry: WorkerRegistry;

    beforeEach(() => {
        redis = new MockRedis();
        registry = new WorkerRegistry(redis as any);
    });

    test('should register a worker and retrieve it', async () => {
        const workerId = 'test-worker-01';
        const agentTypes = ['agent-type-1', 'agent-type-2'];

        await registry.registerWorker(workerId, agentTypes);

        const worker = await registry.getWorker(workerId);
        expect(worker).not.toBeNull();
        expect(worker.agentTypes).toEqual(agentTypes);
        expect(worker.last_seen).toBeLessThanOrEqual(Date.now());
    });

    test('should return null for non-existent worker', async () => {
        const worker = await registry.getWorker('non-existent');
        expect(worker).toBeNull();
    });

    test('should claim, refresh and release worker id lock', async () => {
        const workerId = 'lock-test-worker';

        const token = await registry.claimWorkerId(workerId, 10);
        expect(token).toBeDefined();

        await expect(registry.claimWorkerId(workerId, 10)).rejects.toThrow('already in use');

        const refreshed = await registry.refreshWorkerIdLock(workerId, 20);
        expect(refreshed).toBe(true);

        const released = await registry.releaseWorkerId(workerId, token);
        expect(released).toBe(true);

        const newToken = await registry.claimWorkerId(workerId, 10);
        expect(newToken).toBeDefined();
        expect(newToken).not.toBe(token);
    });

    test('getTargetWorker returns random worker for agentType', async () => {
        await registry.registerWorker('w-1', ['agent-x']);
        await registry.registerWorker('w-2', ['agent-x']);

        const result = await registry.getTargetWorker('agent-x');
        expect(['w-1', 'w-2']).toContain(result);
    });

    test('getTargetWorker returns null when no worker available', async () => {
        const result = await registry.getTargetWorker('nonexistent-agent');
        expect(result).toBeNull();
    });

    test('unregisterWorker removes worker and agentTypes', async () => {
        await registry.registerWorker('w-1', ['agent-a', 'agent-b']);

        await registry.unregisterWorker('w-1');

        const worker = await registry.getWorker('w-1');
        expect(worker).toBeNull();
    });

    test('getAllWorkers returns registered workers', async () => {
        await registry.registerWorker('w-1', ['cap-a']);
        await registry.registerWorker('w-2', ['cap-b']);

        const all = await registry.getAllWorkers();
        expect(Object.keys(all)).toHaveLength(2);
        expect(all['w-1']).toBeDefined();
        expect(all['w-2']).toBeDefined();
    });

    // --- Execution lifecycle tests (对标 Python test_registry.py) ---
    test('saveExecution and getExecution round trip', async () => {
        await registry.saveExecution({
            execution_id: 'exec-1',
            message_id: 'msg-1',
            session_id: 'sess-1',
            worker_id: 'worker-1',
            status: 'RUNNING',
            cancel_requested: false,
        });

        const exec = await registry.getExecution('exec-1', 'sess-1');
        expect(exec).not.toBeNull();
        expect(exec!.execution_id).toBe('exec-1');
        expect(exec!.worker_id).toBe('worker-1');
        expect(exec!.status).toBe('RUNNING');
        expect(exec!.cancel_requested).toBe(false);
    });

    test('getExecution returns null for nonexistent', async () => {
        const exec = await registry.getExecution('nonexistent', 'sess-1');
        expect(exec).toBeNull();
    });

    test('getExecutionByMessageId looks up execution', async () => {
        await registry.saveExecution({
            execution_id: 'exec-2',
            message_id: 'msg-2',
            session_id: 'sess-2',
            worker_id: 'worker-2',
            status: 'RUNNING',
        });

        const exec = await registry.getExecutionByMessageId('msg-2', 'sess-2');
        expect(exec).not.toBeNull();
        expect(exec!.execution_id).toBe('exec-2');
    });

    test('getExecutionByMessageId returns null when no mapping', async () => {
        const exec = await registry.getExecutionByMessageId('missing-msg', 'sess-2');
        expect(exec).toBeNull();
    });

    test('markExecutionCancelling sets status and cancel_requested', async () => {
        await registry.saveExecution({
            execution_id: 'exec-3',
            message_id: 'msg-3',
            session_id: 'sess-3',
            worker_id: 'worker-3',
            status: 'RUNNING',
            cancel_requested: false,
        });

        await registry.markExecutionCancelling('exec-3', 'sess-3', 'user requested');

        const exec = await registry.getExecution('exec-3', 'sess-3');
        expect(exec!.status).toBe('CANCELLING');
        expect(exec!.cancel_requested).toBe(true);
        expect(exec!.cancel_reason).toBe('user requested');
    });

    test('markExecutionCancelling is noop when not found', async () => {
        await expect(registry.markExecutionCancelling('nonexistent', 'sess-1', 'reason')).resolves.not.toThrow();
    });

    test('markExecutionFinished sets status and timestamps', async () => {
        await registry.saveExecution({
            execution_id: 'exec-4',
            message_id: 'msg-4',
            session_id: 'sess-4',
            worker_id: 'worker-4',
            status: 'RUNNING',
        });

        await registry.markExecutionFinished('exec-4', 'sess-4', 'COMPLETED');

        const exec = await registry.getExecution('exec-4', 'sess-4');
        expect(exec!.status).toBe('COMPLETED');
        expect(exec!.finished_at).toBeDefined();
        expect(exec!.updated_at).toBeDefined();
    });

    test('markExecutionFinished is noop when not found', async () => {
        await expect(registry.markExecutionFinished('nonexistent', 'sess-1', 'COMPLETED')).resolves.not.toThrow();
    });

    test('full execution lifecycle: save, query, cancel, finish', async () => {
        // Save
        await registry.saveExecution({
            execution_id: 'exec-life',
            message_id: 'msg-life',
            session_id: 'sess-life',
            worker_id: 'worker-life',
            status: 'RUNNING',
            cancel_requested: false,
        });

        // Query by message_id
        const fetched = await registry.getExecutionByMessageId('msg-life', 'sess-life');
        expect(fetched).not.toBeNull();
        expect(fetched!.status).toBe('RUNNING');

        // Mark cancelling
        await registry.markExecutionCancelling('exec-life', 'sess-life', 'timeout');
        const cancelling = await registry.getExecution('exec-life', 'sess-life');
        expect(cancelling!.status).toBe('CANCELLING');
        expect(cancelling!.cancel_requested).toBe(true);

        // Mark finished
        await registry.markExecutionFinished('exec-life', 'sess-life', 'CANCELLED');
        const finished = await registry.getExecution('exec-life', 'sess-life');
        expect(finished!.status).toBe('CANCELLED');
        expect(finished!.finished_at).toBeDefined();
    });

    test('boolean fields are decoded correctly', async () => {
        await registry.saveExecution({
            execution_id: 'exec-bool',
            message_id: 'msg-bool',
            session_id: 'sess-bool',
            worker_id: 'worker-bool',
            status: 'RUNNING',
            cancel_requested: true,
        });

        const exec = await registry.getExecution('exec-bool', 'sess-bool');
        expect(exec!.cancel_requested).toBe(true);
    });
});


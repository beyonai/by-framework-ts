import { createWorkerRunner } from '../src/factory';

jest.mock('../src/worker', () => ({
    AnonymousWorker: jest.fn(),
}));

jest.mock('../src/runner', () => ({
    WorkerRunner: jest.fn(),
}));

jest.mock('../src/registry', () => ({
    WorkerRegistry: jest.fn(),
}));

const { AnonymousWorker } = jest.requireMock('../src/worker') as { AnonymousWorker: jest.Mock };
const { WorkerRunner } = jest.requireMock('../src/runner') as { WorkerRunner: jest.Mock };
const { WorkerRegistry } = jest.requireMock('../src/registry') as { WorkerRegistry: jest.Mock };

describe('createWorkerRunner', () => {
    beforeEach(() => {
        jest.clearAllMocks();
        AnonymousWorker.mockImplementation((options: any) => ({
            __kind: 'worker',
            ...options,
        }));
        WorkerRunner.mockImplementation((worker: any, options: any) => ({
            __kind: 'runner',
            worker,
            options,
        }));
        WorkerRegistry.mockImplementation((redis: any) => ({
            __kind: 'registry',
            redis,
        }));
    });

    it('uses duplicated redis connection for runner when redisClient is provided', () => {
        const duplicatedRedis = { name: 'runner-redis' };
        const redisClient = {
            duplicate: jest.fn().mockReturnValue(duplicatedRedis),
        } as any;

        const runner = createWorkerRunner({
            workerId: 'worker-1',
            capabilities: ['cap-a'],
            onTask: async () => ({}),
            redisClient,
        });

        expect(redisClient.duplicate).toHaveBeenCalledTimes(1);
        expect(AnonymousWorker).toHaveBeenCalledWith(expect.objectContaining({
            workerId: 'worker-1',
            redisClient,
        }));
        expect(WorkerRunner).toHaveBeenCalledWith(
            expect.anything(),
            expect.objectContaining({ redisClient: duplicatedRedis })
        );
        expect((runner as any).__kind).toBe('runner');
    });

    it('falls back to original redis when duplicate is unavailable', () => {
        const redisClient = {} as any;

        createWorkerRunner({
            workerId: 'worker-1',
            capabilities: ['cap-a'],
            onTask: async () => ({}),
            redisClient,
        });

        expect(WorkerRunner).toHaveBeenCalledWith(
            expect.anything(),
            expect.objectContaining({ redisClient })
        );
    });
});

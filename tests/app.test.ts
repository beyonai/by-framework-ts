import { runWorker } from '../src/app';
import { GatewayWorker } from '../src/worker';
import { GatewayCommand } from '../src/protocol/commands';
import { AgentContext } from '../src/context';
import { Plugin, PluginBuildContext } from '../src/extensions/plugin';
import { BaseHistoryStorage } from '../src/history';

jest.mock('../src/runner', () => ({
    WorkerRunner: jest.fn().mockImplementation((_worker: any, _options: any) => ({
        start: jest.fn().mockResolvedValue(undefined),
    })),
}));

jest.mock('../src/redis_client', () => ({
    initRedis: jest.fn().mockReturnValue({}),
    closeRedis: jest.fn().mockResolvedValue(undefined),
}));

const { WorkerRunner } = jest.requireMock('../src/runner') as { WorkerRunner: jest.Mock };
const { initRedis, closeRedis } = jest.requireMock('../src/redis_client') as {
    initRedis: jest.Mock;
    closeRedis: jest.Mock;
};

class DummyWorker extends GatewayWorker {
    getAgentTypes(): string[] {
        return ['dummy-agent'];
    }

    async processCommand(_command: GatewayCommand, _context: AgentContext): Promise<any> {
        return { ok: true };
    }
}

class DummyPlugin extends Plugin {
    constructor(id: string) {
        super({ plugin_id: id });
    }

    async registerAgentConfigs(_buildContext: PluginBuildContext) {
        return null;
    }
}

class DummyHistoryStorage implements BaseHistoryStorage {
    async saveMessage(
        _sessionId: string,
        _role: string,
        _content: string | Record<string, any>,
        _metadata?: Record<string, any>
    ): Promise<void> {}

    async getSessionHistory(_sessionId: string, _limit?: number) {
        return [];
    }
}

describe('runWorker', () => {
    const envBackup = { ...process.env };

    beforeEach(() => {
        jest.clearAllMocks();
        process.env = { ...envBackup };
    });

    afterAll(() => {
        process.env = envBackup;
    });

    it('applies env defaults for concurrency/fetch and wires worker/runner', async () => {
        process.env.BYAI_WORKER_CONCURRENCY = '77';
        process.env.BYAI_WORKER_FETCH_COUNT = '13';
        process.env.BYAI_REDIS_MAX_CONNECTIONS = '123';

        await runWorker(DummyWorker, {
            workerId: 'w-1',
        });

        expect(initRedis).toHaveBeenCalledWith(
            expect.objectContaining({
                host: 'localhost',
                port: 6379,
                db: 0,
            })
        );
        expect(WorkerRunner).toHaveBeenCalledWith(
            expect.any(DummyWorker),
            expect.objectContaining({
                groupName: 'agent_engines',
                maxConcurrency: 77,
                fetchCount: 13,
            })
        );
        expect(closeRedis).toHaveBeenCalledTimes(1);
    });

    it('supports pluginDir, pluginList, pluginConfigurator and hook settings', async () => {
        const extraPlugin = new DummyPlugin('p-extra');
        const configurator = jest.fn(async (registry: any) => {
            registry.registerBundle(new DummyPlugin('p-config'));
        });

        await runWorker(DummyWorker, {
            workerId: 'w-2',
            pluginDir: '/tmp/fake-plugins',
            pluginList: [extraPlugin],
            pluginConfigurator: configurator,
            pluginHookTimeoutSeconds: 2.5,
            pluginLogHookStatsOnShutdown: false,
        });

        const worker = WorkerRunner.mock.calls[0][0] as any;
        expect(configurator).toHaveBeenCalledTimes(1);
        expect(configurator).toHaveBeenCalledWith(worker.pluginRegistry);
        expect(worker.pluginRegistry.plugins.map((p: any) => p.pluginId)).toEqual(
            expect.arrayContaining(['p-extra', 'p-config'])
        );
        expect(worker.pluginRegistry.logHookStatsOnShutdown).toBe(false);
        expect(worker.pluginRegistry.plugins.every((p: any) => p.hookTimeoutSeconds === 2.5)).toBe(true);
    });

    it('passes history storage to provider and always closes redis on runner failure', async () => {
        const start = jest.fn().mockRejectedValue(new Error('start failed'));
        WorkerRunner.mockImplementationOnce((_worker: any, _options: any) => ({ start }));

        await expect(
            runWorker(DummyWorker, {
                workerId: 'w-3',
                historyStorage: new DummyHistoryStorage(),
            })
        ).rejects.toThrow('start failed');

        expect(closeRedis).toHaveBeenCalledTimes(1);
    });
});


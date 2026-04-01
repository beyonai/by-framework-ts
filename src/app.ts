import { Redis } from 'ioredis';
import { initRedis, closeRedis } from './redis_client';
import { WorkerRunner } from './runner';
import { WorkerRegistry } from './registry';
import { GatewayWorker } from './worker';
import { Plugin } from './extensions/plugin';
import { PluginRegistry } from './extensions/registry';
import { BaseHistoryStorage, HistoryProvider } from './history';
import { WorkspaceManager } from './workspace';
import { HookSandbox } from './sandbox';

export interface RunWorkerOptions {
    workerId?: string;
    redisHost?: string;
    redisPort?: number;
    redisDb?: number;
    redisPassword?: string;
    redisUsername?: string;
    consumerGroup?: string;
    workspaceDir?: string;
    maxConcurrency?: number;
    fetchCount?: number;
    redisMaxConnections?: number;
    pluginList?: Plugin[];
    pluginConfigurator?: (registry: PluginRegistry) => void | Promise<void>;
    pluginDir?: string;
    pluginHookTimeoutSeconds?: number;
    pluginLogHookStatsOnShutdown?: boolean;
    historyStorage?: BaseHistoryStorage;
    redisClient?: Redis;
}

/**
 * Check if a value is a Promise (similar to Python's inspect.isawaitable)
 */
function isAwaitable(value: any): value is Promise<void> {
    return value !== null && typeof value === 'object' && typeof value.then === 'function';
}

export async function runWorker(
    WorkerClass: new (...args: any[]) => GatewayWorker,
    options: RunWorkerOptions = {}
): Promise<void> {
    const maxConcurrency =
        options.maxConcurrency ?? Number(process.env.BYAI_WORKER_CONCURRENCY || 50);
    const fetchCount =
        options.fetchCount ?? Number(process.env.BYAI_WORKER_FETCH_COUNT || 10);
    const redisMaxConnections =
        options.redisMaxConnections ?? Number(process.env.BYAI_REDIS_MAX_CONNECTIONS || maxConcurrency + 10);

    const redisClient =
        options.redisClient ||
        initRedis({
            host: options.redisHost || 'localhost',
            port: options.redisPort || 6379,
            db: options.redisDb || 0,
            username: options.redisUsername,
            password: options.redisPassword,
            // ioredis 单连接模式不支持 Python 版连接池参数，这里仅保留计算结果用于对齐语义。
            ...(redisMaxConnections ? {} : {}),
        });

    if (options.historyStorage) {
        HistoryProvider.setStorage(options.historyStorage);
    }

    const pluginRegistry = new PluginRegistry();
    if (options.pluginDir) {
        await pluginRegistry.loadPluginsFromDir(options.pluginDir);
    }
    for (const plugin of options.pluginList || []) {
        pluginRegistry.registerBundle(plugin);
    }
    if (options.pluginConfigurator) {
        const result = options.pluginConfigurator(pluginRegistry);
        // Check if result is awaitable (Promise), similar to Python's inspect.isawaitable
        if (isAwaitable(result)) {
            await result;
        }
    }
    if (options.pluginHookTimeoutSeconds) {
        pluginRegistry.applyDefaultHookTimeout(options.pluginHookTimeoutSeconds);
    }
    pluginRegistry.logHookStatsOnShutdown = options.pluginLogHookStatsOnShutdown ?? true;

    const registry = new WorkerRegistry(redisClient);
    const workspaceManager = new WorkspaceManager(options.workspaceDir || '/tmp/gateway-workspace');
    const sandbox = new HookSandbox();
    const worker = new WorkerClass(
        options.workerId || 'worker-1',
        registry,
        redisClient,
        pluginRegistry,
        workspaceManager,
        sandbox
    );

    const runner = new WorkerRunner(worker, {
        redisClient,
        groupName: options.consumerGroup || 'agent_engines',
        maxConcurrency,
        fetchCount,
    });

    try {
        await runner.start();
    } catch (error: unknown) {
        // Handle cancellation gracefully, similar to Python's asyncio.CancelledError
        if (error instanceof Error && error.name === 'CancelledError') {
            console.info('Worker runner stopped by cancellation request');
        } else {
            console.error('Error running worker:', error);
            throw error;
        }
    } finally {
        await closeRedis();
    }
}

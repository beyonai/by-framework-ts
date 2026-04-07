import { Redis } from 'ioredis';
import { AnonymousWorker } from './worker';
import { WorkerRunner } from './runner';
import { GatewayCommand } from './protocol/commands';
import { AgentContext } from './context';
import { WorkerRegistry } from './registry';
import { PluginRegistry } from './extensions/registry';

export interface WorkerOptions {
    workerId: string;
    agentTypes: string[];
    onTask: (command: GatewayCommand, context: AgentContext) => Promise<any>;
    redisClient?: Redis;
    groupName?: string;
    maxConcurrency?: number;
    fetchCount?: number;
    pluginRegistry?: PluginRegistry;
}

/**
 * 工厂方法：创建一个一站式的 WorkerRunner。
 * 内部自动创建 AnonymousWorker，并配置好 Runner。
 */
export function createWorkerRunner(options: WorkerOptions): WorkerRunner {
    const registry = new WorkerRegistry(options.redisClient);
    const worker = new AnonymousWorker({
        workerId: options.workerId,
        agentTypes: options.agentTypes,
        onTask: options.onTask,
        registry,
        redisClient: options.redisClient,
        pluginRegistry: options.pluginRegistry,
    });

    let runnerRedisClient = options.redisClient;
    if (options.redisClient && typeof (options.redisClient as any).duplicate === 'function') {
        try {
            runnerRedisClient = (options.redisClient as any).duplicate();
        } catch (err) {
            console.warn(
                `[${options.workerId}] Failed to duplicate redis client for runner, fallback to shared client:`,
                err
            );
            runnerRedisClient = options.redisClient;
        }
    }

    return new WorkerRunner(worker, {
        redisClient: runnerRedisClient,
        groupName: options.groupName,
        maxConcurrency: options.maxConcurrency,
        fetchCount: options.fetchCount,
    });
}

import { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { QueueNames, RegistryKeys } from './constants';
import { EventType } from './protocol/event_type';
import { AskAgentCommand } from './protocol/commands';
import { MessageHeader } from './protocol/message_header';
import {
    StateChangeEvent,
    StreamChunkEvent,
    ArtifactEvent,
    AskUserEvent,
} from './protocol/events';

import { GatewayDataEmitter } from './emitter';
import { AgentConfig } from './extensions/agent_config';
import type { PluginRegistry } from './extensions/registry';
import { HistoryProvider } from './history';

export class TaskCancelledError extends Error {
    constructor(message: string = 'task cancelled') {
        super(message);
        this.name = 'TaskCancelledError';
    }
}

interface CancelSignalLegacy {
    readonly aborted?: boolean;
    readonly is_set?: boolean;
    readonly reason?: string;
}

export class AgentContext {
    private emitter: GatewayDataEmitter;
    private agentConfigs: ReadonlyArray<AgentConfig> = [];
    private prevAgentConfigs: ReadonlyArray<AgentConfig> = [];
    private responseBuffer: ReadonlyArray<string> = [];
    private historySaved = false;

    constructor(
        public readonly sessionId: string,
        public readonly traceId: string,
        private readonly redis: Redis,
        private readonly currentAgentType: string = '',
        private readonly currentMessageId: string = '',
        public readonly currentCommand?: unknown,
        private readonly cancelSignal?: AbortSignal | CancelSignalLegacy,
        private readonly cancelReason: string = '',
        public readonly pluginRegistry?: PluginRegistry
    ) {
        this.emitter = new GatewayDataEmitter(this.redis);
    }

    setAgentConfigs(newConfigs: ReadonlyArray<AgentConfig>): void {
        this.agentConfigs = [...newConfigs];
    }

    listAgentConfigs(): ReadonlyArray<AgentConfig> {
        return Object.freeze([...this.agentConfigs]);
    }

    getAgentConfig(agentId: string): AgentConfig | undefined {
        return this.agentConfigs.find((c) => c.agent_id === agentId);
    }

    freezePrevAgentConfigs(): void {
        this.prevAgentConfigs = [...this.agentConfigs];
    }

    getPrevAgentConfigs(): ReadonlyArray<AgentConfig> {
        return this.prevAgentConfigs;
    }

    async callTool(name: string, kwargs: Readonly<Record<string, unknown>> = {}): Promise<unknown> {
        for (const config of this.agentConfigs) {
            const tool = config.tools?.[name];
            if (typeof tool === 'function') {
                return await tool(kwargs);
            }
        }
        throw new Error(`Tool '${name}' not found in any agent config`);
    }

    isCancelRequested(): boolean {
        return Boolean(this.cancelSignal && (
            (this.cancelSignal as CancelSignalLegacy).aborted ||
            (this.cancelSignal as CancelSignalLegacy).is_set
        ));
    }

    async checkCancelled(): Promise<void> {
        if (this.isCancelRequested()) {
            const signalReason = this.cancelSignal && 'reason' in this.cancelSignal
                ? String((this.cancelSignal as { reason?: string }).reason || '')
                : '';
            throw new TaskCancelledError(this.cancelReason || signalReason || 'task cancelled');
        }
    }

    async getActiveWorkers(): Promise<Record<string, unknown>> {
        const { WorkerRegistry } = await import('./registry');
        const registry = new WorkerRegistry(this.redis);
        return registry.getAllWorkers() as unknown as Record<string, unknown>;
    }

    private async emitEvent(params: {
        readonly eventType: string;
        readonly data?: Readonly<Record<string, unknown>>;
        readonly stateMsg?: string;
        readonly artifactUrl?: string;
        readonly metadata?: Readonly<Record<string, unknown>>;
    }): Promise<void> {
        await this.emitter.emitEvent({
            sessionId: this.sessionId,
            traceId: this.traceId,
            sourceAgentType: this.currentAgentType,
            messageId: this.currentMessageId,
            ...params
        });
    }

    async emitChunk(event: StreamChunkEvent | string, eventType?: string): Promise<void> {
        const content = typeof event === 'string' ? event : (event.content || '');
        if (content) {
            this.responseBuffer = [...this.responseBuffer, content];
        }
        await this.emitter.emitChunk(this.sessionId, this.traceId, event, {
            sourceAgentType: this.currentAgentType,
            messageId: this.currentMessageId,
            eventType
        });
        if (eventType === EventType.APP_STREAM_RESPONSE) {
            await this.flushToHistory();
        }
    }

    async emitState(event: StateChangeEvent | string, eventType?: string): Promise<void> {
        await this.emitter.emitState(this.sessionId, this.traceId, event, {
            sourceAgentType: this.currentAgentType,
            messageId: this.currentMessageId,
            eventType
        });
    }

    async emitArtifact(event: ArtifactEvent | string, eventType?: string): Promise<void> {
        await this.emitter.emitArtifact(this.sessionId, this.traceId, event, {
            sourceAgentType: this.currentAgentType,
            messageId: this.currentMessageId,
            eventType
        });
    }

    async askUser(event: AskUserEvent | string): Promise<{ readonly status: string }> {
        await this.emitter.askUser(this.sessionId, this.traceId, event, {
            sourceAgentType: this.currentAgentType,
            messageId: this.currentMessageId,
        });
        return { status: 'WAITING_USER' };
    }

    async flushToHistory(): Promise<void> {
        if (this.historySaved || this.responseBuffer.length === 0) {
            return;
        }
        const fullContent = this.responseBuffer.join('');
        await HistoryProvider.saveMessage(this.sessionId, 'assistant', fullContent, {
            trace_id: this.traceId,
            agent_id: this.currentAgentType,
            message_id: this.currentMessageId,
        });
        this.historySaved = true;
    }

    async callAgent(params: {
        readonly targetAgentType: string;
        readonly content: string;
        readonly payload?: Readonly<Record<string, unknown>>;
        readonly waitForReply?: boolean;
    }): Promise<{ readonly status: string; readonly messageId: string; readonly targetAgentType: string }> {
        const messageId = `msg-${uuidv4().slice(0, 8)}`;
        const waitForReply = params.waitForReply ?? true;

        const command = new AskAgentCommand(
            new MessageHeader(messageId, this.sessionId, this.traceId, {
                sourceAgentType: waitForReply ? this.currentAgentType : '',
                targetAgentType: params.targetAgentType,
                parentMessageId: this.currentMessageId,
            }),
            params.content,
            waitForReply,
            { ...(params.payload || {}) }
        );

        await this.redis.xadd(
            QueueNames.ctrl_stream(params.targetAgentType),
            '*',
            'data',
            JSON.stringify(command.toDict())
        );

        return {
            status: 'QUEUED',
            messageId,
            targetAgentType: params.targetAgentType,
        };
    }

    /**
     * 并发派发任务组 (Scatter-Gather)
     * @param tasks 任务列表
     * @returns 任务组 ID
     */
    async dispatchGroup(tasks: ReadonlyArray<{ readonly targetAgentType: string; readonly content: string; readonly payload?: Readonly<Record<string, unknown>> }>): Promise<string> {
        const groupId = `tg-${uuidv4().slice(0, 8)}`;
        const groupKey = RegistryKeys.task_group(groupId);

        // 1. 初始化计数器
        await this.redis.hset(groupKey, {
            total: tasks.length.toString(),
            completed: '0',
            source_agent_id: this.currentAgentType || '',
            parent_message_id: this.currentMessageId || '',
            trace_id: this.traceId
        });
        await this.redis.expire(groupKey, 3600); // 1小时过期

        // 2. 批量派发
        for (const task of tasks) {
            const messageId = `msg-${uuidv4().slice(0, 8)}`;
            const command = new AskAgentCommand(
                new MessageHeader(messageId, this.sessionId, this.traceId, {
                    sourceAgentType: this.currentAgentType,
                    targetAgentType: task.targetAgentType,
                    parentMessageId: this.currentMessageId,
                    taskGroupId: groupId, // 关键：绑定任务组
                }),
                task.content,
                true,
                { ...(task.payload || {}) }
            );

            await this.redis.xadd(
                QueueNames.ctrl_stream(task.targetAgentType),
                '*',
                'data',
                JSON.stringify(command.toDict())
            );
        }

        return groupId;
    }
}

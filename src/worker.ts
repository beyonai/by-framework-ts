import * as fs from 'fs/promises';
import * as path from 'path';
import { Redis } from 'ioredis';
import { v4 as uuidv4 } from 'uuid';
import { getRedis } from './redis_client';
import { GatewayCommand, ResumeCommand, AskAgentCommand } from './protocol/commands';
import { AgentState, isTerminalState } from './protocol/agent_state';
import { EventType } from './protocol/event_type';
import { AgentContext, TaskCancelledError } from './context';
import {
    CLIENT_SOURCE_AGENT_TYPE,
    QueueNames,
    RegistryKeys,
    TASK_GROUP_TTL_SECONDS,
    TASK_GROUP_FIELD_ABORTED,
    TASK_GROUP_FIELD_TOTAL,
    TASK_GROUP_FIELD_COMPLETED,
    singleCallTaskGroupId,
} from './constants';
import { flushPendingGroupReplies } from './liveness/wait_reply';
import { mergeResumeMetadata } from './resume_metadata';
import { WorkerRegistry } from './registry';
import { WorkerHeartbeat, type HeartbeatObservers } from './heartbeat';
import { MessageHeader } from './protocol/message_header';
import { JsonValue, ProcessCommandResult, WireContent, normalizeProcessResult, AgentTaskResult } from './protocol/results';
import { PluginRegistry } from './extensions/registry';
import { HistoryProvider } from './history';
import { WorkspaceManager } from './workspace';
import { HookSandbox, getActiveWorkspace, setActiveWorkspace } from './sandbox';
import { FileStorage } from './runtime/filestore/base';
import { SpanRecorder } from './trace/span_recorder';

// === Types ===
interface HandleMessageOptions {
    readonly cancelSignal?: AbortSignal | CancelSignalLegacy;
    readonly cancelReason?: string;
    /**
     * Snapshot of the execution this message reattaches to, forwarded by
     * WorkerRunner. `existingData` is the full dispatch-time registry record —
     * the only place a resumed execution's original caller (source_agent_type /
     * parent_message_id / task_group_id) survives, since the ResumeCommand
     * header describes the hop that just finished instead.
     * Mirrors Python RunningExecution's is_resumed / existing_data.
     */
    readonly execution?: {
        readonly parentMessageId?: string;
        readonly isResumed?: boolean;
        readonly existingData?: Record<string, any> | null;
    };
    /** Execution ID pre-computed by runner; propagated into AgentContext for trace linkage. */
    readonly executionId?: string;
    /** SpanRecorder from runner; reused in AgentContext so spans share the same exporter. */
    readonly spanRecorder?: SpanRecorder;
    /** Mutable ref filled with the AgentContext once it is created; lets runner read telemetry. */
    readonly executionRef?: { context: AgentContext | null };
}

interface CancelSignalLegacy {
    readonly aborted?: boolean;
    readonly is_set?: boolean;
}

export abstract class GatewayWorker {
    public readonly workerId: string;
    protected readonly redis: Redis;
    public readonly registry: WorkerRegistry;
    public readonly pluginRegistry: PluginRegistry;
    protected readonly workspaceManager?: WorkspaceManager;
    protected readonly sandbox?: HookSandbox;
    public readonly storage?: FileStorage;
    private _heartbeat: WorkerHeartbeat | null = null;

    public constructor(
        workerId: string,
        registry?: WorkerRegistry,
        redisClient?: Redis,
        pluginRegistry?: PluginRegistry,
        workspaceManager?: WorkspaceManager,
        sandbox?: HookSandbox,
        storage?: FileStorage
    ) {
        this.workerId = workerId;
        this.redis = redisClient ?? getRedis();
        this.registry = registry ?? new WorkerRegistry(this.redis);
        this.pluginRegistry = pluginRegistry ?? new PluginRegistry();
        this.workspaceManager = workspaceManager;
        this.sandbox = sandbox;
        this.storage = storage;
    }

    /** Return the heartbeat interval in seconds. */
    get heartbeatInterval(): number {
        return RegistryKeys.WORKER_DEFAULT_HEARTBEAT_INTERVAL_SECONDS;
    }

    /** Return the worker online lease TTL in seconds. */
    get heartbeatLeaseTtlSeconds(): number {
        return RegistryKeys.WORKER_DEFAULT_LEASE_TTL_SECONDS;
    }

    abstract getAgentTypes(): ReadonlyArray<string>;

    abstract processCommand(command: GatewayCommand, context: AgentContext): Promise<ProcessCommandResult>;

    async onCancelTask(_command: unknown): Promise<void> {
        console.log(`[${this.workerId}] Received cancel request`);
    }

    async startHeartbeat(
        lifecycleCallback?: (lifecycle: string) => void,
        denylistRefresh?: (denied: Set<string>) => void,
        healthCheck?: () => boolean,
        onUnhealthy?: () => void,
        observers: HeartbeatObservers = {}
    ): Promise<void> {
        await this.pluginRegistry.onWorkerStartup(this);
        this._heartbeat = new WorkerHeartbeat(
            this.workerId,
            [...this.getAgentTypes()],
            this.redis,
            this.registry,
            this.heartbeatInterval * 1000,
            this.heartbeatLeaseTtlSeconds,
            lifecycleCallback,
            denylistRefresh,
            healthCheck,
            onUnhealthy,
            observers
        );
        await this._heartbeat.start();
    }

    async stopHeartbeat(): Promise<void> {
        if (this._heartbeat) {
            await this._heartbeat.stop();
            this._heartbeat = null;
        }
    }

    async handleMessage(
        command: GatewayCommand,
        options: HandleMessageOptions = {}
    ): Promise<AgentTaskResult> {
        const traceId = command.header.traceId || uuidv4().replace(/-/g, '');
        // The command the caller is owed a reply against stays the RAW one:
        // resolveReplyCommand below reads it, and the outbound direction must
        // not inherit the inbound merge. Everything the business side touches
        // — the context's currentCommand and the argument handed to
        // processCommand — uses the restored one instead.
        const rawCommand = command;
        const inboundCommand = GatewayWorker.restoreInboundMetadata(command, options.execution);
        const context = new AgentContext(
            inboundCommand.header.sessionId,
            traceId,
            this.redis,
            inboundCommand.header.targetAgentType,
            inboundCommand.header.messageId,
            inboundCommand,
            options.cancelSignal,
            options.cancelReason || '',
            this.pluginRegistry,
            options.executionId || '',
            options.spanRecorder
        );
        context.setAgentConfigs(this.pluginRegistry.agentConfigs);
        if (options.executionRef) {
            options.executionRef.context = context;
        }

        const isResume = command instanceof ResumeCommand;
        // Not `!!header.sourceAgentType && !isResume`: a resume's header names
        // the sub-agent that just finished, so that predicate both denied every
        // resumed execution a reply AND would have addressed one to the callee.
        // resolveReplyCommand rebuilds the ORIGINAL dispatch header from the
        // execution snapshot instead; the whole reply — routing included — must
        // be driven off that command, not off `command`.
        const replyCommand = GatewayWorker.resolveReplyCommand(rawCommand, options.execution);
        const hasSourceAgent = replyCommand !== null;

        // Determine parent message id - restore from execution if resumed
        let parentMessageId = command.header.parentMessageId || '';
        if (options.execution?.isResumed && options.execution?.parentMessageId) {
            parentMessageId = options.execution.parentMessageId;
            console.log(`[${this.workerId}] Task Resumed: Restored parent_message_id=${parentMessageId}`);
        } else {
            console.log(`[${this.workerId}] New Task: message_id=${command.header.messageId}, parent_message_id=${parentMessageId}`);
        }

        // Permission transfer tracking
        let permissionTransferred = false;

        console.log(`[${this.workerId}] Processing message: ${command.header.messageId}`);
        let workspacePaths: { private: string; public?: string } | null = null;
        const prevWorkspace = getActiveWorkspace();

        try {
            await this.pluginRegistry.onTaskStart(context);
            if (!isResume && command instanceof AskAgentCommand) {
                await HistoryProvider.saveMessage(command.header.sessionId, 'user', command.content as any, {
                    message_id: command.header.messageId,
                    trace_id: command.header.traceId,
                });
            }

            if (this.workspaceManager) {
                workspacePaths = await this.workspaceManager.setupWorkspace(
                    command.header.sessionId,
                    command.header.messageId
                ) as { private: string; public?: string } | null;
                if (this.sandbox) {
                    this.sandbox.install();
                }
                if (workspacePaths?.private) {
                    setActiveWorkspace(workspacePaths.private);
                }
            }

            // Pre-processing cancellation check: if cancelled before processing, bail out immediately
            const cancelSignal = options.cancelSignal as CancelSignalLegacy | AbortSignal | undefined;
            if (cancelSignal && ((cancelSignal as CancelSignalLegacy).aborted || (cancelSignal as CancelSignalLegacy).is_set)) {
                throw new TaskCancelledError(options.cancelReason || 'task cancelled before processing');
            }

            if (isResume) {
                // Persist agent return state
                await this.persistAgentReturnState(workspacePaths, command);

                // Check for scatter-gather join
                if (command.header.taskGroupId) {
                    const groupKey = QueueNames.task_group(command.header.taskGroupId);
                    const resultsKey = QueueNames.task_group_results(command.header.taskGroupId);
                    const totalStr = await this.redis.hget(groupKey, TASK_GROUP_FIELD_TOTAL);
                    if (totalStr !== null) {
                        if (await this.redis.hget(groupKey, TASK_GROUP_FIELD_ABORTED)) {
                            // The fan-out threw partway through, so this caller
                            // execution was already failed. Counting a late
                            // sibling's reply would resume a dead execution and,
                            // once `completed` reached `total`, aggregate a group
                            // that was never fully dispatched.
                            console.warn(
                                `[${this.workerId}] TaskGroup ${command.header.taskGroupId} is aborted, `
                                + `discarding late reply from sub-task `
                                + `message_id=${command.header.parentMessageId}`
                            );
                            return new AgentTaskResult({ status: `${AgentState.CANCELLED}: group_aborted` });
                        }

                        // Store result in Redis Hash for distributed access
                        const resultData = {
                            status: (command as ResumeCommand).status,
                            reply_data: (command as ResumeCommand).replyData,
                            content: (command as ResumeCommand).content,
                            // This ResumeCommand flows FROM the sub-agent back
                            // TO the caller, so its header's sourceAgentType is
                            // the sub-agent that produced this result — i.e. the
                            // original dispatch's targetAgentType.
                            target_agent_type: command.header.sourceAgentType,
                            metadata: command.header.metadata,
                            extra_payload: (command as ResumeCommand).extraPayload,
                        };
                        // enqueueAgentReturn sets a reply's header.messageId to
                        // the ORIGINAL dispatch's parentMessageId (the caller's
                        // own message id) — identical across every sibling in
                        // this Task Group. Keying results by that lets siblings
                        // overwrite each other, so only the last reply survives.
                        // header.parentMessageId on the reply is the sub-task's
                        // own dispatch-time message id instead, which is unique
                        // per task. Mirrors Python worker.py's group join.
                        await this.redis.hset(resultsKey, command.header.parentMessageId, JSON.stringify(resultData));
                        await this.redis.expire(resultsKey, TASK_GROUP_TTL_SECONDS);

                        const completed = await this.redis.hincrby(groupKey, TASK_GROUP_FIELD_COMPLETED, 1);
                        if (completed < parseInt(totalStr, 10)) {
                            console.log(`[${this.workerId}] TaskGroup ${command.header.taskGroupId} completed ${completed}/${totalStr}, waiting...`);
                            // Still waiting on the rest of the group: this caller
                            // is suspended, not queued behind a worker. QUEUED
                            // here is indistinguishable from "never picked up",
                            // which is what a sweep's triage has to tell apart.
                            return new AgentTaskResult({ status: `${AgentState.WAITING_AGENT}: waiting_for_group` });
                        }
                        console.log(`[${this.workerId}] TaskGroup ${command.header.taskGroupId} ALL COMPLETED (${totalStr})!`);
                    }
                }
                await context.emitState({ state: AgentState.RESUMED });
            }

            // inboundCommand, not command: the handler reads what this
            // execution was originally dispatched with, merged under the waking
            // message's own metadata. Everything above this line — the Task
            // Group join's resultData in particular — deliberately stays on the
            // raw command, matching Python, where the join reads `header` off
            // `raw_command` rather than the restored one.
            const result = await this.processCommand(inboundCommand, context);
            // Stand-ins for Task Group members that never reached a worker go
            // out only once the handler has returned normally, and never when it
            // threw. Sent from inside dispatchGroup instead, they would land on
            // this caller's own control stream strictly before it is recorded as
            // suspended, making the "a sibling replies before the caller
            // suspends" race certain rather than rare.
            await flushPendingGroupReplies(this.redis, context, this.workerId);
            const normalizedResult = normalizeProcessResult(result);
            // "Suspended" has to mean the same thing here as it does for the
            // persisted status (see applySuspendedStatus): a handler that
            // reached a terminal state is finished whatever it dispatched, and
            // it will never be resumed to produce the reply later — so it owes
            // its caller one NOW. Reading context.isSuspended() alone would let
            // such an execution both record itself COMPLETED and stay silent,
            // suspending its caller until a sweep bails it out.
            const isSuspended = context.isSuspended() && !isTerminalState(normalizedResult.status);
            const taskResult = GatewayWorker.applySuspendedStatus(normalizedResult, context);

            // Determine final status from result
            const finalStatus = taskResult.status;

            if (hasSourceAgent && !isSuspended) {
                // A suspended execution has no result yet — only the value the
                // handler returned so it could unwind. Forwarding that as the
                // reply wakes our caller early with a placeholder and burns the
                // one reply it was waiting for; the real result goes out when
                // this execution resumes and finishes.
                permissionTransferred = true;
                const returnOptions = {
                    content: taskResult.content,
                    metadata: taskResult.metadata,
                    extraPayload: taskResult.extraPayload,
                };
                const returnInfo = { status: taskResult.status, replyData: taskResult.replyData, ...returnOptions };
                await this.pluginRegistry.onAgentReturnStart(context, command, returnInfo);
                try {
                    await this.enqueueAgentReturn(replyCommand!, taskResult.status, taskResult.replyData, returnOptions);
                    await this.pluginRegistry.onAgentReturnComplete(context, command, returnInfo);
                } catch (returnErr: any) {
                    await this.pluginRegistry.onAgentReturnError(context, command, returnInfo, returnErr instanceof Error ? returnErr : new Error(String(returnErr)));
                    throw returnErr;
                }
            }
            await this.pluginRegistry.onTaskComplete(context, result);

            // Extract final message and emit FINAL_ANSWER
            let finalMessage: string | null = null;
            if (typeof taskResult.content === 'string' && taskResult.content) {
                finalMessage = taskResult.content;
            } else if (typeof taskResult.replyData === 'string' && taskResult.replyData) {
                finalMessage = taskResult.replyData;
            } else if (taskResult.replyData !== null && taskResult.replyData !== undefined) {
                finalMessage = JSON.stringify(taskResult.replyData);
            }
            taskResult.finalAnswer = finalMessage || "";

            if (finalMessage !== null && !context.isFinalAnswerEmitted()) {
                await context.emitChunk(finalMessage, EventType.FINAL_ANSWER);
            }

            // Emit APP_STREAM_RESPONSE if conditions are met
            const shouldEmitStreamEnd = !hasSourceAgent && isTerminalState(finalStatus) && !permissionTransferred && !context.isSuspended();
            if (shouldEmitStreamEnd) {
                if (!context.isStreamFinished()) {
                    await context.emitChunk('', EventType.APP_STREAM_RESPONSE);
                }
            } else {
                await context.flushToHistory();
            }

            return taskResult;
        } catch (error: unknown) {
            if (error instanceof TaskCancelledError || (error instanceof Error && error.name === 'TaskCancelledError')) {
                // Check if parent execution also has cancel_requested — if so, skip callback.
                // The id to check is the CALLER's, which for a resumed execution
                // only the rebuilt reply header carries (the raw header's
                // parentMessageId is the sub-task we just called).
                let shouldCallback = hasSourceAgent;
                const callerMessageId = replyCommand?.header.parentMessageId || '';
                if (shouldCallback && callerMessageId) {
                    const parentExec = await this.registry.getExecutionByMessageId(callerMessageId, command.header.sessionId);
                    if (parentExec?.cancel_requested) {
                        shouldCallback = false;
                    }
                }
                if (shouldCallback) {
                    // Sent even if the context suspended: a cancelled execution
                    // will never resume, so this is the caller's last chance to
                    // hear anything at all.
                    const cancelReplyData = { reason: String(error instanceof Error ? error.message : error) };
                    const cancelReturnInfo = { status: AgentState.CANCELLED, replyData: cancelReplyData };
                    await this.pluginRegistry.onAgentReturnStart(context, command, cancelReturnInfo);
                    try {
                        await this.enqueueAgentReturn(replyCommand!, AgentState.CANCELLED, cancelReplyData);
                        await this.pluginRegistry.onAgentReturnComplete(context, command, cancelReturnInfo);
                    } catch (returnErr: any) {
                        await this.pluginRegistry.onAgentReturnError(context, command, cancelReturnInfo, returnErr instanceof Error ? returnErr : new Error(String(returnErr)));
                    }
                }

                const shouldEmitStreamEnd = !hasSourceAgent && !permissionTransferred;
                if (shouldEmitStreamEnd) {
                    await context.emitChunk('', EventType.APP_STREAM_RESPONSE);
                } else {
                    await context.flushToHistory();
                }
                return new AgentTaskResult({ status: AgentState.CANCELLED });
            }
            const err = error instanceof Error ? error : new Error(String(error));
            console.error(`[${this.workerId}] Task failed:`, err);
            if (hasSourceAgent) {
                // Also sent regardless of suspension: the execution died, so no
                // later resume will produce the reply the caller awaits.
                const failedReplyData = { error: String(error) };
                const failedReturnInfo = { status: AgentState.FAILED, replyData: failedReplyData };
                await this.pluginRegistry.onAgentReturnStart(context, command, failedReturnInfo);
                try {
                    await this.enqueueAgentReturn(replyCommand!, AgentState.FAILED, failedReplyData);
                    await this.pluginRegistry.onAgentReturnComplete(context, command, failedReturnInfo);
                } catch (returnErr: any) {
                    await this.pluginRegistry.onAgentReturnError(context, command, failedReturnInfo, returnErr instanceof Error ? returnErr : new Error(String(returnErr)));
                }
            }
            await this.pluginRegistry.onTaskError(context, err);

            const shouldEmitStreamEnd = !hasSourceAgent && !permissionTransferred;
            if (shouldEmitStreamEnd) {
                await context.emitChunk('', EventType.APP_STREAM_RESPONSE);
            } else {
                await context.flushToHistory();
            }

            return new AgentTaskResult({ status: AgentState.FAILED });
        } finally {
            setActiveWorkspace(prevWorkspace);
            if (this.sandbox) {
                this.sandbox.uninstall();
            }
            if (this.workspaceManager && workspacePaths) {
                await this.workspaceManager.cleanupTask(command.header.sessionId, command.header.messageId);
            }
        }
    }

    /**
     * Return the command whose caller this execution owes a reply to.
     *
     * `null` means "nobody is waiting" — a root execution, or a resume we could
     * not attribute.
     *
     * For a fresh dispatch the answer is the command itself: its header's
     * sourceAgentType is the caller.
     *
     * For a **resumed** execution it is not. The ResumeCommand that woke us
     * describes the hop that finished (its `sourceAgentType` is our *sub*-agent,
     * its `parentMessageId` is our sub-task, and its `taskGroupId` is our
     * sub-group) — replying against that header would send our result back down
     * to the sub-agent we just called. The caller is instead whatever the
     * ORIGINAL dispatch recorded in the execution registry, which is why this
     * rebuilds the original dispatch header from the execution snapshot.
     * Treating "is a resume" as "has no caller" (the previous
     * `!!sourceAgentType && !isResume`) is what made an A -> B -> C chain
     * silently drop B's result on the floor.
     *
     * `header.metadata` is restored the same way, as a full REPLACEMENT rather
     * than a merge with the waking message's own metadata: the waking message
     * (an askUser answer, or a sub-call's reply) is transient plumbing for that
     * one hop, not something the caller ever sent or asked for. Leaking it would
     * let a transient hop overwrite the caller's own data instead of being
     * layered under taskResult.metadata the way enqueueAgentReturn's merge
     * already does correctly. If the caller's metadata is missing from the
     * snapshot (an execution recorded before this field existed), this degrades
     * to an empty object rather than leaking the waking message's metadata.
     *
     * A root execution's record names CLIENT_SOURCE_AGENT_TYPE as its source,
     * which is a marker rather than an agent type — it has to be excluded
     * explicitly, or every client-dispatched execution that ever resumes (an
     * askUser round is the common one) would post its result to a control
     * stream nobody consumes AND stop emitting the end-of-stream event the user
     * is actually waiting on.
     *
     * Mirrors Python worker.py's _resolve_reply_command.
     */
    private static resolveReplyCommand(
        command: GatewayCommand,
        execution?: HandleMessageOptions['execution']
    ): GatewayCommand | null {
        const header = command.header;
        if (!(command instanceof ResumeCommand)) {
            return header.sourceAgentType ? command : null;
        }

        const snapshot = execution?.existingData || {};
        const callerAgentType = String(snapshot.source_agent_type || '');
        if (!callerAgentType || callerAgentType === CLIENT_SOURCE_AGENT_TYPE) {
            return null;
        }
        return new ResumeCommand(
            new MessageHeader(header.messageId, header.sessionId, header.traceId, {
                sourceAgentType: callerAgentType,
                targetAgentType: header.targetAgentType,
                parentMessageId: String(snapshot.parent_message_id || ''),
                taskGroupId: String(snapshot.task_group_id || ''),
                userCode: header.userCode,
                userName: header.userName,
                // Replacement, not a merge with header.metadata — see above.
                metadata: { ...(snapshot.metadata || {}) },
                traceParentSpanId: header.traceParentSpanId,
                langfuseParentObservationId: header.langfuseParentObservationId,
            }),
            command.content,
            command.status,
            command.replyData,
            command.extraPayload
        );
    }

    /**
     * Give a resumed handler its own dispatch metadata back.
     *
     * The mirror image of resolveReplyCommand, and deliberately not the same
     * rule. That one rebuilds the header this execution *sends*; this one
     * rebuilds the header it *reads*. A resumed handler otherwise sees only the
     * metadata of whatever woke it up — an askUser answer's, or a sub-call's
     * reply — and everything the execution was originally dispatched with is
     * gone from the moment it first suspends.
     *
     * Merged, not replaced (the opposite of the outbound direction): this agent
     * IS the addressee of the waking message, so its metadata is real payload
     * here rather than someone else's plumbing. Original dispatch metadata is
     * the base, the waking message wins collisions. See resume_metadata.ts for
     * why the framework's per-hop trace keys are excluded from the base.
     *
     * Returns a NEW command — never mutates. The caller keeps the raw one for
     * resolveReplyCommand, so mutating here would leak the inbound merge into
     * the reply that goes out.
     *
     * Mirrors Python worker.py's _restore_inbound_metadata.
     */
    private static restoreInboundMetadata(
        command: GatewayCommand,
        execution?: HandleMessageOptions['execution']
    ): GatewayCommand {
        if (!(command instanceof ResumeCommand)) {
            return command;
        }
        const header = command.header;
        const snapshot = execution?.existingData || {};
        return new ResumeCommand(
            new MessageHeader(header.messageId, header.sessionId, header.traceId, {
                sourceAgentType: header.sourceAgentType,
                targetAgentType: header.targetAgentType,
                parentMessageId: header.parentMessageId,
                taskGroupId: header.taskGroupId,
                userCode: header.userCode,
                userName: header.userName,
                metadata: mergeResumeMetadata(
                    snapshot.metadata as Record<string, unknown> | undefined,
                    header.metadata
                ),
                traceParentSpanId: header.traceParentSpanId,
                langfuseParentObservationId: header.langfuseParentObservationId,
            }),
            command.content,
            command.status,
            command.replyData,
            command.extraPayload
        );
    }

    /**
     * Persist a suspended execution as WAITING_AGENT / WAITING_USER.
     *
     * The framework, not the business code, decides this: AgentContext knows an
     * execution suspended because it is what suspended it, whereas a business
     * handler is free to return anything (every in-tree handler returns plain
     * QUEUED, which is indistinguishable from "still queued behind a worker"
     * once persisted).
     *
     * A terminal status wins: a handler that reached COMPLETED/FAILED/CANCELLED
     * after dispatching is finished, whatever it dispatched.
     *
     * Mirrors Python worker.py's _apply_suspended_status. AgentTaskResult.status
     * is readonly, so the result is rebuilt rather than mutated (Python uses
     * dataclasses.replace for the same reason).
     */
    private static applySuspendedStatus(
        taskResult: AgentTaskResult,
        context: AgentContext
    ): AgentTaskResult {
        const suspendedState = context.suspendedState();
        if (!suspendedState || isTerminalState(taskResult.status)) {
            return taskResult;
        }
        return new AgentTaskResult({
            status: suspendedState,
            content: taskResult.content,
            replyData: taskResult.replyData,
            metadata: taskResult.metadata,
            extraPayload: taskResult.extraPayload,
            finalAnswer: taskResult.finalAnswer,
        });
    }

    private async enqueueAgentReturn(
        command: GatewayCommand,
        status: string,
        replyData: JsonValue,
        options: {
            readonly content?: WireContent;
            readonly metadata?: Readonly<Record<string, JsonValue>>;
            readonly extraPayload?: Readonly<Record<string, JsonValue>>;
        } = {}
    ): Promise<void> {
        const header = command.header;
        if (!header.sourceAgentType) return;
        const mergedMetadata = {
            ...header.metadata,
            ...(options.metadata ?? {}),
        };

        const callbackMsg = new ResumeCommand(
            new MessageHeader(header.parentMessageId || `msg-${uuidv4().slice(0, 8)}`, header.sessionId, header.traceId, {
                sourceAgentType: header.targetAgentType || this.workerId,
                targetAgentType: header.sourceAgentType,
                parentMessageId: header.messageId,
                taskGroupId: header.taskGroupId,
                userCode: header.userCode,
                userName: header.userName,
                metadata: mergedMetadata,
            }),
            options.content ?? '',
            status,
            replyData,
            options.extraPayload ?? {}
        );

        await this.persistSingleCallResult(header, callbackMsg);

        await this.redis.xadd(
            QueueNames.ctrl_stream(callbackMsg.header.targetAgentType),
            '*',
            'data',
            JSON.stringify(callbackMsg.toDict())
        );
    }

    /**
     * Persist a single (non-group) callAgent result before replying.
     *
     * markExecutionFinished() only stores the status, so a lost reply message
     * used to lose the answer with it. A Task Group already keeps full results
     * in task_group_results; a single call reuses that exact storage as a group
     * of size 1 (see singleCallTaskGroupId), which keeps recovery on one code
     * path. The reply message then carries no information that isn't
     * recoverable from Redis — it degenerates into a pure notification, which
     * is what lets a sweep synthesize a replacement reply carrying the real
     * answer instead of an error.
     *
     * The stored payload must stay isomorphic to the group-join resultData
     * built in handleMessage, since both feed the same readers. The field is
     * the sub-task's own message_id — identical to the reply's
     * header.parentMessageId the group path keys by.
     *
     * Fail-soft: losing the copy only costs recoverability, so a Redis error
     * here must never stop the reply from being sent.
     *
     * Mirrors Python worker.py's _persist_single_call_result.
     */
    private async persistSingleCallResult(
        header: MessageHeader,
        callbackMsg: ResumeCommand
    ): Promise<void> {
        if (header.taskGroupId) {
            return; // Real Task Group: handleMessage's join already stores it.
        }
        const childMessageId = header.messageId;
        if (!childMessageId) {
            return;
        }
        try {
            const resultsKey = QueueNames.task_group_results(singleCallTaskGroupId(childMessageId));
            const resultData = {
                status: callbackMsg.status,
                reply_data: callbackMsg.replyData,
                content: callbackMsg.content,
                // The sub-agent that produced this result, i.e. the original
                // dispatch's targetAgentType — which is what the reply carries
                // as its sourceAgentType.
                target_agent_type: callbackMsg.header.sourceAgentType,
                metadata: callbackMsg.header.metadata,
                extra_payload: callbackMsg.extraPayload,
            };
            await this.redis.hset(resultsKey, childMessageId, JSON.stringify(resultData));
            await this.redis.expire(resultsKey, TASK_GROUP_TTL_SECONDS);
        } catch (error) {
            console.warn(
                `[${this.workerId}] Failed to persist single-call result for `
                + `message_id=${childMessageId}: ${error}`
            );
        }
    }

    /** Persist agent return state to filesystem (aligned with Python _persist_agent_return_state). */
    protected async persistAgentReturnState(paths: { private?: string; public?: string } | null, command: GatewayCommand): Promise<void> {
        if (!paths || !paths.public) return;

        const header = command.header;
        const stateDir = path.join(paths.public, 'session', 'agent_returns');

        let stateFile: string;
        if (header.taskGroupId) {
            const groupDir = path.join(stateDir, header.taskGroupId);
            await fs.mkdir(groupDir, { recursive: true });
            stateFile = path.join(groupDir, `${header.messageId}.json`);
        } else {
            await fs.mkdir(stateDir, { recursive: true });
            const fileKey = header.parentMessageId || header.messageId;
            stateFile = path.join(stateDir, `${fileKey}.json`);
        }

        const stateData = {
            message_id: header.messageId,
            parent_message_id: header.parentMessageId,
            source_agent_type: header.sourceAgentType,
            target_agent_type: header.targetAgentType,
            user_code: header.userCode,
            user_name: header.userName,
            action_type: (command as any).constructor?.name || 'Unknown',
            status: (command as ResumeCommand).status || '',
            content: (command as ResumeCommand).content || null,
            reply_data: (command as ResumeCommand).replyData || null,
            trace_id: header.traceId,
            session_id: header.sessionId,
            metadata: header.metadata || {},
        };

        await fs.writeFile(stateFile, JSON.stringify(stateData, null, 2), 'utf-8');
    }
}

/**
 * Anonymous Worker class that allows passing callback functions to process tasks without inheritance.
 * Suitable for decoupled mode or quick integration.
 */
export class AnonymousWorker extends GatewayWorker {
    private readonly agentTypes: ReadonlyArray<string>;
    private readonly onTask: (command: GatewayCommand, context: AgentContext) => Promise<ProcessCommandResult>;

    constructor(options: {
        readonly workerId: string;
        readonly agentTypes: ReadonlyArray<string>;
        readonly onTask: (command: GatewayCommand, context: AgentContext) => Promise<ProcessCommandResult>;
        readonly registry?: WorkerRegistry;
        readonly redisClient?: Redis;
        readonly pluginRegistry?: PluginRegistry;
        readonly storage?: FileStorage;
    }) {
        const redis = options.redisClient ?? new Redis();
        const registry = options.registry ?? new WorkerRegistry(redis);
        const pluginRegistry = options.pluginRegistry ?? new PluginRegistry();
        super(options.workerId, registry, redis, pluginRegistry, undefined, undefined, options.storage);
        this.agentTypes = options.agentTypes;
        this.onTask = options.onTask;
    }

    getAgentTypes(): ReadonlyArray<string> {
        return this.agentTypes;
    }

    async processCommand(command: GatewayCommand, context: AgentContext): Promise<ProcessCommandResult> {
        return this.onTask(command, context);
    }
}

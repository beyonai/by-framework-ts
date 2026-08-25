import { WorkerRunner } from '../src/runner';
import { AnonymousWorker } from '../src/worker';
import { WorkerRegistry } from '../src/registry';
import { AskAgentCommand, ResumeCommand, GatewayCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { AgentState } from '../src/protocol/agent_state';
import { AgentContext } from '../src/context';
import { QueueNames, TASK_GROUP_FIELD_TOTAL, TASK_GROUP_FIELD_COMPLETED } from '../src/constants';
import { PluginRegistry } from '../src/extensions/registry';
import { MockRedis } from './helpers/mock_redis';

/**
 * Resume-path parity against by-framework-python's worker/runner.py +
 * core/registry.py. Everything here runs the REAL WorkerRunner, REAL
 * GatewayWorker and REAL WorkerRegistry over the shared in-memory Redis —
 * the shape Python's tests/integration/test_orphan_recovery.py uses. A fake
 * registry cannot exercise the getExecutionByMessageId -> resume causal chain
 * this file is about.
 */

interface Harness {
    redis: MockRedis;
    registry: WorkerRegistry;
    worker: AnonymousWorker;
    runner: WorkerRunner;
    handled: Array<{ command: GatewayCommand; options: any }>;
}

function buildHarness(
    onTask?: (command: GatewayCommand, context: AgentContext) => Promise<any>
): Harness {
    const redis = new MockRedis();
    const registry = new WorkerRegistry(redis as any);
    const worker = new AnonymousWorker({
        workerId: 'worker-parity',
        agentTypes: ['parity-agent'],
        registry,
        redisClient: redis as any,
        pluginRegistry: new PluginRegistry(),
        onTask: onTask ?? (async () => 'ok'),
    });

    const handled: Array<{ command: GatewayCommand; options: any }> = [];
    const originalHandleMessage = worker.handleMessage.bind(worker);
    jest.spyOn(worker, 'handleMessage').mockImplementation(async (command: any, options: any = {}) => {
        handled.push({ command, options });
        return originalHandleMessage(command, options);
    });

    const runner = new WorkerRunner(worker, {
        redisClient: redis as any,
        groupName: 'group-parity',
    });

    return { redis, registry, worker, runner, handled };
}

function resumeCommand(overrides: {
    messageId: string;
    sessionId: string;
    parentMessageId?: string;
    taskGroupId?: string;
    sourceAgentType?: string;
    status?: string;
    replyData?: any;
}): ResumeCommand {
    return new ResumeCommand(
        new MessageHeader(overrides.messageId, overrides.sessionId, 'trace-parity', {
            targetAgentType: 'parity-agent',
            sourceAgentType: overrides.sourceAgentType ?? 'child-agent',
            parentMessageId: overrides.parentMessageId ?? '',
            taskGroupId: overrides.taskGroupId ?? '',
        }),
        'reply content',
        overrides.status ?? AgentState.COMPLETED,
        overrides.replyData ?? { answer: 42 }
    );
}

describe('registry.markExecutionFinished stamps finished_at only for terminal states', () => {
    // Mirrors Python core/registry.py:
    //   if is_terminal_state(status): current["finished_at"] = now
    // Once a suspended caller is persisted as WAITING_AGENT (D2), stamping
    // finished_at unconditionally makes it look completed to latency /
    // completed_count math while it is still waiting on a sub-agent.
    let redis: MockRedis;
    let registry: WorkerRegistry;

    beforeEach(async () => {
        redis = new MockRedis();
        registry = new WorkerRegistry(redis as any);
        await registry.initializeExecution({
            execution_id: 'exec-fin',
            message_id: 'msg-fin',
            session_id: 'sess-fin',
            status: 'QUEUED',
        });
    });

    test.each([
        AgentState.COMPLETED,
        AgentState.FAILED,
        AgentState.CANCELLED,
    ])('stamps finished_at for terminal status %s', async (status) => {
        await registry.markExecutionFinished('exec-fin', 'sess-fin', status);
        const record = await registry.getExecution('exec-fin', 'sess-fin');
        expect(record!.status).toBe(status);
        expect(record!.finished_at).toBeGreaterThan(0);
    });

    test.each([
        AgentState.WAITING_AGENT,
        AgentState.WAITING_USER,
        'WAITING_AGENT: waiting_for_group',
    ])('leaves finished_at at 0 for non-terminal status %s', async (status) => {
        await registry.markExecutionFinished('exec-fin', 'sess-fin', status);
        const record = await registry.getExecution('exec-fin', 'sess-fin');
        expect(record!.status).toBe(status);
        expect(record!.finished_at).toBe(0);
        // The status change itself must still be recorded.
        expect(record!.timeline.map((e: any) => e.status)).toContain(status);
    });

    test('a later terminal status still stamps finished_at after a waiting hop', async () => {
        await registry.markExecutionFinished('exec-fin', 'sess-fin', AgentState.WAITING_AGENT);
        expect((await registry.getExecution('exec-fin', 'sess-fin'))!.finished_at).toBe(0);

        await registry.markExecutionFinished('exec-fin', 'sess-fin', AgentState.COMPLETED);
        expect((await registry.getExecution('exec-fin', 'sess-fin'))!.finished_at).toBeGreaterThan(0);
    });
});

describe('runner terminal-replay skip makes an exception for ResumeCommand', () => {
    // Mirrors Python worker/runner.py's
    //   `and not isinstance(command, ResumeCommand)`.
    // A caller suspended on call_agent ENDS its execution, so the record the
    // reply reattaches to legitimately sits in a terminal state. Skipping it
    // silently drops the reply — the regression that pairs with "the agent
    // return inherits the caller's message_id" (spec execution-model.md).
    test('a ResumeCommand on a terminal execution is still processed', async () => {
        const { redis, registry, runner, handled } = buildHarness();
        await registry.initializeExecution({
            execution_id: 'exec-terminal',
            message_id: 'msg-caller',
            session_id: 'sess-term',
            parent_message_id: 'msg-grandparent',
            source_agent_type: 'upstream-agent',
            status: 'QUEUED',
        });
        await registry.markExecutionFinished('exec-terminal', 'sess-term', AgentState.COMPLETED);

        await runner.processAndAck(
            QueueNames.ctrl_stream('parity-agent'),
            '1-0',
            resumeCommand({ messageId: 'msg-caller', sessionId: 'sess-term', parentMessageId: 'msg-child' })
        );

        expect(handled).toHaveLength(1);
        expect(handled[0].command).toBeInstanceOf(ResumeCommand);
        expect(redis.ackCalls).toContainEqual([QueueNames.ctrl_stream('parity-agent'), 'group-parity', '1-0']);
    });

    test('a non-resume command on a terminal execution is still skipped', async () => {
        const { redis, registry, runner, handled } = buildHarness();
        await registry.initializeExecution({
            execution_id: 'exec-terminal-2',
            message_id: 'msg-done',
            session_id: 'sess-term-2',
            status: 'QUEUED',
        });
        await registry.markExecutionFinished('exec-terminal-2', 'sess-term-2', AgentState.COMPLETED);

        const ask = new AskAgentCommand(
            new MessageHeader('msg-done', 'sess-term-2', 'trace-parity', { targetAgentType: 'parity-agent' }),
            'replayed'
        );
        await runner.processAndAck(QueueNames.ctrl_stream('parity-agent'), '2-0', ask);

        expect(handled).toHaveLength(0);
        expect(redis.ackCalls).toContainEqual([QueueNames.ctrl_stream('parity-agent'), 'group-parity', '2-0']);
    });
});

describe('runner warns when a ResumeCommand resolves to no execution', () => {
    // Mirrors the warning in Python worker/runner.py. Without it this defect
    // class is completely silent: a disconnected new execution is started and
    // the suspended caller is never continued.
    test('logs a warning naming message_id and session_id', async () => {
        const warn = jest.spyOn(console, 'warn').mockImplementation(() => undefined);
        try {
            const { runner } = buildHarness();
            await runner.processAndAck(
                QueueNames.ctrl_stream('parity-agent'),
                '3-0',
                resumeCommand({ messageId: 'msg-orphan', sessionId: 'sess-orphan' })
            );

            const message = warn.mock.calls.map((call) => String(call[0])).find((m) => m.includes('ResumeCommand'));
            expect(message).toBeDefined();
            expect(message).toContain('did not resolve to an existing execution');
            expect(message).toContain('message_id=msg-orphan');
            expect(message).toContain('session_id=sess-orphan');
            expect(message).toContain('disconnected');
        } finally {
            warn.mockRestore();
        }
    });

    test('does not warn when the ResumeCommand does resolve', async () => {
        const warn = jest.spyOn(console, 'warn').mockImplementation(() => undefined);
        try {
            const { registry, runner } = buildHarness();
            await registry.initializeExecution({
                execution_id: 'exec-resolvable',
                message_id: 'msg-resolvable',
                session_id: 'sess-resolvable',
                status: 'QUEUED',
            });

            await runner.processAndAck(
                QueueNames.ctrl_stream('parity-agent'),
                '4-0',
                resumeCommand({ messageId: 'msg-resolvable', sessionId: 'sess-resolvable' })
            );

            const message = warn.mock.calls.map((call) => String(call[0])).find((m) => m.includes('ResumeCommand'));
            expect(message).toBeUndefined();
        } finally {
            warn.mockRestore();
        }
    });
});

describe('runner forwards the execution snapshot into handleMessage', () => {
    // Mirrors Python runner.py passing
    //   execution=self._tracker.get_execution(execution_id)
    // into worker._handle_message. Without this channel the worker cannot
    // recover a resumed execution's ORIGINAL caller (the reply header
    // describes the hop that just finished instead), so worker.ts's resume
    // restore branch never runs in production.
    test('a resumed execution carries isResumed and the full dispatch record', async () => {
        const { registry, runner, handled } = buildHarness();
        await registry.initializeExecution({
            execution_id: 'exec-snap',
            message_id: 'msg-caller-snap',
            session_id: 'sess-snap',
            parent_message_id: 'msg-grandparent',
            source_agent_type: 'upstream-agent',
            target_agent_type: 'parity-agent',
            task_group_id: 'tg-original',
            status: 'QUEUED',
        });

        await runner.processAndAck(
            QueueNames.ctrl_stream('parity-agent'),
            '5-0',
            resumeCommand({
                messageId: 'msg-caller-snap',
                sessionId: 'sess-snap',
                parentMessageId: 'msg-child-snap',
                sourceAgentType: 'child-agent',
            })
        );

        expect(handled).toHaveLength(1);
        const execution = handled[0].options.execution;
        expect(execution).toBeDefined();
        expect(execution.isResumed).toBe(true);
        // parentMessageId comes from the RECORD, not the reply header.
        expect(execution.parentMessageId).toBe('msg-grandparent');
        // existingData is the whole dispatch snapshot: the only place the
        // original caller (source_agent_type / task_group_id) survives.
        expect(execution.existingData).toMatchObject({
            execution_id: 'exec-snap',
            source_agent_type: 'upstream-agent',
            task_group_id: 'tg-original',
        });
    });

    test('a first-time QUEUED dispatch is not marked resumed', async () => {
        const { registry, runner, handled } = buildHarness();
        await registry.initializeExecution({
            execution_id: 'exec-fresh',
            message_id: 'msg-fresh',
            session_id: 'sess-fresh',
            status: 'QUEUED',
        });

        const ask = new AskAgentCommand(
            new MessageHeader('msg-fresh', 'sess-fresh', 'trace-parity', { targetAgentType: 'parity-agent' }),
            'first run'
        );
        await runner.processAndAck(QueueNames.ctrl_stream('parity-agent'), '6-0', ask);

        expect(handled[0].options.execution.isResumed).toBe(false);
        expect(handled[0].options.execution.existingData).toMatchObject({ execution_id: 'exec-fresh' });
    });

    test('a non-QUEUED status marks the execution resumed even without a ResumeCommand', async () => {
        // QUEUED is the only status meaning "never been through a worker".
        // Mirrors Python runner.py's is_resumed_execution.
        const { registry, runner, handled } = buildHarness();
        await registry.initializeExecution({
            execution_id: 'exec-waiting',
            message_id: 'msg-waiting',
            session_id: 'sess-waiting',
            parent_message_id: 'msg-upstream',
            status: 'QUEUED',
        });
        await registry.updateExecutionStatus('exec-waiting', 'sess-waiting', AgentState.WAITING_AGENT);

        const ask = new AskAgentCommand(
            new MessageHeader('msg-waiting', 'sess-waiting', 'trace-parity', { targetAgentType: 'parity-agent' }),
            'redelivered'
        );
        await runner.processAndAck(QueueNames.ctrl_stream('parity-agent'), '7-0', ask);

        expect(handled[0].options.execution.isResumed).toBe(true);
    });

    test('no execution record still yields a snapshot with a null existingData', async () => {
        const warn = jest.spyOn(console, 'warn').mockImplementation(() => undefined);
        try {
            const { runner, handled } = buildHarness();
            const ask = new AskAgentCommand(
                new MessageHeader('msg-unknown', 'sess-unknown', 'trace-parity', { targetAgentType: 'parity-agent' }),
                'no record'
            );
            await runner.processAndAck(QueueNames.ctrl_stream('parity-agent'), '8-0', ask);

            expect(handled[0].options.execution).toEqual({
                parentMessageId: '',
                isResumed: false,
                existingData: null,
            });
        } finally {
            warn.mockRestore();
        }
    });
});

describe('Task Group join keys results by the sub-task id, not the caller id', () => {
    // Mirrors Python worker.py's group join. enqueueAgentReturn sets a reply's
    // header.messageId to the caller's own message id — IDENTICAL across every
    // sibling in the group. Keying results by that lets siblings overwrite each
    // other so only the last reply survives; header.parentMessageId is the
    // sub-task's own dispatch id, unique per sibling.
    async function joinSibling(
        redis: MockRedis,
        worker: AnonymousWorker,
        groupId: string,
        childMessageId: string,
        replyData: any
    ) {
        return worker.handleMessage(resumeCommand({
            messageId: 'msg-caller-group',
            sessionId: 'sess-group',
            parentMessageId: childMessageId,
            taskGroupId: groupId,
            replyData,
        }));
    }

    test('two siblings produce two distinct result entries', async () => {
        const { redis, worker } = buildHarness();
        const groupId = 'tg-siblings';
        await redis.hset(QueueNames.task_group(groupId), {
            [TASK_GROUP_FIELD_TOTAL]: '2',
            [TASK_GROUP_FIELD_COMPLETED]: '0',
        });

        const first = await joinSibling(redis, worker, groupId, 'msg-child-a', { from: 'a' });
        // A partially-joined group leaves the caller suspended, not queued —
        // the framework says so, and a sweep's triage depends on telling those
        // two apart. Mirrors Python worker.py's group-join return.
        expect(first.status).toBe(`${AgentState.WAITING_AGENT}: waiting_for_group`);

        await joinSibling(redis, worker, groupId, 'msg-child-b', { from: 'b' });

        const results = await redis.hgetall(QueueNames.task_group_results(groupId));
        expect(Object.keys(results).sort()).toEqual(['msg-child-a', 'msg-child-b']);
        expect(JSON.parse(results['msg-child-a']).reply_data).toEqual({ from: 'a' });
        expect(JSON.parse(results['msg-child-b']).reply_data).toEqual({ from: 'b' });
        expect(await redis.hget(QueueNames.task_group(groupId), TASK_GROUP_FIELD_COMPLETED)).toBe('2');
    });

    test('collectGroupResults sees one entry per sibling', async () => {
        const { redis, worker } = buildHarness();
        const groupId = 'tg-collect';
        await redis.hset(QueueNames.task_group(groupId), {
            [TASK_GROUP_FIELD_TOTAL]: '3',
            [TASK_GROUP_FIELD_COMPLETED]: '0',
        });
        for (const child of ['msg-child-1', 'msg-child-2', 'msg-child-3']) {
            await joinSibling(redis, worker, groupId, child, { child });
        }

        const context = new AgentContext(
            'sess-group',
            'trace-parity',
            redis as any,
            'parity-agent',
            'msg-caller-group'
        );
        const collected = await context.collectGroupResults(groupId, 1);
        expect(collected.map((r) => r.message_id).sort()).toEqual([
            'msg-child-1',
            'msg-child-2',
            'msg-child-3',
        ]);
    });
});

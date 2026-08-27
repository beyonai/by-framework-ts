import { WorkerRunner } from '../src/runner';
import { AnonymousWorker } from '../src/worker';
import { WorkerRegistry } from '../src/registry';
import { AgentContext } from '../src/context';
import { PluginRegistry } from '../src/extensions/registry';
import { AgentState } from '../src/protocol/agent_state';
import { AskAgentCommand, GatewayCommand, ResumeCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { AgentTaskResult } from '../src/protocol/results';
import { RoutePolicy } from '../src/availability';
import { QueueNames, TASK_GROUP_FIELD_COMPLETED, TASK_GROUP_FIELD_TOTAL } from '../src/constants';
import { MockRedis, bringAgentTypeOnline } from './helpers/mock_redis';

/**
 * A suspended caller is persisted as WAITING_AGENT / WAITING_USER, and the
 * FRAMEWORK decides that — not the business handler. Mirrors Python
 * worker.py's _apply_suspended_status. Every in-tree handler returns plain
 * QUEUED, which once persisted is indistinguishable from "still queued behind a
 * worker" — and telling those apart is what a liveness sweep's triage is.
 */

function buildWorker(
    redis: MockRedis,
    onTask: (command: GatewayCommand, context: AgentContext) => Promise<any>
): AnonymousWorker {
    return new AnonymousWorker({
        workerId: 'worker-suspend',
        agentTypes: ['caller-agent'],
        registry: new WorkerRegistry(redis as any),
        redisClient: redis as any,
        pluginRegistry: new PluginRegistry(),
        onTask,
    });
}

function askCommand(overrides: { sourceAgentType?: string } = {}): AskAgentCommand {
    return new AskAgentCommand(
        new MessageHeader('msg-caller', 'sess-suspend', 'trace-suspend', {
            sourceAgentType: overrides.sourceAgentType ?? '',
            targetAgentType: 'caller-agent',
            parentMessageId: '',
        }),
        'work',
        true
    );
}

describe('the framework overrides the handler status for a suspended execution', () => {
    test('callAgent(waitForReply) makes the handler\'s QUEUED persist as WAITING_AGENT', async () => {
        const redis = new MockRedis();
        const worker = buildWorker(redis, async (_command, context) => {
            await context.callAgent({
                targetAgentType: 'child-agent', content: 'sub',
                routePolicy: RoutePolicy.SEND_ANYWAY,
            });
            // What every in-tree handler returns after dispatching.
            return { status: AgentState.QUEUED };
        });

        const result = await worker.handleMessage(askCommand());

        expect(result.status).toBe(AgentState.WAITING_AGENT);
    });

    test('askUser makes it persist as WAITING_USER', async () => {
        const redis = new MockRedis();
        const worker = buildWorker(redis, async (_command, context) => {
            await context.askUser('what next?');
            return { status: AgentState.QUEUED };
        });

        const result = await worker.handleMessage(askCommand());

        expect(result.status).toBe(AgentState.WAITING_USER);
    });

    test('a terminal handler status wins over the suspension flag', async () => {
        // A handler that reached COMPLETED after dispatching is finished,
        // whatever it dispatched, and will never be resumed to say so later.
        const redis = new MockRedis();
        const worker = buildWorker(redis, async (_command, context) => {
            await context.callAgent({
                targetAgentType: 'child-agent', content: 'sub',
                routePolicy: RoutePolicy.SEND_ANYWAY,
            });
            return { status: AgentState.COMPLETED, replyData: { done: true } };
        });

        const result = await worker.handleMessage(askCommand());

        expect(result.status).toBe(AgentState.COMPLETED);
    });

    test.each([AgentState.FAILED, AgentState.CANCELLED])(
        'terminal status %s also wins',
        async (status) => {
            const redis = new MockRedis();
            const worker = buildWorker(redis, async (_command, context) => {
                await context.askUser('what next?');
                return new AgentTaskResult({ status });
            });
            expect((await worker.handleMessage(askCommand())).status).toBe(status);
        }
    );

    test('a handler that never suspends keeps its own status untouched', async () => {
        const redis = new MockRedis();
        const worker = buildWorker(redis, async () => ({ status: AgentState.QUEUED }));
        expect((await worker.handleMessage(askCommand())).status).toBe(AgentState.QUEUED);
    });

    test('the rest of the result survives the status rewrite', async () => {
        const redis = new MockRedis();
        const worker = buildWorker(redis, async (_command, context) => {
            await context.askUser('what next?');
            return new AgentTaskResult({
                status: AgentState.QUEUED,
                content: 'partial',
                replyData: { k: 'v' },
                metadata: { m: 1 },
                extraPayload: { e: 2 },
            });
        });

        const result = await worker.handleMessage(askCommand());

        expect(result.status).toBe(AgentState.WAITING_USER);
        expect(result.content).toBe('partial');
        expect(result.replyData).toEqual({ k: 'v' });
        expect(result.metadata).toEqual({ m: 1 });
        expect(result.extraPayload).toEqual({ e: 2 });
    });
});

describe('AgentContext.suspendedState tracks WHY, not just THAT', () => {
    test('callAgent(waitForReply) -> WAITING_AGENT', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext('s', 't', redis as any, 'caller-agent', 'msg-caller');
        await ctx.callAgent({
            targetAgentType: 'child-agent', content: 'x', routePolicy: RoutePolicy.SEND_ANYWAY,
        });
        expect(ctx.isSuspended()).toBe(true);
        expect(ctx.suspendedState()).toBe(AgentState.WAITING_AGENT);
    });

    test('dispatchGroup(wait) -> WAITING_AGENT', async () => {
        const redis = new MockRedis();
        await bringAgentTypeOnline(redis, 'a');
        const ctx = new AgentContext('s', 't', redis as any, 'caller-agent', 'msg-caller');
        await ctx.dispatchGroup({ tasks: [{ targetAgentType: 'a', content: 'x' }] });
        expect(ctx.suspendedState()).toBe(AgentState.WAITING_AGENT);
    });

    test('askUser -> WAITING_USER, which is what a sweep must never compensate', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext('s', 't', redis as any, 'caller-agent', 'msg-caller');
        await ctx.askUser('hi');
        expect(ctx.suspendedState()).toBe(AgentState.WAITING_USER);
    });

    test('a non-waiting callAgent transfers permission instead of suspending', async () => {
        const redis = new MockRedis();
        const ctx = new AgentContext('s', 't', redis as any, 'caller-agent', 'msg-caller');
        await ctx.callAgent({
            targetAgentType: 'child-agent', content: 'x',
            waitForReply: false, routePolicy: RoutePolicy.SEND_ANYWAY,
        });
        expect(ctx.isSuspended()).toBe(false);
        expect(ctx.suspendedState()).toBe('');
        expect(ctx.isPermissionTransferred()).toBe(true);
    });
});

describe('a partially-joined Task Group returns WAITING_AGENT, not QUEUED', () => {
    test('the caller is suspended on its siblings, not queued behind a worker', async () => {
        const redis = new MockRedis();
        const worker = buildWorker(redis, async () => 'ok');
        await redis.hset(QueueNames.task_group('tg-partial'), {
            [TASK_GROUP_FIELD_TOTAL]: '2',
            [TASK_GROUP_FIELD_COMPLETED]: '0',
        });

        const result = await worker.handleMessage(new ResumeCommand(
            new MessageHeader('msg-caller', 'sess-suspend', 'trace-suspend', {
                sourceAgentType: 'child-agent',
                targetAgentType: 'caller-agent',
                parentMessageId: 'msg-child-a',
                taskGroupId: 'tg-partial',
            }),
            '', AgentState.COMPLETED, { from: 'a' }
        ));

        expect(result.status).toBe(`${AgentState.WAITING_AGENT}: waiting_for_group`);
    });
});

describe('runner: WAITING_* must not be mistaken for "never claimed" (status-enumeration audit)', () => {
    /**
     * runner.processAndAck used to early-finalize any execution with
     * cancel_requested whose status was `!== 'RUNNING'`. Once a suspended caller
     * is persisted as WAITING_AGENT, that predicate swallows it: the worker never
     * runs, so the caller never replies CANCELLED to ITS caller, which stays
     * suspended in turn. Python makes no status comparison at all here — it just
     * sets the cancel event and lets the worker unwind.
     */
    function harness(): { redis: MockRedis; registry: WorkerRegistry; runner: WorkerRunner; ran: string[] } {
        const redis = new MockRedis();
        const registry = new WorkerRegistry(redis as any);
        const ran: string[] = [];
        const worker = new AnonymousWorker({
            workerId: 'worker-cancel',
            agentTypes: ['caller-agent'],
            registry,
            redisClient: redis as any,
            pluginRegistry: new PluginRegistry(),
            onTask: async (command) => { ran.push(command.header.messageId); return 'ok'; },
        });
        const runner = new WorkerRunner(worker, { redisClient: redis as any, groupName: 'g-cancel' });
        return { redis, registry, runner, ran };
    }

    async function seed(registry: WorkerRegistry, status: string): Promise<void> {
        await registry.initializeExecution({
            execution_id: 'exec-cancel',
            message_id: 'msg-cancel-target',
            session_id: 'sess-cancel',
            status: 'QUEUED',
        });
        await registry.updateExecutionStatus('exec-cancel', 'sess-cancel', status, {
            cancel_requested: true,
            cancel_reason: 'user aborted',
        });
    }

    const resume = () => new ResumeCommand(
        new MessageHeader('msg-cancel-target', 'sess-cancel', 'trace-cancel', {
            targetAgentType: 'caller-agent',
            sourceAgentType: 'child-agent',
            parentMessageId: 'msg-child',
        }),
        '', AgentState.COMPLETED, null
    );

    test.each([AgentState.WAITING_AGENT, AgentState.WAITING_USER])(
        'a %s execution with a pending cancel still reaches the worker',
        async (status) => {
            const { registry, runner, ran } = harness();
            await seed(registry, status);

            await runner.processAndAck('stream-cancel', '1-0', resume());

            // It reaches the worker, which unwinds it as CANCELLED — rather than
            // being finalized behind the worker's back.
            expect(ran).toEqual([]); // TaskCancelledError fires before the handler
            const record = await registry.getExecution('exec-cancel', 'sess-cancel');
            expect(record!.status).toBe(AgentState.CANCELLED);
            // The reply its own caller was waiting for did go out.
            expect(record!.timeline.map((e: any) => e.status)).toContain(AgentState.CANCELLED);
        }
    );

    test('a QUEUED execution with a pending cancel is still finalized without running', async () => {
        // The branch's original purpose: cancelled before any worker claimed it.
        const { registry, runner, ran } = harness();
        await seed(registry, AgentState.QUEUED);

        await runner.processAndAck('stream-cancel', '1-0', resume());

        expect(ran).toEqual([]);
        const record = await registry.getExecution('exec-cancel', 'sess-cancel');
        expect(record!.status).toBe(AgentState.CANCELLED);
        // Never promoted to RUNNING: no worker ever claimed it.
        expect(record!.timeline.map((e: any) => e.status)).not.toContain('RUNNING');
    });

    test('a RUNNING execution with a pending cancel is unchanged by this fix', async () => {
        const { registry, runner } = harness();
        await seed(registry, 'RUNNING');

        await runner.processAndAck('stream-cancel', '1-0', resume());

        const record = await registry.getExecution('exec-cancel', 'sess-cancel');
        expect(record!.status).toBe(AgentState.CANCELLED);
        expect(record!.timeline.map((e: any) => e.status)).toContain('RUNNING');
    });

    test('WAITING_* is treated as "already been through a worker" for resume identity', async () => {
        // The is_resumed inference keys off "status !== QUEUED"; WAITING_* must
        // land on the resumed side or the record's identity is re-derived from
        // the (wrong) message header.
        const redis = new MockRedis();
        const registry = new WorkerRegistry(redis as any);
        const seen: any[] = [];
        const worker = new AnonymousWorker({
            workerId: 'worker-resumed',
            agentTypes: ['caller-agent'],
            registry,
            redisClient: redis as any,
            pluginRegistry: new PluginRegistry(),
            onTask: async () => 'ok',
        });
        jest.spyOn(worker, 'handleMessage').mockImplementation(async (_c: any, o: any = {}) => {
            seen.push(o.execution);
            return new AgentTaskResult({ status: AgentState.COMPLETED });
        });
        const runner = new WorkerRunner(worker, { redisClient: redis as any, groupName: 'g-resumed' });

        await registry.initializeExecution({
            execution_id: 'exec-w', message_id: 'msg-w', session_id: 'sess-w',
            parent_message_id: 'msg-original-caller', status: 'QUEUED',
        });
        await registry.updateExecutionStatus('exec-w', 'sess-w', AgentState.WAITING_AGENT);

        await runner.processAndAck('stream-w', '1-0', new AskAgentCommand(
            new MessageHeader('msg-w', 'sess-w', 'trace-w', { targetAgentType: 'caller-agent' }),
            'work'
        ));

        expect(seen[0].isResumed).toBe(true);
        expect(seen[0].parentMessageId).toBe('msg-original-caller');
        expect(seen[0].existingData.status).toBe(AgentState.WAITING_AGENT);
        jest.restoreAllMocks();
    });
});

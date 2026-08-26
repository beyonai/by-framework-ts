import { GatewayClient } from '../src/client';
import { WorkerRunner } from '../src/runner';
import { AnonymousWorker } from '../src/worker';
import { WorkerRegistry } from '../src/registry';
import { AgentContext } from '../src/context';
import { PluginRegistry } from '../src/extensions/registry';
import { ActionType } from '../src/protocol/action_type';
import { AgentState } from '../src/protocol/agent_state';
import { GatewayCommand, commandFromDict } from '../src/protocol/commands';
import { CLIENT_SOURCE_AGENT_TYPE, QueueNames } from '../src/constants';
import { MockRedis, asRedis, bringAgentTypeOnline } from './helpers/mock_redis';

/**
 * A RESUME sent through GatewayClient must REATTACH to the suspended execution,
 * never re-initialize it.
 *
 * initializeExecution() rewrites `msg_map:<messageId>` to point at whatever
 * execution_id it was handed. Minting a fresh one for a resume therefore
 * detaches the resume from the execution it is meant to continue: the record
 * the worker then reads back carries neither `source_agent_type` nor
 * `metadata`, so the callee replies to nobody and loses its caller's dispatch
 * metadata on the way.
 *
 * This is the highest-frequency defect class in the Python SDK's history
 * (90764e1, closes #75/#76/#77) and it shipped here too, because every other
 * resume test in this repo builds a ResumeCommand by hand and feeds it straight
 * to `runner.processAndAck` — bypassing the client, and with it the only code
 * path a real askUser answer takes.
 */

const SESSION = 'sess-reattach';
const AGENT = 'agent-b';

async function suspendedExecution(registry: WorkerRegistry, metadata: Record<string, unknown>) {
    // What A's dispatch wrote for B, after B suspended on askUser.
    await registry.initializeExecution({
        execution_id: 'exec-b-original',
        message_id: 'msg-b',
        session_id: SESSION,
        trace_id: 'trace-reattach',
        parent_message_id: 'msg-a',
        source_agent_type: 'agent-a',
        target_agent_type: AGENT,
        task_group_id: '',
        metadata,
        status: AgentState.WAITING_USER,
    });
}

function newClient(redis: MockRedis, registry: WorkerRegistry): GatewayClient {
    const client = new GatewayClient(registry, asRedis(redis));
    (client as any).redis = asRedis(redis);
    return client;
}

describe('a RESUME sent through GatewayClient reattaches instead of re-initializing', () => {
    it('reuses the suspended execution_id and leaves its record intact', async () => {
        const redis = new MockRedis();
        const registry = new WorkerRegistry(asRedis(redis));
        await bringAgentTypeOnline(redis, AGENT, 'worker-b');
        await suspendedExecution(registry, { tenant: 'acme', tag: 'from-dispatch' });

        await newClient(redis, registry).sendMessage({
            targetAgentType: AGENT,
            sessionId: SESSION,
            content: 'Pink',
            actionType: ActionType.RESUME,
            messageId: 'msg-b',
            metadata: { client_tag: 'this-hop-only' },
        });

        const record = await registry.getExecutionByMessageId('msg-b', SESSION);
        // Same execution, not a fresh one hiding the original.
        expect(record?.execution_id).toBe('exec-b-original');
        // And the two fields the resume path reads back are still there.
        expect(record?.source_agent_type).toBe('agent-a');
        expect(record?.metadata).toEqual({ tenant: 'acme', tag: 'from-dispatch' });
    });

    it('still mints a fresh execution when nothing matches the message id', async () => {
        // A genuinely new resume-shaped message: fall back to today's behaviour
        // rather than dropping the record on the floor.
        const redis = new MockRedis();
        const registry = new WorkerRegistry(asRedis(redis));
        await bringAgentTypeOnline(redis, AGENT, 'worker-b');

        await newClient(redis, registry).sendMessage({
            targetAgentType: AGENT,
            sessionId: SESSION,
            content: 'Pink',
            actionType: ActionType.RESUME,
            messageId: 'msg-unmatched',
        });

        const record = await registry.getExecutionByMessageId('msg-unmatched', SESSION);
        expect(record).toBeTruthy();
        expect(record?.execution_id).toMatch(/^exec-/);
    });

    it('falls back when the registry has no lookup method at all', async () => {
        // Registry doubles and older implementations must not throw here.
        const redis = new MockRedis();
        const registry = new WorkerRegistry(asRedis(redis));
        await bringAgentTypeOnline(redis, AGENT, 'worker-b');
        const crippled = Object.create(registry);
        crippled.getExecutionByMessageId = undefined;

        const response = await newClient(redis, crippled as WorkerRegistry).sendMessage({
            targetAgentType: AGENT,
            sessionId: SESSION,
            content: 'Pink',
            actionType: ActionType.RESUME,
            messageId: 'msg-b',
        });

        expect(response.success).toBe(true);
    });

    it('an ASK_AGENT dispatch is untouched by the guard', async () => {
        const redis = new MockRedis();
        const registry = new WorkerRegistry(asRedis(redis));
        await bringAgentTypeOnline(redis, AGENT, 'worker-b');

        await newClient(redis, registry).sendMessage({
            targetAgentType: AGENT,
            sessionId: SESSION,
            content: 'hello',
            actionType: ActionType.ASK_AGENT,
            messageId: 'msg-fresh',
            metadata: { tenant: 'acme' },
        });

        const record = await registry.getExecutionByMessageId('msg-fresh', SESSION);
        expect(record?.source_agent_type).toBe(CLIENT_SOURCE_AGENT_TYPE);
        expect(record?.status).toBe('QUEUED');
    });
});

describe('the full askUser round-trip through the client', () => {
    it('lets B reply to A with A\'s metadata after a client-sent answer', async () => {
        // The end-to-end shape the fixture never exercised: the answer goes out
        // through GatewayClient, not a hand-built ResumeCommand. Everything the
        // resume path restores — the caller's identity AND its metadata — has to
        // survive that trip.
        const redis = new MockRedis();
        const registry = new WorkerRegistry(asRedis(redis));
        await bringAgentTypeOnline(redis, AGENT, 'worker-b');
        await suspendedExecution(registry, { caller: 'agent-a', tag: 'keep' });

        const seen: Record<string, unknown>[] = [];
        const worker = new AnonymousWorker({
            workerId: 'worker-b',
            agentTypes: [AGENT],
            registry,
            redisClient: asRedis(redis),
            pluginRegistry: new PluginRegistry(),
            onTask: async (command: GatewayCommand, _context: AgentContext) => {
                seen.push({ ...command.header.metadata });
                return {
                    status: AgentState.COMPLETED,
                    reply_data: { from: AGENT },
                    metadata: { agent: AGENT, tag: 'from-agent-b' },
                };
            },
        });
        const runner = new WorkerRunner(worker, {
            redisClient: asRedis(redis),
            groupName: 'g-b',
        });

        await newClient(redis, registry).sendMessage({
            targetAgentType: AGENT,
            sessionId: SESSION,
            content: 'Pink',
            actionType: ActionType.RESUME,
            messageId: 'msg-b',
            metadata: { client_tag: 'should-not-leak' },
        });

        const inbox = redis.getStreamPayloads(QueueNames.ctrl_stream(AGENT));
        expect(inbox).toHaveLength(1);
        await runner.processAndAck(QueueNames.ctrl_stream(AGENT), '1-0', commandFromDict(inbox[0]));

        // Inbound: B reads A's dispatch metadata again, with the answering
        // client's own metadata layered on top.
        expect(seen).toHaveLength(1);
        expect(seen[0].caller).toBe('agent-a');
        expect(seen[0].tag).toBe('keep');
        expect(seen[0].client_tag).toBe('should-not-leak');

        // Outbound: B replies to A at all (it would not have, with the record
        // detached), carrying A's metadata and NOT the answering client's.
        const replies = redis
            .getStreamPayloads(QueueNames.ctrl_stream('agent-a'))
            .map((p) => commandFromDict(p));
        expect(replies).toHaveLength(1);
        expect(replies[0].header.messageId).toBe('msg-a');
        expect(replies[0].header.metadata.caller).toBe('agent-a');
        expect(replies[0].header.metadata.tag).toBe('from-agent-b');
        expect(replies[0].header.metadata).not.toHaveProperty('client_tag');
    });
});

import { GatewayWorker } from '../src/worker';
import { AgentContext } from '../src/context';
import { AskAgentCommand, GatewayCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';
import { getActiveWorkspace, setActiveWorkspace } from '../src/sandbox';

class MockRedis {
    async xadd(_stream: string, _id: string, _field: string, _payload: string): Promise<string> {
        return '1-0';
    }

    pipeline() {
        const self = this;
        const pipe = {
            xadd: (stream: string, id: string, field: string, payload: string) => {
                self.xadd(stream, id, field, payload);
                return pipe;
            },
            expire: (key: string, seconds: number) => {
                return pipe;
            },
            exec: async () => {
                return [];
            }
        };
        return pipe;
    }
}

class WorkspaceAwareWorker extends GatewayWorker {
    constructor(
        private readonly behavior: 'success' | 'fail',
        registry: any,
        redisClient: any,
        workspaceManager: any,
        sandbox: any
    ) {
        super('workspace-worker', registry, redisClient, undefined, workspaceManager, sandbox);
    }

    getAgentTypes(): string[] {
        return ['workspace-agent'];
    }

    async processCommand(_command: GatewayCommand, _context: AgentContext): Promise<any> {
        if (this.behavior === 'fail') {
            throw new Error('boom');
        }
        return { ok: true };
    }
}

describe('GatewayWorker workspace/sandbox lifecycle', () => {
    const makeCommand = () =>
        new AskAgentCommand(
            new MessageHeader('msg-1', 'sess-1', 'trace-1', {
                targetAgentType: 'workspace-agent',
            }),
            'hello'
        );

    test('installs sandbox, sets active workspace, and restores/cleans up on success', async () => {
        const workspaceManager = {
            setupWorkspace: jest.fn().mockResolvedValue({
                private: '/tmp/ws/sess-1/private/msg-1',
            }),
            cleanupTask: jest.fn().mockResolvedValue(undefined),
        };
        const sandbox = {
            install: jest.fn(),
            uninstall: jest.fn(),
        };
        const worker = new WorkspaceAwareWorker('success', {}, new MockRedis() as any, workspaceManager, sandbox);

        setActiveWorkspace('/tmp/ws/previous');
        const result = await worker.handleMessage(makeCommand());

        expect(result.status).toBe('COMPLETED');
        expect(workspaceManager.setupWorkspace).toHaveBeenCalledWith('sess-1', 'msg-1');
        expect(sandbox.install).toHaveBeenCalledTimes(1);
        expect(sandbox.uninstall).toHaveBeenCalledTimes(1);
        expect(workspaceManager.cleanupTask).toHaveBeenCalledWith('sess-1', 'msg-1');
        expect(getActiveWorkspace()).toBe('/tmp/ws/previous');
    });

    test('still restores and cleans up when process fails', async () => {
        const workspaceManager = {
            setupWorkspace: jest.fn().mockResolvedValue({
                private: '/tmp/ws/sess-1/private/msg-1',
            }),
            cleanupTask: jest.fn().mockResolvedValue(undefined),
        };
        const sandbox = {
            install: jest.fn(),
            uninstall: jest.fn(),
        };
        const worker = new WorkspaceAwareWorker('fail', {}, new MockRedis() as any, workspaceManager, sandbox);

        setActiveWorkspace('/tmp/ws/previous-fail');
        const result = await worker.handleMessage(makeCommand());

        expect(result.status).toBe('FAILED');
        expect(workspaceManager.setupWorkspace).toHaveBeenCalledWith('sess-1', 'msg-1');
        expect(sandbox.install).toHaveBeenCalledTimes(1);
        expect(sandbox.uninstall).toHaveBeenCalledTimes(1);
        expect(workspaceManager.cleanupTask).toHaveBeenCalledWith('sess-1', 'msg-1');
        expect(getActiveWorkspace()).toBe('/tmp/ws/previous-fail');
    });
});

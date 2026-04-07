import {
    AgentContext,
    BaseCommand,
    GatewayClient,
    GatewayWorker,
    MessageHeader,
    WorkerRunner,
    createRedis,
    registerCommand,
} from '../src';

class CustomAuditCommand extends BaseCommand {
    static actionType = 'CUSTOM_AUDIT';
    readonly actionType = CustomAuditCommand.actionType;

    constructor(
        header: MessageHeader,
        public readonly auditId: string,
        public readonly payload: Record<string, any>
    ) {
        super(header);
    }

    toDict(): Record<string, any> {
        return {
            action_type: this.actionType,
            header: this.header.toDict(),
            body: {
                audit_id: this.auditId,
                payload: this.payload,
            },
        };
    }

    static fromDict(data: Record<string, any>): CustomAuditCommand {
        return new CustomAuditCommand(
            MessageHeader.fromDict(data.header),
            String(data.body?.audit_id || ''),
            { ...(data.body?.payload || {}) }
        );
    }
}

registerCommand(CustomAuditCommand);

class CustomAuditWorker extends GatewayWorker {
    getAgentTypes(): string[] {
        return ['custom-audit-agent-ts'];
    }

    async processCommand(command: BaseCommand, context: AgentContext): Promise<any> {
        if (command instanceof CustomAuditCommand) {
            await context.emitState({ state: `AUDIT:${command.auditId}` });
            await context.emitChunk({
                content: `Handled custom audit payload: ${JSON.stringify(command.payload)}`,
            });
            return { ok: true, auditId: command.auditId };
        }

        throw new Error(`Unsupported command: ${command.actionType}`);
    }
}

async function sendCustomCommand(): Promise<void> {
    const redis = createRedis({
        host: 'localhost',
        port: 6379,
        db: 0,
        username: process.env.REDIS_USERNAME,
        password: process.env.REDIS_PASSWORD,
    });
    const client = new GatewayClient(undefined, redis);

    const command = new CustomAuditCommand(
        new MessageHeader('msg-custom-audit-1', 'sess-custom-audit', 'trace-custom-audit', {
            targetAgentType: 'custom-audit-agent-ts',
            tenantId: 'demo-tenant',
            metadata: { source: 'ts-example' },
        }),
        'audit-001',
        { action: 'reindex', priority: 'high' }
    );

    const response = await client.sendCommand(command);
    console.log('Custom command queued:', response);
    await redis.quit();
}

async function startWorker(): Promise<void> {
    const worker = new CustomAuditWorker('custom-audit-worker-ts');
    const runner = new WorkerRunner(worker);
    await runner.start({ handleSignals: true });
}

void (async () => {
    const mode = process.argv[2];
    if (mode === 'worker') {
        await startWorker();
        return;
    }
    await sendCustomCommand();
})();

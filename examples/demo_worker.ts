import {
    GatewayWorker,
    AskAgentCommand,
    AgentContext,
    WorkerRunner,
} from '../src';

class DemoWorker extends GatewayWorker {
    getCapabilities(): string[] {
        return ['demo-agent-ts'];
    }

    async processCommand(command: AskAgentCommand, context: AgentContext): Promise<any> {
        console.log(`[${this.workerId}] Processing message: ${command.content}`);

        // Discovery Demo
        const activeWorkers = await context.getActiveWorkers();
        console.log(`[${this.workerId}] Active workers in cluster: ${Object.keys(activeWorkers).join(', ')}`);

        const text = `Echo from TypeScript SDK: ${command.content}. I am processing your request.`;

        for (const char of text) {
            await context.emitChunk({ content: char });
            await new Promise((resolve) => setTimeout(resolve, 50));
        }

        return {
            status: 'done',
            reply: 'Message processed by TS SDK',
        };
    }
}

async function main() {
    const worker = new DemoWorker('worker-ts-01');
    const runner = new WorkerRunner(worker);

    await runner.start({ handleSignals: true });
}

main().catch(console.error);

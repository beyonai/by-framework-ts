import { GatewayWorker } from './worker';
import { GatewayCommand, AskAgentCommand, ResumeCommand } from './protocol/commands';
import { ByaiAskAgentCommand, ByaiResumeCommand } from './protocol/byai_command';
import { ByaiContentCodec } from './protocol/byai_codec';
import { ContentCodec } from './protocol/content_codec';
import { AgentContext } from './context';
import { ByaiAgentContext } from './byai_context';

/** GatewayWorker variant that decodes Byai message payloads for business logic. */
export abstract class ByaiWorker extends GatewayWorker {
    protected getContextClass(): typeof AgentContext {
        return ByaiAgentContext;
    }

    protected getContentCodec(): ContentCodec | undefined {
        return new ByaiContentCodec();
    }

    protected prepareCommandForProcessing(command: GatewayCommand): GatewayCommand {
        if (!('content' in command)) {
            return command;
        }

        const codec = this.getContentCodec();
        if (!codec) {
            return command;
        }

        const decodedContent = codec.deserialize((command as AskAgentCommand | ResumeCommand).content as any);

        if (command instanceof AskAgentCommand) {
            return new ByaiAskAgentCommand(command.header, decodedContent, command.waitForReply, command.extraPayload);
        }
        if (command instanceof ResumeCommand) {
            return new ByaiResumeCommand(command.header, decodedContent, command.status, command.replyData, command.extraPayload);
        }
        return command;
    }
}

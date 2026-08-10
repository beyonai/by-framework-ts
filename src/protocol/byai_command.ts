import { AskAgentCommand, ResumeCommand } from './commands';
import { serializeByaiContent } from './byai_codec';

/** AskAgentCommand whose content is serialized back to wire form via the Byai codec. */
export class ByaiAskAgentCommand extends AskAgentCommand {
    toDict(): Readonly<Record<string, unknown>> {
        const body: Record<string, unknown> = {
            content: serializeByaiContent(this.content),
            wait_for_reply: this.waitForReply,
        };
        if (Object.keys(this.extraPayload).length > 0) {
            body.extra_payload = { ...this.extraPayload };
        }
        return {
            action_type: this.actionType,
            header: this.header.toDict(),
            body,
        };
    }
}

/** ResumeCommand whose content is serialized back to wire form via the Byai codec. */
export class ByaiResumeCommand extends ResumeCommand {
    toDict(): Readonly<Record<string, unknown>> {
        const body: Record<string, unknown> = {
            content: serializeByaiContent(this.content),
            status: this.status,
            reply_data: this.replyData,
        };
        if (Object.keys(this.extraPayload).length > 0) {
            body.extra_payload = { ...this.extraPayload };
        }
        return {
            action_type: this.actionType,
            header: this.header.toDict(),
            body,
        };
    }
}

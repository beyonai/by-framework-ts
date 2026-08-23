import { AgentContext } from '../src/context';

describe('AgentContext cancellation signal', () => {
    test('exposes the runner-owned abort signal to nested worker operations', () => {
        const abortController = new AbortController();
        const context = new AgentContext(
            'session-cancel',
            'trace-cancel',
            {} as any,
            'agent-cancel',
            'message-cancel',
            undefined,
            abortController.signal
        );

        expect(context.getCancellationSignal()).toBe(abortController.signal);
        abortController.abort('cancelled');
        expect(context.isCancelRequested()).toBe(true);
    });
});

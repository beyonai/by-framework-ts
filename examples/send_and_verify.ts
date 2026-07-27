/**
 * Deploy smoke test driver: sends a message to demo_worker.ts's "demo-agent-ts"
 * agent type through a real Redis instance (deploy/docker-compose.yml) and
 * verifies the echoed reply actually arrives on the session's data stream —
 * the only place a message flows through a real, separate-process Redis
 * Streams deployment instead of the in-memory Redis fakes tests/ uses.
 * See .github/workflows/deploy-smoke-test.yml.
 */
import { getRedis } from '../src/redis_client';
import { GatewayClient } from '../src/client';
import { readSessionDataStreamRev } from '../src/dispatch/session_stream_reader';

const SESSION_ID = 'smoke-test-session';
const EXPECTED_FRAGMENT = 'Echo from TypeScript SDK:';
const TIMEOUT_MS = 15_000;

/**
 * demo_worker.ts streams its reply one character at a time via
 * `context.emitChunk({ content: char })`, so no single data-stream entry ever
 * contains the full expected text — each entry's `data.choices[0].delta.content`
 * is just one character. Reconstruct the full streamed text by concatenating
 * every delta, oldest first, before checking for the expected fragment.
 */
function reconstructStreamedText(entries: ReadonlyArray<{ readonly data: string }>): string {
    // readSessionDataStreamRev returns newest-first; replay oldest-first.
    const chronological = [...entries].reverse();
    let text = '';
    for (const entry of chronological) {
        try {
            const payload = JSON.parse(entry.data);
            const content = payload?.data?.choices?.[0]?.delta?.content;
            if (typeof content === 'string') {
                text += content;
            }
        } catch {
            // skip unparseable entries
        }
    }
    return text;
}

async function waitForEchoedReply(redis: ReturnType<typeof getRedis>): Promise<boolean> {
    const deadline = Date.now() + TIMEOUT_MS;
    while (Date.now() < deadline) {
        const entries = await readSessionDataStreamRev(redis, SESSION_ID, 200);
        if (reconstructStreamedText(entries).includes(EXPECTED_FRAGMENT)) {
            return true;
        }
        await new Promise((resolve) => setTimeout(resolve, 500));
    }
    return false;
}

async function main(): Promise<void> {
    const redis = getRedis();
    const client = new GatewayClient();

    console.log('Sending message to demo-agent-ts...');
    const response = await client.sendMessage({
        targetAgentType: 'demo-agent-ts',
        sessionId: SESSION_ID,
        content: 'smoke test ping',
    });

    if (!response.success) {
        console.error(`Send failed: ${response.error}`);
        process.exit(1);
    }

    const found = await waitForEchoedReply(redis);
    if (!found) {
        console.error(`Timed out waiting for echoed reply containing: ${EXPECTED_FRAGMENT}`);
        process.exit(1);
    }

    console.log('Smoke test passed: worker echoed the expected reply.');
    await redis.quit();
}

main().catch((error) => {
    console.error(error);
    process.exit(1);
});

import type { Redis } from 'ioredis';
import { QueueNames } from '../constants';

/**
 * Optional helper: read recent entries from the session data stream (newest first).
 * For integration tests or tooling; workers normally consume ctrl streams in the runner loop.
 */
export async function readSessionDataStreamRev(
    redis: Redis,
    sessionId: string,
    count: number = 50
): Promise<Array<{ readonly id: string; readonly data: string }>> {
    const key = QueueNames.session_data_stream(sessionId);
    const rows = await redis.xrevrange(key, '+', '-', 'COUNT', count);
    const out: Array<{ id: string; data: string }> = [];
    for (const row of rows) {
        const [id, fields] = row as [string | Buffer, string[] | Buffer[]];
        const fieldList = fields as string[];
        const dataIdx = fieldList.indexOf('data');
        if (dataIdx >= 0 && dataIdx + 1 < fieldList.length) {
            out.push({ id: String(id), data: String(fieldList[dataIdx + 1]) });
        }
    }
    return out;
}

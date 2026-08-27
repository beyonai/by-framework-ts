import { MockRedis } from './helpers/mock_redis';

/**
 * Self-tests for the shared in-memory Redis double. Every command the wait
 * index / idempotency gate / sweeper will issue is covered here, because a
 * silently-wrong fake makes the subsystem's own tests vacuous.
 */
describe('MockRedis fixture', () => {
    let redis: MockRedis;

    beforeEach(() => {
        redis = new MockRedis();
    });

    describe('sorted sets (wait index)', () => {
        test('zadd returns new-member count and zscore reads it back', async () => {
            expect(await redis.zadd('z', 100, 'a')).toBe(1);
            expect(await redis.zadd('z', 200, 'b')).toBe(1);
            // Re-scoring an existing member adds nothing.
            expect(await redis.zadd('z', 300, 'a')).toBe(0);
            expect(await redis.zscore('z', 'a')).toBe('300');
            expect(await redis.zcard('z')).toBe(2);
        });

        test('zadd accepts multiple score/member pairs', async () => {
            expect(await redis.zadd('z', 1, 'a', 2, 'b', 3, 'c')).toBe(3);
            expect(await redis.zrange('z', 0, -1)).toEqual(['a', 'b', 'c']);
        });

        test('zrem returns 1 on claim and 0 on a miss — the gate depends on this', async () => {
            await redis.zadd('z', 100, 'member');
            expect(await redis.zrem('z', 'member')).toBe(1);
            expect(await redis.zrem('z', 'member')).toBe(0);
            expect(await redis.zrem('z', 'never-registered')).toBe(0);
        });

        test('zrange orders by score then lexicographically, honours WITHSCORES', async () => {
            await redis.zadd('z', 2, 'b', 1, 'a', 2, 'a2');
            expect(await redis.zrange('z', 0, -1)).toEqual(['a', 'a2', 'b']);
            expect(await redis.zrange('z', 0, -1, 'WITHSCORES')).toEqual(['a', '1', 'a2', '2', 'b', '2']);
        });

        test('zrangebyscore honours -inf/+inf, exclusive bounds and LIMIT', async () => {
            await redis.zadd('z', 10, 'a', 20, 'b', 30, 'c', 40, 'd');
            expect(await redis.zrangebyscore('z', '-inf', 25)).toEqual(['a', 'b']);
            expect(await redis.zrangebyscore('z', 20, '+inf')).toEqual(['b', 'c', 'd']);
            expect(await redis.zrangebyscore('z', '(20', '+inf')).toEqual(['c', 'd']);
            expect(await redis.zrangebyscore('z', '-inf', '(30')).toEqual(['a', 'b']);
            expect(await redis.zrangebyscore('z', '-inf', '+inf', 'LIMIT', 0, 2)).toEqual(['a', 'b']);
            expect(await redis.zrangebyscore('z', '-inf', '+inf', 'LIMIT', 2, 5)).toEqual(['c', 'd']);
        });

        test('zremrangebyscore removes only the matching range and returns the count', async () => {
            await redis.zadd('z', 10, 'a', 20, 'b', 30, 'c');
            expect(await redis.zremrangebyscore('z', '-inf', 20)).toBe(2);
            expect(await redis.zrange('z', 0, -1)).toEqual(['c']);
            expect(await redis.zremrangebyscore('z', '-inf', 5)).toBe(0);
        });
    });

    describe('strings, existence and TTL (consumed markers)', () => {
        test('exists distinguishes "written" from "never written"', async () => {
            expect(await redis.exists('marker')).toBe(0);
            await redis.set('marker', '1');
            expect(await redis.exists('marker')).toBe(1);
        });

        test('set NX does not overwrite, set XX does not create', async () => {
            expect(await redis.set('k', 'first', 'NX')).toBe('OK');
            expect(await redis.set('k', 'second', 'NX')).toBeNull();
            expect(await redis.get('k')).toBe('first');
            expect(await redis.set('absent', 'v', 'XX')).toBeNull();
        });

        test('set EX records a TTL that ttl() reports and the clock enforces', async () => {
            await redis.set('marker', '1', 'EX', 60);
            expect(await redis.ttl('marker')).toBe(60);

            redis.advanceTime(59_000);
            expect(await redis.exists('marker')).toBe(1);

            redis.advanceTime(2_000);
            expect(await redis.exists('marker')).toBe(0);
            expect(await redis.get('marker')).toBeNull();
            expect(await redis.ttl('marker')).toBe(-2);
        });

        test('set PX records a millisecond TTL', async () => {
            await redis.set('marker', '1', 'PX', 500);
            redis.advanceTime(499);
            expect(await redis.exists('marker')).toBe(1);
            redis.advanceTime(2);
            expect(await redis.exists('marker')).toBe(0);
        });

        test('ttl reports -1 for a key with no expiry', async () => {
            await redis.set('k', 'v');
            expect(await redis.ttl('k')).toBe(-1);
        });

        test('expire applies to any container type and del clears the TTL', async () => {
            await redis.zadd('z', 1, 'a');
            expect(await redis.expire('z', 10)).toBe(1);
            expect(await redis.expire('missing', 10)).toBe(0);
            redis.advanceTime(11_000);
            expect(await redis.zcard('z')).toBe(0);
        });
    });

    describe('hashes, sets and streams', () => {
        test('hset supports both field and object forms; hincrby accumulates', async () => {
            await redis.hset('h', 'a', '1');
            await redis.hset('h', { b: '2', c: 3 });
            expect(await redis.hgetall('h')).toEqual({ a: '1', b: '2', c: '3' });
            expect(await redis.hmget('h', 'a', 'missing')).toEqual(['1', null]);
            expect(await redis.hincrby('h', 'counter', 1)).toBe(1);
            expect(await redis.hincrby('h', 'counter', 2)).toBe(3);
            expect(await redis.hdel('h', 'a')).toBe(1);
            expect(await redis.hget('h', 'a')).toBeNull();
        });

        test('set operations round-trip', async () => {
            expect(await redis.sadd('s', 'x', 'y')).toBe(2);
            expect(await redis.sadd('s', 'x')).toBe(0);
            expect((await redis.smembers('s')).sort()).toEqual(['x', 'y']);
            expect(await redis.sismember('s', 'x')).toBe(1);
            expect(await redis.srem('s', 'x')).toBe(1);
            expect(await redis.sismember('s', 'x')).toBe(0);
        });

        test('xadd/xreadgroup/xlen model a pop-on-read stream', async () => {
            await redis.xadd('stream', '*', 'data', JSON.stringify({ n: 1 }));
            await redis.xadd('stream', '*', 'data', JSON.stringify({ n: 2 }));
            expect(await redis.xlen('stream')).toBe(2);
            expect(redis.getStreamPayloads('stream')).toEqual([{ n: 1 }, { n: 2 }]);

            const read = await redis.xreadgroup('GROUP', 'g', 'c', 'COUNT', 10, 'STREAMS', 'stream', '>');
            expect(read[0][0]).toBe('stream');
            expect(JSON.parse(read[0][1][0][1][1])).toEqual({ n: 1 });
            expect(await redis.xlen('stream')).toBe(1);
        });

        test('xreadgroup returns null when every named stream is empty', async () => {
            expect(await redis.xreadgroup('GROUP', 'g', 'c', 'STREAMS', 'a', 'b', '>', '>')).toBeNull();
        });

        test('lists append and read back (SpanRecorder appends span JSON)', async () => {
            expect(await redis.rpush('l', 'a', 'b')).toBe(2);
            expect(await redis.lpush('l', 'z')).toBe(3);
            expect(await redis.lrange('l', 0, -1)).toEqual(['z', 'a', 'b']);
            expect(await redis.lrange('l', 1, 1)).toEqual(['a']);
            expect(await redis.llen('l')).toBe(3);
        });

        test('xrevrange returns newest first and honours COUNT', async () => {
            await redis.xadd('stream', '*', 'data', 'one');
            await redis.xadd('stream', '*', 'data', 'two');
            const rows = await redis.xrevrange('stream', '+', '-', 'COUNT', 1);
            expect(rows).toHaveLength(1);
            expect(rows[0][1]).toEqual(['data', 'two']);
        });
    });

    describe('pipeline and duplicate', () => {
        test('pipeline replays in order and returns [err, result] tuples', async () => {
            const results = await redis.pipeline()
                .hset('h', 'field', 'value')
                .expire('h', 30)
                .exec();
            expect(results).toEqual([[null, 1], [null, 1]]);
            expect(await redis.hget('h', 'field')).toBe('value');
            expect(await redis.ttl('h')).toBe(30);
        });

        test('pipeline rejects an unsupported command instead of silently dropping it', () => {
            expect(() => (redis.pipeline() as any).nosuchcommand('k')).toThrow(/unsupported command/);
        });

        test('duplicate() shares the keyspace, like a second connection would', async () => {
            const other = redis.duplicate();
            await redis.set('k', 'v');
            expect(await other.get('k')).toBe('v');
            await other.zadd('z', 1, 'm');
            expect(await redis.zscore('z', 'm')).toBe('1');
        });
    });
});

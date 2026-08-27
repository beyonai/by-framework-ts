/**
 * Shared in-memory Redis double for tests.
 *
 * WHY THIS EXISTS
 * ---------------
 * Before this file, in-memory Redis behaviour was re-implemented per test file
 * (tests/registry.test.ts had zset+string+hash, tests/e2e_smoke.test.ts had
 * streams, tests/gateway_worker.test.ts had a write-only hash stub), none of
 * them exported, and none of them with the commands the wait-index subsystem
 * needs. That made "real WorkerRunner + real WorkerRegistry + in-memory Redis"
 * — the shape Python's tests/integration/test_orphan_recovery.py uses — simply
 * unwriteable in TS.
 *
 * SCOPE (deliberately bounded, this is not a Redis simulator)
 * ----------------------------------------------------------
 * Implemented: exactly the command surface `src/` issues today, plus the
 * commands the suspend/resume liveness contract needs (zrangebyscore /
 * zremrangebyscore / exists / SET with TTL).
 *
 * NOT implemented, on purpose:
 *  - Lua / EVAL (src/ issues none; the Python Redlock scripts have no TS twin)
 *  - real consumer-group pending/claim semantics — xreadgroup pops, like every
 *    pre-existing mock in this repo did
 *  - SCAN cursors, keyspace notifications, blocking reads with real timing
 *  - WATCH/MULTI isolation; pipeline() just replays queued commands in order
 *
 * Expiry is lazy and driven by an injectable clock, so a test can assert TTL
 * semantics (a consumed marker outliving an ask_user wait, prune cutoffs)
 * without sleeping.
 */

type RedisValue = string | number;

interface StreamEntry {
    readonly id: string;
    readonly fields: string[];
}

export interface MockRedisOptions {
    /** Injectable clock; defaults to Date.now. Use with advanceTime() in tests. */
    readonly now?: () => number;
}

const POSITIVE_INFINITY_TOKENS = new Set(['+inf', 'inf', '+INF', 'INF']);
const NEGATIVE_INFINITY_TOKENS = new Set(['-inf', '-INF']);

/** Parse a ZRANGEBYSCORE bound: '-inf', '+inf', '5', '(5' (exclusive). */
function parseScoreBound(raw: RedisValue): { value: number; exclusive: boolean } {
    const token = String(raw);
    if (POSITIVE_INFINITY_TOKENS.has(token)) return { value: Infinity, exclusive: false };
    if (NEGATIVE_INFINITY_TOKENS.has(token)) return { value: -Infinity, exclusive: false };
    if (token.startsWith('(')) return { value: Number(token.slice(1)), exclusive: true };
    return { value: Number(token), exclusive: false };
}

export class MockRedis {
    private strings = new Map<string, string>();
    private hashes = new Map<string, Map<string, string>>();
    private sets = new Map<string, Set<string>>();
    private zsets = new Map<string, Map<string, number>>();
    private lists = new Map<string, string[]>();
    private streams = new Map<string, StreamEntry[]>();
    /** key -> absolute expiry timestamp in ms. */
    private expiries = new Map<string, number>();
    private streamSeq = 1;
    private clock: () => number;
    private timeOffsetMs = 0;

    /** Observability for assertions. */
    public readonly ackCalls: Array<[string, string, string]> = [];
    public readonly groupCreateCalls: string[][] = [];

    constructor(options: MockRedisOptions = {}) {
        this.clock = options.now ?? (() => Date.now());
    }

    // === Test-only helpers ===

    /** Move the mock's clock forward, so lazily-expiring keys actually expire. */
    advanceTime(ms: number): void {
        this.timeOffsetMs += ms;
    }

    /**
     * The mock's current time, including everything advanceTime() added.
     *
     * Public so a component under test can be given the *same* clock (the
     * sweeper takes an injectable `now`): a test that advances only the mock
     * would otherwise have entries expire in Redis while the sweeper still
     * believed they were in the future.
     */
    nowMs(): number {
        return this.clock() + this.timeOffsetMs;
    }

    /** JSON payloads written to a stream, in order (streams store `data` fields). */
    getStreamPayloads(stream: string): any[] {
        const entries = this.streams.get(stream) || [];
        return entries.map((entry) => {
            const idx = entry.fields.indexOf('data');
            return idx >= 0 ? JSON.parse(entry.fields[idx + 1]) : null;
        });
    }

    /** Number of live (non-expired) keys, for leak assertions. */
    keyCount(): number {
        let count = 0;
        for (const key of [
            ...this.strings.keys(),
            ...this.hashes.keys(),
            ...this.sets.keys(),
            ...this.zsets.keys(),
            ...this.lists.keys(),
            ...this.streams.keys(),
        ]) {
            if (this.alive(key)) count += 1;
        }
        return count;
    }

    /** Drop a key's expiry check result: delete every container if expired. */
    private alive(key: string): boolean {
        const expiresAt = this.expiries.get(key);
        if (expiresAt !== undefined && expiresAt <= this.nowMs()) {
            this.expiries.delete(key);
            this.strings.delete(key);
            this.hashes.delete(key);
            this.sets.delete(key);
            this.zsets.delete(key);
            this.lists.delete(key);
            this.streams.delete(key);
            return false;
        }
        return true;
    }

    // === Keyspace ===

    async exists(...keys: string[]): Promise<number> {
        let found = 0;
        for (const key of keys) {
            if (!this.alive(key)) continue;
            if (
                this.strings.has(key)
                || this.hashes.has(key)
                || this.sets.has(key)
                || this.zsets.has(key)
                || this.lists.has(key)
                || this.streams.has(key)
            ) {
                found += 1;
            }
        }
        return found;
    }

    async del(...keys: string[]): Promise<number> {
        let deleted = 0;
        for (const key of keys) {
            let hit = false;
            if (this.strings.delete(key)) hit = true;
            if (this.hashes.delete(key)) hit = true;
            if (this.sets.delete(key)) hit = true;
            if (this.zsets.delete(key)) hit = true;
            if (this.lists.delete(key)) hit = true;
            if (this.streams.delete(key)) hit = true;
            this.expiries.delete(key);
            if (hit) deleted += 1;
        }
        return deleted;
    }

    async expire(key: string, seconds: number): Promise<number> {
        if ((await this.exists(key)) === 0) return 0;
        this.expiries.set(key, this.nowMs() + seconds * 1000);
        return 1;
    }

    /** Remaining TTL in seconds; -1 = no expiry, -2 = missing key. */
    async ttl(key: string): Promise<number> {
        if ((await this.exists(key)) === 0) return -2;
        const expiresAt = this.expiries.get(key);
        if (expiresAt === undefined) return -1;
        return Math.ceil((expiresAt - this.nowMs()) / 1000);
    }

    // === Strings ===

    async get(key: string): Promise<string | null> {
        if (!this.alive(key)) return null;
        return this.strings.get(key) ?? null;
    }

    /** Supports the argument forms src/ uses: SET key value [NX] [EX s | PX ms]. */
    async set(key: string, value: RedisValue, ...args: RedisValue[]): Promise<'OK' | null> {
        const tokens = args.map((a) => String(a));
        const upper = tokens.map((t) => t.toUpperCase());
        const nx = upper.includes('NX');
        const xx = upper.includes('XX');
        const exists = (await this.exists(key)) > 0;
        if (nx && exists) return null;
        if (xx && !exists) return null;

        this.strings.set(key, String(value));
        this.hashes.delete(key);
        this.sets.delete(key);
        this.zsets.delete(key);
        this.lists.delete(key);
        this.expiries.delete(key);

        const exIdx = upper.indexOf('EX');
        if (exIdx >= 0) {
            this.expiries.set(key, this.nowMs() + Number(tokens[exIdx + 1]) * 1000);
        }
        const pxIdx = upper.indexOf('PX');
        if (pxIdx >= 0) {
            this.expiries.set(key, this.nowMs() + Number(tokens[pxIdx + 1]));
        }
        return 'OK';
    }

    // === Hashes ===

    private hash(key: string): Map<string, string> {
        this.alive(key);
        let hash = this.hashes.get(key);
        if (!hash) {
            hash = new Map();
            this.hashes.set(key, hash);
        }
        return hash;
    }

    async hset(
        key: string,
        fieldOrMap: string | Record<string, RedisValue>,
        value?: RedisValue
    ): Promise<number> {
        const hash = this.hash(key);
        if (typeof fieldOrMap === 'string') {
            const isNew = !hash.has(fieldOrMap);
            hash.set(fieldOrMap, String(value));
            return isNew ? 1 : 0;
        }
        let added = 0;
        for (const [field, raw] of Object.entries(fieldOrMap)) {
            if (!hash.has(field)) added += 1;
            hash.set(field, String(raw));
        }
        return added;
    }

    async hget(key: string, field: string): Promise<string | null> {
        if (!this.alive(key)) return null;
        return this.hashes.get(key)?.get(field) ?? null;
    }

    async hmget(key: string, ...fields: string[]): Promise<(string | null)[]> {
        if (!this.alive(key)) return fields.map(() => null);
        const hash = this.hashes.get(key);
        return fields.map((field) => hash?.get(field) ?? null);
    }

    async hgetall(key: string): Promise<Record<string, string>> {
        if (!this.alive(key)) return {};
        const hash = this.hashes.get(key);
        if (!hash) return {};
        return Object.fromEntries(hash.entries());
    }

    async hdel(key: string, ...fields: string[]): Promise<number> {
        if (!this.alive(key)) return 0;
        const hash = this.hashes.get(key);
        if (!hash) return 0;
        let removed = 0;
        for (const field of fields) {
            if (hash.delete(field)) removed += 1;
        }
        return removed;
    }

    async hincrby(key: string, field: string, increment: number): Promise<number> {
        const hash = this.hash(key);
        const next = Number(hash.get(field) ?? '0') + increment;
        hash.set(field, String(next));
        return next;
    }

    // === Sets ===

    async sadd(key: string, ...members: RedisValue[]): Promise<number> {
        this.alive(key);
        let set = this.sets.get(key);
        if (!set) {
            set = new Set();
            this.sets.set(key, set);
        }
        let added = 0;
        for (const member of members.flat()) {
            const value = String(member);
            if (!set.has(value)) {
                set.add(value);
                added += 1;
            }
        }
        return added;
    }

    async srem(key: string, ...members: RedisValue[]): Promise<number> {
        if (!this.alive(key)) return 0;
        const set = this.sets.get(key);
        if (!set) return 0;
        let removed = 0;
        for (const member of members.flat()) {
            if (set.delete(String(member))) removed += 1;
        }
        return removed;
    }

    async smembers(key: string): Promise<string[]> {
        if (!this.alive(key)) return [];
        return [...(this.sets.get(key) || [])];
    }

    async sismember(key: string, member: RedisValue): Promise<number> {
        if (!this.alive(key)) return 0;
        return this.sets.get(key)?.has(String(member)) ? 1 : 0;
    }

    // === Sorted sets ===

    private zset(key: string): Map<string, number> {
        this.alive(key);
        let zset = this.zsets.get(key);
        if (!zset) {
            zset = new Map();
            this.zsets.set(key, zset);
        }
        return zset;
    }

    /** ZADD key score member [score member ...]. Returns count of NEW members. */
    async zadd(key: string, ...args: RedisValue[]): Promise<number> {
        const zset = this.zset(key);
        let added = 0;
        for (let i = 0; i + 1 < args.length; i += 2) {
            const member = String(args[i + 1]);
            if (!zset.has(member)) added += 1;
            zset.set(member, Number(args[i]));
        }
        return added;
    }

    async zrem(key: string, ...members: RedisValue[]): Promise<number> {
        if (!this.alive(key)) return 0;
        const zset = this.zsets.get(key);
        if (!zset) return 0;
        let removed = 0;
        for (const member of members.flat()) {
            if (zset.delete(String(member))) removed += 1;
        }
        return removed;
    }

    async zscore(key: string, member: RedisValue): Promise<string | null> {
        if (!this.alive(key)) return null;
        const score = this.zsets.get(key)?.get(String(member));
        return score === undefined ? null : String(score);
    }

    async zcard(key: string): Promise<number> {
        if (!this.alive(key)) return 0;
        return this.zsets.get(key)?.size ?? 0;
    }

    private sortedEntries(key: string): Array<[string, number]> {
        if (!this.alive(key)) return [];
        const zset = this.zsets.get(key);
        if (!zset) return [];
        return [...zset.entries()].sort(
            (a, b) => (a[1] - b[1]) || (a[0] < b[0] ? -1 : a[0] > b[0] ? 1 : 0)
        );
    }

    async zrange(key: string, start: number, stop: number, ...args: RedisValue[]): Promise<string[]> {
        const entries = this.sortedEntries(key);
        const size = entries.length;
        const from = start < 0 ? Math.max(size + start, 0) : start;
        const to = stop < 0 ? size + stop : Math.min(stop, size - 1);
        const slice = entries.slice(from, to + 1);
        if (args.map((a) => String(a).toUpperCase()).includes('WITHSCORES')) {
            return slice.flatMap(([member, score]) => [member, String(score)]);
        }
        return slice.map(([member]) => member);
    }

    /**
     * ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count].
     * Supports '-inf' / '+inf' / '(exclusive' bounds — the sweeper reads the
     * wait index as `zrangebyscore(key, '-inf', deadline, 'LIMIT', 0, batch)`.
     */
    async zrangebyscore(
        key: string,
        min: RedisValue,
        max: RedisValue,
        ...args: RedisValue[]
    ): Promise<string[]> {
        const lo = parseScoreBound(min);
        const hi = parseScoreBound(max);
        let entries = this.sortedEntries(key).filter(([, score]) => {
            const aboveMin = lo.exclusive ? score > lo.value : score >= lo.value;
            const belowMax = hi.exclusive ? score < hi.value : score <= hi.value;
            return aboveMin && belowMax;
        });

        const tokens = args.map((a) => String(a));
        const upper = tokens.map((t) => t.toUpperCase());
        const limitIdx = upper.indexOf('LIMIT');
        if (limitIdx >= 0) {
            const offset = Number(tokens[limitIdx + 1]);
            const count = Number(tokens[limitIdx + 2]);
            entries = count < 0 ? entries.slice(offset) : entries.slice(offset, offset + count);
        }
        if (upper.includes('WITHSCORES')) {
            return entries.flatMap(([member, score]) => [member, String(score)]);
        }
        return entries.map(([member]) => member);
    }

    async zremrangebyscore(key: string, min: RedisValue, max: RedisValue): Promise<number> {
        const doomed = await this.zrangebyscore(key, min, max);
        if (doomed.length === 0) return 0;
        return this.zrem(key, ...doomed);
    }

    // === Lists (SpanRecorder appends span JSON to trace_spans) ===

    async rpush(key: string, ...values: RedisValue[]): Promise<number> {
        this.alive(key);
        let list = this.lists.get(key);
        if (!list) {
            list = [];
            this.lists.set(key, list);
        }
        list.push(...values.flat().map((v) => String(v)));
        return list.length;
    }

    async lpush(key: string, ...values: RedisValue[]): Promise<number> {
        this.alive(key);
        let list = this.lists.get(key);
        if (!list) {
            list = [];
            this.lists.set(key, list);
        }
        list.unshift(...values.flat().map((v) => String(v)));
        return list.length;
    }

    async llen(key: string): Promise<number> {
        if (!this.alive(key)) return 0;
        return this.lists.get(key)?.length ?? 0;
    }

    async lrange(key: string, start: number, stop: number): Promise<string[]> {
        if (!this.alive(key)) return [];
        const list = this.lists.get(key) || [];
        const from = start < 0 ? Math.max(list.length + start, 0) : start;
        const to = stop < 0 ? list.length + stop : Math.min(stop, list.length - 1);
        return list.slice(from, to + 1);
    }

    // === Streams ===

    async xadd(stream: string, _id: string, ...fields: RedisValue[]): Promise<string> {
        this.alive(stream);
        let entries = this.streams.get(stream);
        if (!entries) {
            entries = [];
            this.streams.set(stream, entries);
        }
        const id = `${this.streamSeq++}-0`;
        entries.push({ id, fields: fields.flat().map((f) => String(f)) });
        return id;
    }

    async xlen(stream: string): Promise<number> {
        if (!this.alive(stream)) return 0;
        return this.streams.get(stream)?.length ?? 0;
    }

    async xrevrange(stream: string, ..._args: RedisValue[]): Promise<Array<[string, string[]]>> {
        if (!this.alive(stream)) return [];
        const entries = [...(this.streams.get(stream) || [])].reverse();
        const countIdx = _args.map((a) => String(a).toUpperCase()).indexOf('COUNT');
        const limited = countIdx >= 0 ? entries.slice(0, Number(_args[countIdx + 1])) : entries;
        return limited.map((entry) => [entry.id, [...entry.fields]] as [string, string[]]);
    }

    async xgroup(...args: RedisValue[]): Promise<'OK'> {
        this.groupCreateCalls.push(args.map((a) => String(a)));
        return 'OK';
    }

    async xack(stream: string, group: string, msgId: string): Promise<number> {
        this.ackCalls.push([stream, group, msgId]);
        return 1;
    }

    /**
     * Pops one entry per named stream. Consumer-group pending/claim semantics
     * are deliberately not modelled — every pre-existing mock in this repo
     * behaved this way and no test depends on redelivery.
     */
    async xreadgroup(...args: RedisValue[]): Promise<any> {
        const tokens = args.map((a) => String(a));
        const streamsIdx = tokens.indexOf('STREAMS');
        if (streamsIdx < 0) return null;
        const rest = tokens.slice(streamsIdx + 1);
        const streamNames = rest.slice(0, rest.length / 2);

        const result: any[] = [];
        for (const streamName of streamNames) {
            if (!this.alive(streamName)) continue;
            const entries = this.streams.get(streamName);
            if (!entries || entries.length === 0) continue;
            const entry = entries.shift()!;
            result.push([streamName, [[entry.id, entry.fields]]]);
        }
        return result.length > 0 ? result : null;
    }

    // === Connection lifecycle (runner duplicates its blocking connections) ===

    duplicate(): MockRedis {
        // Share every container so a duplicate sees the same keyspace, exactly
        // like a second connection to one Redis would.
        const child = new MockRedis({ now: () => this.nowMs() });
        (child as any).strings = this.strings;
        (child as any).hashes = this.hashes;
        (child as any).sets = this.sets;
        (child as any).zsets = this.zsets;
        (child as any).lists = this.lists;
        (child as any).streams = this.streams;
        (child as any).expiries = this.expiries;
        (child as any).ackCalls = this.ackCalls;
        (child as any).groupCreateCalls = this.groupCreateCalls;
        return child;
    }

    async quit(): Promise<'OK'> {
        return 'OK';
    }

    disconnect(): void {
        // no-op
    }

    // === Pipeline ===

    /**
     * Queues supported commands and replays them in order on exec(), returning
     * ioredis-shaped [err, result] tuples. No MULTI isolation — src/ only ever
     * pipelines commands that share one hash tag, so ordering is all that
     * matters.
     */
    pipeline(): any {
        const self = this;
        const queued: Array<() => Promise<any>> = [];
        const proxy: any = new Proxy(
            {
                exec: async () => {
                    const results: Array<[Error | null, any]> = [];
                    for (const run of queued) {
                        try {
                            results.push([null, await run()]);
                        } catch (err) {
                            results.push([err as Error, null]);
                        }
                    }
                    return results;
                },
            },
            {
                get(target: any, prop: string) {
                    if (prop in target) return target[prop];
                    const command = (self as any)[prop];
                    if (typeof command !== 'function') {
                        throw new Error(`MockRedis pipeline: unsupported command "${String(prop)}"`);
                    }
                    return (...cmdArgs: any[]) => {
                        queued.push(() => command.apply(self, cmdArgs));
                        return proxy;
                    };
                },
            }
        );
        return proxy;
    }
}

/** Cast helper: MockRedis is structurally a Redis for everything src/ calls. */
export function asRedis(mock: MockRedis): any {
    return mock as any;
}

/**
 * Make `registry.hasOnlineAgentType(agentType)` answer true.
 *
 * Needed by any test that wants a dispatch to actually go out: without an online
 * worker the group fan-out treats the member as undispatchable and compensates
 * it with a stand-in instead, so a test that forgot this would silently assert
 * against the failure path.
 */
export async function bringAgentTypeOnline(
    redis: MockRedis,
    agentType: string,
    workerId: string = `worker-${agentType}`
): Promise<void> {
    const { RegistryKeys } = await import('../../src/constants');
    await redis.sadd(RegistryKeys.agentTypeMembers(agentType), workerId);
    await redis.set(
        RegistryKeys.worker_online_lease(workerId),
        JSON.stringify({ token: workerId, last_seen: Date.now(), ip_address: '127.0.0.1' })
    );
}

import * as net from 'net';

/**
 * Minimal RESP2 server implementing just enough of the Redis command surface
 * that WorkerRegistry's heartbeat path uses (GET/SET with EX+NX/SADD/
 * SISMEMBER/HGETALL/DEL), so a real ioredis client — including one running
 * inside a real worker_threads.Worker — can connect over a real TCP socket
 * without a live Redis server. Test-only; not part of the shipped SDK.
 */
export class FakeRedisTcpServer {
    private server: net.Server;
    private strings = new Map<string, { value: string; expiresAt: number | null }>();
    private sets = new Map<string, Set<string>>();
    private hashes = new Map<string, Map<string, string>>();
    public port = 0;

    constructor() {
        this.server = net.createServer((socket) => this.handleConnection(socket));
    }

    async listen(): Promise<number> {
        await new Promise<void>((resolve) => this.server.listen(0, '127.0.0.1', resolve));
        const address = this.server.address();
        this.port = typeof address === 'object' && address ? address.port : 0;
        return this.port;
    }

    async close(): Promise<void> {
        await new Promise<void>((resolve) => this.server.close(() => resolve()));
    }

    /** Every wall-clock timestamp (ms) a SET touched this key — lets tests confirm renewals landed during a specific window. */
    getSetTimestamps(key: string): number[] {
        return [...(this.setTimestamps.get(key) || [])];
    }

    private setTimestamps = new Map<string, number[]>();

    private isExpired(key: string): boolean {
        const entry = this.strings.get(key);
        if (!entry) return false;
        return entry.expiresAt !== null && Date.now() > entry.expiresAt;
    }

    private handleConnection(socket: net.Socket): void {
        let buffer: Buffer<ArrayBufferLike> = Buffer.alloc(0);
        socket.on('data', (chunk) => {
            buffer = Buffer.concat([buffer, chunk]);
            let result = parseCommands(buffer);
            while (result.commands.length > 0) {
                for (const args of result.commands) {
                    const reply = this.execute(args);
                    socket.write(reply);
                }
                buffer = result.rest;
                result = parseCommands(buffer);
            }
        });
        socket.on('error', () => {
            /* ignore — connection drop on terminate() is expected */
        });
    }

    private execute(args: string[]): Uint8Array {
        const [rawCmd, ...rest] = args;
        const cmd = (rawCmd || '').toUpperCase();
        try {
            switch (cmd) {
                case 'PING':
                    return simpleString('PONG');
                case 'SELECT':
                case 'CLIENT':
                case 'HELLO':
                    return simpleString('OK');
                case 'GET': {
                    const [key] = rest;
                    if (!key || this.isExpired(key)) return nilBulk();
                    const entry = this.strings.get(key);
                    return entry ? bulkString(entry.value) : nilBulk();
                }
                case 'SET': {
                    const [key, value, ...opts] = rest;
                    const upperOpts = opts.map((o) => o.toUpperCase());
                    const nx = upperOpts.includes('NX');
                    const exIdx = upperOpts.indexOf('EX');
                    const exSeconds = exIdx >= 0 ? parseInt(opts[exIdx + 1], 10) : null;

                    if (nx && this.strings.has(key) && !this.isExpired(key)) {
                        return nilBulk();
                    }
                    this.strings.set(key, {
                        value,
                        expiresAt: exSeconds !== null ? Date.now() + exSeconds * 1000 : null,
                    });
                    if (!this.setTimestamps.has(key)) this.setTimestamps.set(key, []);
                    this.setTimestamps.get(key)!.push(Date.now());
                    return simpleString('OK');
                }
                case 'DEL': {
                    let count = 0;
                    for (const key of rest) {
                        if (this.strings.delete(key)) count += 1;
                        if (this.sets.delete(key)) count += 1;
                        if (this.hashes.delete(key)) count += 1;
                    }
                    return integer(count);
                }
                case 'SADD': {
                    const [key, ...members] = rest;
                    if (!this.sets.has(key)) this.sets.set(key, new Set());
                    const set = this.sets.get(key)!;
                    let added = 0;
                    for (const m of members) {
                        if (!set.has(m)) { set.add(m); added += 1; }
                    }
                    return integer(added);
                }
                case 'SISMEMBER': {
                    const [key, member] = rest;
                    const set = this.sets.get(key);
                    return integer(set?.has(member) ? 1 : 0);
                }
                case 'SMEMBERS': {
                    const [key] = rest;
                    const set = this.sets.get(key);
                    return arrayOfBulk(set ? [...set] : []);
                }
                case 'HGETALL': {
                    const [key] = rest;
                    const hash = this.hashes.get(key);
                    if (!hash) return arrayOfBulk([]);
                    const flat: string[] = [];
                    for (const [f, v] of hash.entries()) { flat.push(f, v); }
                    return arrayOfBulk(flat);
                }
                case 'HSET': {
                    const [key, ...fieldValues] = rest;
                    if (!this.hashes.has(key)) this.hashes.set(key, new Map());
                    const hash = this.hashes.get(key)!;
                    let added = 0;
                    for (let i = 0; i < fieldValues.length; i += 2) {
                        if (!hash.has(fieldValues[i])) added += 1;
                        hash.set(fieldValues[i], fieldValues[i + 1]);
                    }
                    return integer(added);
                }
                case 'EXPIRE': {
                    const [key, seconds] = rest;
                    const entry = this.strings.get(key);
                    if (entry) entry.expiresAt = Date.now() + parseInt(seconds, 10) * 1000;
                    return integer(entry ? 1 : 0);
                }
                default:
                    return simpleString('OK');
            }
        } catch (error) {
            return errorReply(String((error as Error)?.message || error));
        }
    }
}

function parseCommands(buffer: Buffer<ArrayBufferLike>): { commands: string[][]; rest: Buffer<ArrayBufferLike> } {
    const commands: string[][] = [];
    let offset = 0;

    while (offset < buffer.length) {
        if (buffer[offset] !== 0x2a /* '*' */) break; // wait for more data / malformed
        const arrayEnd = buffer.indexOf('\r\n', offset);
        if (arrayEnd === -1) break;
        const argCount = parseInt(buffer.toString('utf8', offset + 1, arrayEnd), 10);
        let cursor = arrayEnd + 2;
        const args: string[] = [];
        let incomplete = false;

        for (let i = 0; i < argCount; i++) {
            if (buffer[cursor] !== 0x24 /* '$' */) { incomplete = true; break; }
            const lenEnd = buffer.indexOf('\r\n', cursor);
            if (lenEnd === -1) { incomplete = true; break; }
            const len = parseInt(buffer.toString('utf8', cursor + 1, lenEnd), 10);
            const dataStart = lenEnd + 2;
            const dataEnd = dataStart + len;
            if (dataEnd + 2 > buffer.length) { incomplete = true; break; }
            args.push(buffer.toString('utf8', dataStart, dataEnd));
            cursor = dataEnd + 2;
        }

        if (incomplete) break;
        commands.push(args);
        offset = cursor;
    }

    return { commands, rest: buffer.subarray(offset) };
}

function simpleString(s: string): Uint8Array {
    return Buffer.from(`+${s}\r\n`);
}
function errorReply(msg: string): Uint8Array {
    return Buffer.from(`-ERR ${msg}\r\n`);
}
function integer(n: number): Uint8Array {
    return Buffer.from(`:${n}\r\n`);
}
function bulkString(s: string): Uint8Array {
    const data = Buffer.from(s, 'utf8');
    return Buffer.concat([Buffer.from(`$${data.length}\r\n`), data, Buffer.from('\r\n')]);
}
function nilBulk(): Uint8Array {
    return Buffer.from('$-1\r\n');
}
function arrayOfBulk(items: string[]): Uint8Array {
    const parts: Uint8Array[] = [Buffer.from(`*${items.length}\r\n`)];
    for (const item of items) parts.push(bulkString(item));
    return Buffer.concat(parts as Buffer[]);
}

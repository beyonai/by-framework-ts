import { constants, createPublicKey, publicEncrypt } from 'crypto';
import { RedisConnectionError } from '../src/exceptions';
import { resolveRedisPassword } from '../src/redis_password';

const MODULUS = BigInt(
    '118249911269313777353369205468358971445904616358074252669429944094950609' +
        '311848727388859640174096832607664732279928019002795333396858212446930193' +
        '162882569448161371673123891912144968110489981164388431260818873249786366' +
        '362488910362896712606417158347288417834035704864053434682010900207183838' +
        '254091105621956825021'
);
const KEY_LENGTH = 128;
// PKCS#1 v1.5: 11 bytes of overhead per block.
const MAX_CHUNK = KEY_LENGTH - 11;

const publicKey = createPublicKey({
    key: {
        kty: 'RSA',
        n: Buffer.from(MODULUS.toString(16), 'hex').toString('base64url'),
        e: 'AQAB',
    },
    format: 'jwk',
});

/** Produce the byclaw-be {cipher} format: hex of concatenated RSA PKCS#1 v1.5 blocks. */
function encrypt(plaintext: string, upper = false): string {
    const data = Buffer.from(plaintext, 'utf8');
    const blocks: Buffer[] = [];
    for (let i = 0; i < data.length; i += MAX_CHUNK) {
        blocks.push(
            publicEncrypt(
                { key: publicKey, padding: constants.RSA_PKCS1_PADDING },
                data.subarray(i, i + MAX_CHUNK)
            )
        );
    }
    const hex = Buffer.concat(blocks).toString('hex');
    return '{cipher}' + (upper ? hex.toUpperCase() : hex);
}

/** Raw RSA on a hand-built encoded message, to exercise malformed padding. */
function rawBlock(em: Buffer): string {
    let result = 1n;
    let b = BigInt('0x' + em.toString('hex')) % MODULUS;
    let e = 65537n;
    while (e > 0n) {
        if (e & 1n) result = (result * b) % MODULUS;
        b = (b * b) % MODULUS;
        e >>= 1n;
    }
    return '{cipher}' + result.toString(16).padStart(KEY_LENGTH * 2, '0');
}

describe('resolveRedisPassword', () => {
    test('plain values pass through unchanged', () => {
        for (const value of [undefined, '', 'secret', ' {cipher}leading-space', 'cipher}x']) {
            expect(resolveRedisPassword(value)).toBe(value);
        }
    });

    test('decrypts a single block', () => {
        expect(resolveRedisPassword(encrypt('s3cr3t!'))).toBe('s3cr3t!');
    });

    test('accepts uppercase hex', () => {
        expect(resolveRedisPassword(encrypt('s3cr3t!', true))).toBe('s3cr3t!');
    });

    test('decrypts multiple blocks', () => {
        const plaintext = 'p'.repeat(MAX_CHUNK * 2 + 5);
        expect(resolveRedisPassword(encrypt(plaintext))).toBe(plaintext);
    });

    test('decrypts non-ASCII plaintext', () => {
        expect(resolveRedisPassword(encrypt('密码-ü'))).toBe('密码-ü');
    });

    test('rejects odd-length hex', () => {
        expect(() => resolveRedisPassword(encrypt('secret').slice(0, -1))).toThrow(RedisConnectionError);
    });

    test('rejects bad padding', () => {
        expect(() => resolveRedisPassword('{cipher}' + '01'.repeat(KEY_LENGTH))).toThrow(
            RedisConnectionError
        );
    });

    test('rejects fewer than 8 padding bytes', () => {
        const em = Buffer.concat([
            Buffer.from([0x00, 0x02]),
            Buffer.alloc(7, 0x01),
            Buffer.from([0x00]),
            Buffer.alloc(KEY_LENGTH - 10, 0x78),
        ]);
        expect(() => resolveRedisPassword(rawBlock(em))).toThrow(RedisConnectionError);
    });

    test('accepts exactly 8 padding bytes', () => {
        const em = Buffer.concat([
            Buffer.from([0x00, 0x02]),
            Buffer.alloc(8, 0x01),
            Buffer.from([0x00]),
            Buffer.alloc(KEY_LENGTH - 11, 0x78),
        ]);
        expect(resolveRedisPassword(rawBlock(em))).toBe('x'.repeat(KEY_LENGTH - 11));
    });

    test('rejects a value larger than the modulus', () => {
        expect(() => resolveRedisPassword('{cipher}' + 'ff'.repeat(KEY_LENGTH))).toThrow(
            RedisConnectionError
        );
    });

    test('error message does not leak the value', () => {
        expect(() => resolveRedisPassword('{cipher}' + 'ab'.repeat(KEY_LENGTH))).toThrow(
            /^Failed to decrypt REDIS_PASSWORD \{cipher\} value$/
        );
    });
});

const mockRedisConstructor = jest.fn();

jest.mock('ioredis', () => ({
    __esModule: true,
    default: jest.fn().mockImplementation((options) => {
        mockRedisConstructor(options);
        return { options, quit: jest.fn().mockResolvedValue('OK') };
    }),
    Cluster: jest.fn(),
}));

describe('createRedis resolves {cipher} passwords', () => {
    const originalEnv = { ...process.env };

    beforeEach(() => {
        mockRedisConstructor.mockClear();
        process.env = { ...originalEnv };
        delete process.env.REDIS_PASSWORD;
        delete process.env.REDIS_MODE;
        delete process.env.REDIS_CLUSTER_HOST;
        delete process.env.REDIS_CLUSTER_NODES;
    });

    afterAll(() => {
        process.env = originalEnv;
    });

    test('decrypts a {cipher} password from REDIS_PASSWORD', async () => {
        process.env.REDIS_PASSWORD = encrypt('env-secret');
        const { createRedis } = await import('../src/redis_client');
        createRedis();
        expect(mockRedisConstructor.mock.calls[0][0].password).toBe('env-secret');
    });

    test('decrypts an explicit {cipher} password option', async () => {
        const { createRedis } = await import('../src/redis_client');
        createRedis({ password: encrypt('option-secret') });
        expect(mockRedisConstructor.mock.calls[0][0].password).toBe('option-secret');
    });

    test('passes a plain password through', async () => {
        const { createRedis } = await import('../src/redis_client');
        createRedis({ password: 'plain-secret' });
        expect(mockRedisConstructor.mock.calls[0][0].password).toBe('plain-secret');
    });

    test('an undecryptable {cipher} password fails before any client is built', async () => {
        const { createRedis } = await import('../src/redis_client');
        expect(() => createRedis({ password: '{cipher}zz' })).toThrow(RedisConnectionError);
        expect(mockRedisConstructor).not.toHaveBeenCalled();
    });
});

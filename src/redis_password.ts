/**
 * Redis password resolution.
 *
 * Plain text values are returned unchanged; values prefixed with `{cipher}`
 * use the built-in RSA cipher format shared with byclaw-be. This is a
 * line-for-line port of the Java SDK's `RedisPasswordCipher` (and the Python
 * SDK's `redis_password.py`) so every SDK accepts exactly the same
 * ciphertexts.
 */
import { RedisConnectionError } from './exceptions';

export const CIPHER_PREFIX = '{cipher}';

const MODULUS = BigInt(
    '118249911269313777353369205468358971445904616358074252669429944094950609' +
        '311848727388859640174096832607664732279928019002795333396858212446930193' +
        '162882569448161371673123891912144968110489981164388431260818873249786366' +
        '362488910362896712606417158347288417834035704864053434682010900207183838' +
        '254091105621956825021'
);

const PRIVATE_KEY = BigInt(
    '486373286567224954199523979218622218639212176106359698331805479808884847' +
        '431251704456124091815761206611869710749307975683865756297314490245121120' +
        '420321732982564126362643243208709861523940153746233446702040720249396157' +
        '848350707068412091346394073481926826790160613505333343349449539446344171' +
        '83180191532927602945'
);

// Java: privateKey.getModulus().bitLength() / 8
const KEY_LENGTH = Math.floor(MODULUS.toString(2).length / 8);

// The JDK's PKCS#1 v1.5 unpadding requires at least 8 bytes of padding.
const MIN_PADDING_LENGTH = 8;

/**
 * Return `value` unchanged unless it starts with `{cipher}`, in which case
 * decrypt the remainder.
 *
 * @throws RedisConnectionError the `{cipher}` value could not be decrypted.
 *   The message never includes the ciphertext or any decrypted bytes.
 */
export function resolveRedisPassword(value: string): string;
export function resolveRedisPassword(value: string | undefined): string | undefined;
export function resolveRedisPassword(value: string | undefined): string | undefined {
    if (!value || !value.startsWith(CIPHER_PREFIX)) {
        return value;
    }
    return decrypt(value.slice(CIPHER_PREFIX.length));
}

function decrypt(text: string): string {
    try {
        // Java passes input.length() (UTF-16 units) as the length, which
        // String#length matches exactly.
        const bcd = asciiToBcd(Buffer.from(text, 'utf8'), text.length);
        let plaintext = '';
        for (const block of splitArray(bcd, KEY_LENGTH)) {
            // Buffer#toString replaces malformed UTF-8, like Java's new String().
            plaintext += rsaDecryptBlock(block).toString('utf8');
        }
        return plaintext;
    } catch (error) {
        throw new RedisConnectionError(
            'Failed to decrypt REDIS_PASSWORD {cipher} value',
            error instanceof Error ? error : null
        );
    }
}

/**
 * Equivalent of the JDK's `Cipher.getInstance("RSA")` decrypt, i.e.
 * RSA/ECB/PKCS1Padding, for a single block.
 */
function rsaDecryptBlock(block: Buffer): Buffer {
    const c = BigInt('0x' + (block.toString('hex') || '0'));
    if (c >= MODULUS) {
        throw new RedisConnectionError('Message is larger than modulus');
    }
    const em = Buffer.from(modPow(c, PRIVATE_KEY, MODULUS).toString(16).padStart(KEY_LENGTH * 2, '0'), 'hex');
    if (em[0] !== 0x00 || em[1] !== 0x02) {
        throw new RedisConnectionError('Decryption error');
    }
    const separator = em.indexOf(0x00, 2);
    if (separator < 0 || separator - 2 < MIN_PADDING_LENGTH) {
        throw new RedisConnectionError('Decryption error');
    }
    return em.subarray(separator + 1);
}

function modPow(base: bigint, exponent: bigint, modulus: bigint): bigint {
    let result = 1n;
    let b = base % modulus;
    let e = exponent;
    while (e > 0n) {
        if (e & 1n) {
            result = (result * b) % modulus;
        }
        b = (b * b) % modulus;
        e >>= 1n;
    }
    return result;
}

function asciiToBcd(ascii: Buffer, length: number): Buffer {
    const bcd = Buffer.alloc(Math.floor(length / 2));
    let j = 0;
    for (let i = 0; i < Math.floor((length + 1) / 2); i++) {
        // Java's byte[] indexing throws on odd-length input here; Buffer
        // silently ignores out-of-range writes, so check explicitly.
        if (i >= bcd.length) {
            throw new RedisConnectionError('Odd-length {cipher} value');
        }
        const high = ascToBcd(ascii[j++]);
        const low = j >= length ? 0x00 : ascToBcd(ascii[j++]);
        bcd[i] = (low + (high << 4)) & 0xff;
    }
    return bcd;
}

function ascToBcd(asc: number): number {
    if (asc >= 0x30 && asc <= 0x39) {
        return asc - 0x30;
    }
    if (asc >= 0x41 && asc <= 0x46) {
        return asc - 0x41 + 10;
    }
    if (asc >= 0x61 && asc <= 0x66) {
        return asc - 0x61 + 10;
    }
    return (asc - 48) & 0xff;
}

function splitArray(data: Buffer, length: number): Buffer[] {
    // A short trailing block is right-padded with zeros, as in Java.
    const blocks: Buffer[] = [];
    for (let i = 0; i < data.length; i += length) {
        const block = Buffer.alloc(length);
        data.copy(block, 0, i, Math.min(i + length, data.length));
        blocks.push(block);
    }
    return blocks;
}

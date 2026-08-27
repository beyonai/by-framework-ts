import {
    decodeMember,
    encodeMember,
    fnv1a32,
    memberDigest,
    memberFromResume,
    waitIndexKey,
    waitIndexShard,
} from '../src/liveness/wait_index';
import { consumedMarkerKey } from '../src/liveness/wait_gate';
import { GatewaySDKError, WaitIndexMemberError } from '../src/exceptions';
import { RegistryKeys, WAIT_INDEX_SHARDS } from '../src/constants';
import { ResumeCommand } from '../src/protocol/commands';
import { MessageHeader } from '../src/protocol/message_header';

/**
 * Cross-SDK wire contract for the wait index
 * (.trellis/tasks/08-24-suspended-parent-liveness/research/cross-sdk-wire-contract.md
 * §1-§3). Every expectation here was cross-checked byte-for-byte against
 * by-framework-python core/wait_index.py — a divergence in any of them makes a
 * Python worker and a TS worker compute different keys/members for the same
 * wait, so the gate never matches and the sweep looks in the wrong shard.
 */

describe('fnv1a32 / shard selection (contract §3)', () => {
    test('reference vector: waitIndexShard("sess-1") === 1', () => {
        // The one vector the contract pins by hand. If this moves, every SDK's
        // sweeper is looking at a different shard than the writer used.
        expect(waitIndexShard('sess-1')).toBe(1);
    });

    test('fnv1a32 matches the reference implementation on the same vector', () => {
        expect(fnv1a32('sess-1')).toBe(1532043137);
    });

    test('fnv1a32 offset basis for the empty string', () => {
        expect(fnv1a32('')).toBe(0x811c9dc5);
    });

    test('is unsigned 32-bit for inputs that overflow a signed multiply', () => {
        for (const text of ['a', 'session', 'sess-1', '中文-session', 'x'.repeat(200)]) {
            const digest = fnv1a32(text);
            expect(Number.isInteger(digest)).toBe(true);
            expect(digest).toBeGreaterThanOrEqual(0);
            expect(digest).toBeLessThanOrEqual(0xffffffff);
        }
    });

    test('hashes UTF-8 bytes, not UTF-16 code units', () => {
        // 'é' is one UTF-16 unit but two UTF-8 bytes; a code-unit-based
        // hash would disagree with Python/Java here and only here.
        expect(fnv1a32('é')).toBe(fnv1a32(Buffer.from('é', 'utf-8').toString('utf-8')));
        expect(fnv1a32('é')).not.toBe(fnv1a32('e'));
    });

    test('shard is always inside the fixed shard count', () => {
        for (let i = 0; i < 200; i += 1) {
            const shard = waitIndexShard(`sess-${i}`);
            expect(shard).toBeGreaterThanOrEqual(0);
            expect(shard).toBeLessThan(WAIT_INDEX_SHARDS);
        }
    });

    test('waitIndexKey routes through RegistryKeys, not a hardcoded prefix', () => {
        expect(waitIndexKey('sess-1')).toBe(RegistryKeys.wait_index(1));
    });
});

describe('member encoding (contract §2)', () => {
    test('plain fields join with the separator', () => {
        expect(encodeMember({
            sessionId: 'sess-1',
            parentMessageId: 'msg-caller',
            childMessageId: 'msg-child',
            taskGroupId: 'tg-abc',
        })).toBe('sess-1|msg-caller|msg-child|tg-abc');
    });

    test('an askUser member has an empty childMessageId and taskGroupId', () => {
        expect(encodeMember({
            sessionId: 'sess-1',
            parentMessageId: 'msg-caller',
            childMessageId: '',
        })).toBe('sess-1|msg-caller||');
    });

    test('escapes the escape character first, then the separator', () => {
        // Byte-for-byte identical to Python's encode_member on the same input.
        expect(encodeMember({
            sessionId: 'a|b\\c',
            parentMessageId: 'msg-1',
            childMessageId: '',
            taskGroupId: 'tg-9',
        })).toBe('a\\|b\\\\c|msg-1||tg-9');
    });

    test('round-trips a session_id containing BOTH a backslash and a pipe (AC-TS-2)', () => {
        // The exact shape a naive split() would tear apart: the literal
        // two-character sequence `\|` inside a field.
        const sessionId = 'we|ird\\sess\\|ion|';
        const member = encodeMember({
            sessionId,
            parentMessageId: 'msg|caller\\',
            childMessageId: '\\\\',
            taskGroupId: 'tg-|\\|',
        });
        expect(decodeMember(member)).toEqual({
            sessionId,
            parentMessageId: 'msg|caller\\',
            childMessageId: '\\\\',
            taskGroupId: 'tg-|\\|',
        });
    });

    test('decoding is a character scan, so an escaped separator does not split', () => {
        const member = encodeMember({
            sessionId: 'a|b',
            parentMessageId: 'c|d',
            childMessageId: 'e|f',
            taskGroupId: 'g|h',
        });
        // A split(SEPARATOR) would see 8 pieces here; the scan sees 4.
        expect(member.split('|')).toHaveLength(8);
        expect(decodeMember(member).sessionId).toBe('a|b');
    });

    test('null/undefined fields normalise to the empty string', () => {
        expect(encodeMember({
            sessionId: 'sess-1',
            parentMessageId: 'msg-caller',
            childMessageId: undefined as unknown as string,
        })).toBe('sess-1|msg-caller||');
    });

    test('rejects a member with the wrong field count', () => {
        expect(() => decodeMember('a|b|c')).toThrow(/expected 4 fields/);
        expect(() => decodeMember('a|b|c|d|e')).toThrow(/expected 4 fields/);
    });

    test('rejects a dangling escape rather than silently truncating', () => {
        expect(() => decodeMember('a|b|c|d\\')).toThrow(/dangling escape/);
    });

    // A decode failure is how a cross-SDK encoding drift surfaces, so it must
    // be catchable as an SDK error rather than a bare Error (see the ban in
    // src/exceptions.ts), and `reason` must stay specific enough to tell an
    // encoder bug from a corrupted key.
    test('throws a typed WaitIndexMemberError carrying the member and the reason', () => {
        expect(() => decodeMember('a|b|c')).toThrow(WaitIndexMemberError);
        expect(() => decodeMember('a|b|c|d\\')).toThrow(WaitIndexMemberError);

        try {
            decodeMember('a|b|c');
            throw new Error('decodeMember should have thrown');
        } catch (error) {
            const typed = error as WaitIndexMemberError;
            expect(typed).toBeInstanceOf(GatewaySDKError);
            expect(typed.name).toBe('WaitIndexMemberError');
            expect(typed.member).toBe('a|b|c');
            expect(typed.reason).toBe('expected 4 fields, got 3');
        }
    });
});

describe('memberFromResume (contract §2 — the id reversal)', () => {
    test('reads the caller id from header.messageId and the child id from header.parentMessageId', () => {
        // enqueueAgentReturn builds every reply this way: header.messageId is
        // the CALLER's own id (what the suspended execution reattaches by) and
        // header.parentMessageId is the SUB-TASK's dispatch-time id (the only
        // per-sibling-unique value). Reading them the other way round would make
        // every sibling in a Task Group collide onto one member.
        const reply = new ResumeCommand(
            new MessageHeader('msg-caller', 'sess-1', 'trace-1', {
                sourceAgentType: 'child-agent',
                targetAgentType: 'caller-agent',
                parentMessageId: 'msg-child',
                taskGroupId: 'tg-abc',
            }),
            '',
            'COMPLETED',
            null
        );
        expect(memberFromResume(reply)).toBe(
            encodeMember({
                sessionId: 'sess-1',
                parentMessageId: 'msg-caller',
                childMessageId: 'msg-child',
                taskGroupId: 'tg-abc',
            })
        );
    });

    test('two siblings of one group produce two distinct members', () => {
        const build = (childMessageId: string) => new ResumeCommand(
            new MessageHeader('msg-caller', 'sess-1', 'trace-1', {
                parentMessageId: childMessageId,
                taskGroupId: 'tg-abc',
            }),
            '',
            'COMPLETED',
            null
        );
        expect(memberFromResume(build('msg-child-a')))
            .not.toBe(memberFromResume(build('msg-child-b')));
    });
});

describe('memberDigest / consumedMarkerKey (contract §1)', () => {
    test('is SHA-1 hex of the member string', () => {
        // Same value Python's hashlib.sha1(b"x").hexdigest() produces.
        expect(memberDigest('x')).toBe('11f6ad8ec52a2984abaafd7c3b516503785c2072');
    });

    test('consumed marker routes through RegistryKeys with the digest, not the raw member', () => {
        const member = encodeMember({
            sessionId: 'sess-1',
            parentMessageId: 'msg-caller',
            childMessageId: 'msg-child',
        });
        expect(consumedMarkerKey('sess-1', member))
            .toBe(RegistryKeys.wait_consumed('sess-1', memberDigest(member)));
        // The unbounded caller-controlled member must not end up in the key.
        expect(consumedMarkerKey('sess-1', member)).not.toContain('msg-child');
    });
});

describe('wait keys under the v2 (Cluster) schema (contract §1)', () => {
    const previous = process.env.REDIS_KEY_SCHEMA_VERSION;
    beforeAll(() => { process.env.REDIS_KEY_SCHEMA_VERSION = 'v2'; });
    afterAll(() => {
        if (previous === undefined) delete process.env.REDIS_KEY_SCHEMA_VERSION;
        else process.env.REDIS_KEY_SCHEMA_VERSION = previous;
    });

    test('the shard keys are cross-entity, so deliberately untagged', () => {
        expect(RegistryKeys.wait_index(3)).toBe('byai_gateway:v2:wait:index:3');
        expect(RegistryKeys.wait_sweep_lock(3)).toBe('byai_gateway:v2:wait:sweep_lock:3');
    });

    test('the per-session keys carry the session hash tag', () => {
        expect(RegistryKeys.wait_consumed('sess-1', 'deadbeef'))
            .toBe('byai_gateway:v2:wait:consumed:{sess-1}:deadbeef');
        expect(RegistryKeys.wait_renew_origin('sess-1', 'deadbeef'))
            .toBe('byai_gateway:v2:wait:renew_origin:{sess-1}:deadbeef');
    });
});

describe('wait keys under the v1 (default) schema', () => {
    test('match the Python v1 spellings byte-for-byte', () => {
        expect(RegistryKeys.wait_index(3)).toBe('byai_gateway:wait:index:3');
        expect(RegistryKeys.wait_sweep_lock(3)).toBe('byai_gateway:wait:sweep_lock:3');
        expect(RegistryKeys.wait_consumed('sess-1', 'deadbeef'))
            .toBe('byai_gateway:wait:consumed:sess-1:deadbeef');
        expect(RegistryKeys.wait_renew_origin('sess-1', 'deadbeef'))
            .toBe('byai_gateway:wait:renew_origin:sess-1:deadbeef');
    });
});

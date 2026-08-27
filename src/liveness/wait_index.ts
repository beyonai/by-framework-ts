/**
 * Wait-index member encoding for suspended-caller liveness.
 *
 * A caller that suspends on `callAgent(waitForReply=true)` (or `dispatchGroup`
 * / `askUser`) registers one entry in the sharded ZSET
 * `RegistryKeys.wait_index(shard)`; the entry is removed again when the reply
 * lands. This module owns the *pure* half of that contract: how the ZSET member
 * is spelled, and how a member is reconstructed from a reply.
 *
 * The load-bearing property is {@link memberFromResume}: every field of a
 * member must be derivable from a single `ResumeCommand`, with no Redis lookup.
 * That is what lets the idempotency gate `ZREM` the entry the moment a reply is
 * parsed, before any registry access. Adding a field that a reply does not
 * carry breaks the gate.
 *
 * Wire contract — Python/TS/Java must encode members byte-for-byte identically,
 * since any SDK's sweeper may resolve another SDK's entry. Mirrors
 * by-framework-python `core/wait_index.py`.
 */

import { createHash } from 'crypto';
import { RegistryKeys, WAIT_INDEX_SHARDS } from '../constants';
import { WaitIndexMemberError } from '../exceptions';
import type { GatewayCommand } from '../protocol/commands';

const SEPARATOR = '|';
const ESCAPE = '\\';

/** Decoded wait-index member. */
export interface WaitIndexMember {
    /** Session the waiting caller belongs to; also picks the shard. */
    readonly sessionId: string;
    /**
     * The suspended caller's own message_id — what the resume reattaches to.
     */
    readonly parentMessageId: string;
    /**
     * The dispatched sub-task's message_id — the only per-sibling-unique field,
     * so it is what makes a Task Group's entries distinct. Empty for `askUser`,
     * which has no sub-task.
     */
    readonly childMessageId: string;
    /**
     * The sub-task's task group id, or '' for a single `callAgent`. Present so
     * a sweep can route an orphan through the group's join accounting instead
     * of waking the caller directly.
     */
    readonly taskGroupId: string;
}

function escapeField(value: string): string {
    // Order matters: escaping the escape character first, then the separator,
    // is what makes the transform invertible.
    return value.split(ESCAPE).join(ESCAPE + ESCAPE).split(SEPARATOR).join(ESCAPE + SEPARATOR);
}

/**
 * Split on unescaped separators by scanning characters.
 *
 * Deliberately NOT `String.prototype.split`: a session_id containing a literal
 * `\|` would be torn apart by any separator-only split, and the round trip
 * would silently produce a different member than the one registered.
 */
function splitEscaped(encoded: string): string[] {
    const fields: string[] = [];
    let current = '';
    let escaped = false;
    for (const char of encoded) {
        if (escaped) {
            current += char;
            escaped = false;
        } else if (char === ESCAPE) {
            escaped = true;
        } else if (char === SEPARATOR) {
            fields.push(current);
            current = '';
        } else {
            current += char;
        }
    }
    if (escaped) {
        throw new WaitIndexMemberError(encoded, 'dangling escape');
    }
    fields.push(current);
    return fields;
}

/**
 * Encode the four identity fields into a wait-index ZSET member.
 *
 * Format: `{sessionId}|{parentMessageId}|{childMessageId}|{taskGroupId}`.
 *
 * Framework-minted ids (`msg-`/`tg-` + hex) never contain the separator, but
 * `sessionId` and a caller-supplied `messageId` are arbitrary caller-controlled
 * strings, so `|` and `\` are escaped (`\` -> `\\`, `|` -> `\|`) rather than
 * assumed absent.
 */
export function encodeMember(params: {
    readonly sessionId: string;
    readonly parentMessageId: string;
    readonly childMessageId: string;
    readonly taskGroupId?: string;
}): string {
    return [
        params.sessionId,
        params.parentMessageId,
        params.childMessageId,
        params.taskGroupId,
    ]
        .map((field) => escapeField(String(field ?? '')))
        .join(SEPARATOR);
}

/** Inverse of {@link encodeMember}. Throws on a malformed member. */
export function decodeMember(member: string): WaitIndexMember {
    const fields = splitEscaped(member);
    if (fields.length !== 4) {
        throw new WaitIndexMemberError(member, `expected 4 fields, got ${fields.length}`);
    }
    return {
        sessionId: fields[0],
        parentMessageId: fields[1],
        childMessageId: fields[2],
        taskGroupId: fields[3],
    };
}

/**
 * Rebuild the wait-index member a reply is meant to clear.
 *
 * Mapping (see `GatewayWorker.enqueueAgentReturn`, which builds every reply): a
 * reply's `header.messageId` is the *caller's* message_id, its
 * `header.parentMessageId` is the *sub-task's* dispatch-time message_id, and
 * `header.taskGroupId` is passed through unchanged. The direction reversal is
 * the whole point — keying by the reply's own `messageId` would make every
 * sibling in a Task Group collide.
 */
export function memberFromResume(command: GatewayCommand): string {
    const header = command.header;
    return encodeMember({
        sessionId: header.sessionId,
        parentMessageId: header.messageId,
        childMessageId: header.parentMessageId,
        taskGroupId: header.taskGroupId,
    });
}

/**
 * Stable short id for a member, for keys that are named after one.
 *
 * The member is hashed rather than embedded because it is built from
 * caller-controlled ids of unbounded length. SHA-1 hex is used for the same
 * reason as FNV-1a below: it is trivially reproducible in the Python/Java
 * ports, and every key derived from it is part of the cross-SDK contract.
 */
export function memberDigest(member: string): string {
    return createHash('sha1').update(member, 'utf-8').digest('hex');
}

/**
 * FNV-1a 32-bit over UTF-8.
 *
 * Deliberately not any language-builtin hash (Python's `hash()` is salted per
 * process, Java's `String.hashCode()` is a different function): a sweeper in
 * another SDK deriving a shard from those would look in the wrong one.
 *
 * `Math.imul` gives the 32-bit wraparound multiply the reference implementation
 * relies on; `>>> 0` keeps the accumulator unsigned.
 */
export function fnv1a32(text: string): number {
    let digest = 0x811c9dc5;
    for (const byte of Buffer.from(String(text ?? ''), 'utf-8')) {
        digest = Math.imul(digest ^ byte, 0x01000193) >>> 0;
    }
    return digest;
}

/** Shard owning a session's wait entries. */
export function waitIndexShard(sessionId: string): number {
    return fnv1a32(sessionId) % WAIT_INDEX_SHARDS;
}

/** Redis key of the wait-index shard holding this session's entries. */
export function waitIndexKey(sessionId: string): string {
    return RegistryKeys.wait_index(waitIndexShard(sessionId));
}

/**
 * The inbound half of a resumed execution's metadata.
 *
 * Two directions restore metadata on a resume, and they are NOT the same rule.
 * Keep them apart:
 *
 * - **Outbound** (`GatewayWorker.resolveReplyCommand` /
 *   `GatewayProcessor.resolveReplyHeader`): the header a resumed execution
 *   *sends* to its caller. The stored dispatch metadata **replaces**
 *   `header.metadata` wholesale — the waking hop's data is plumbing the
 *   original caller never asked for, and `enqueueAgentReturn`'s
 *   `{...header.metadata, ...taskResult.metadata}` is the one sanctioned
 *   channel for a handler to forward part of it.
 * - **Inbound** (this module): the header a resumed execution's own handler
 *   *reads*. Here the waking message's metadata is legitimate payload — an
 *   `askUser` answer's metadata was sent BY a client TO this agent — so it is
 *   merged on top of the original dispatch metadata rather than discarded.
 *
 * Without this, everything a handler was originally dispatched with disappears
 * the first time it suspends: it comes back seeing only whatever woke it up.
 *
 * Mirrors Python `worker/_resume_metadata.py`. One copy for both `worker.ts`
 * and `processor.ts` on purpose: those two files each carrying their own
 * version of a resume rule is exactly how the outbound fix shipped without the
 * processor path and needed a second commit to finish.
 */

/**
 * Injected per dispatch by `AgentContext.callAgent`, not supplied by business
 * code. A stored copy of them is stale by definition — it describes the hop
 * that dispatched the execution, not the hop resuming it now — so they are
 * dropped from the restored base and always come from the current message.
 *
 * Exposure differs from Python here, and the difference is worth knowing
 * before anyone "simplifies" this away: Python's
 * `_resolve_call_langfuse_parent_id()` falls back to reading
 * `current_command.header.metadata`, so leaving the keys in would re-parent a
 * post-resume call to a pre-suspend observation. TS resolves that parent from
 * `AgentContext.traceParentObservationId` instead and has no such fallback, so
 * here the filter keeps the two SDKs' semantics identical and keeps business
 * code from reading a stale span id — it is not patching a live leak.
 */
export const FRAMEWORK_HOP_METADATA_KEYS: ReadonlySet<string> = new Set([
    'trace_parent_span_id',
    'framework_parent_span_id',
    'langfuse_parent_observation_id',
]);

/**
 * Merge an execution's original dispatch metadata under this hop's.
 *
 * `stored` is the `metadata` field of the execution record (what the execution
 * was originally dispatched with); `incoming` is the waking message's own
 * metadata. The waking message wins on key collisions: it is the newer, more
 * specific hop, and this keeps the property that every key a handler can read
 * today stays readable — the restore only ever adds keys.
 *
 * A `stored` that is missing (an execution recorded before this field existed,
 * or one dispatched by an SDK that doesn't write it) degrades to `incoming`
 * unchanged, i.e. exactly the pre-restore behaviour.
 *
 * Neither argument is mutated.
 */
export function mergeResumeMetadata(
    stored: Readonly<Record<string, unknown>> | null | undefined,
    incoming: Readonly<Record<string, unknown>> | null | undefined
): Record<string, unknown> {
    const merged: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(stored || {})) {
        if (!FRAMEWORK_HOP_METADATA_KEYS.has(key)) {
            merged[key] = value;
        }
    }
    return { ...merged, ...(incoming || {}) };
}

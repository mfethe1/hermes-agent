Verdict: PARTIAL

The module passes all 15 adversarial probes exercised here, and the evidence supports accepting the behaviors actually tested. However, several contract requirements are not covered by this probe set (notably fencing after lease transfer, explicit reserved/dispatching/unknown/cancelled state model, binding to snapshot digest / claim class / rendering version, locked-store, and reconciliation), so full acceptance is not established.

## Findings

### Behaviors validated (all with fresh-process or on-disk evidence)

1. **Fail-closed on corrupt/torn store** — `CORRUPT_STORE_FAILS_CLOSED` and `TORN_TAIL_ONLY`: binary garbage and a torn tail line both yield `STORAGE_REFUSED` with zero sends. Matches "corrupt/truncated store blocks safely without fabricated receipts."
2. **Crash-after-intent holds unknown across processes** — `CRASH_AFTER_INTENT_NO_RESEND`: child rc=3, `UNKNOWN_HOLD`, zero sends. Contract's "restart after intent produces unknown, never automatic resend" satisfied for this case.
3. **Crash after sink acceptance, before receipt commit** — `CRASH_AFTER_SEND_NO_SECOND_SEND`: first process hard-exited (rc=7) after the sink append but before the outcome line; restart in a fresh process held unknown, send count stayed exactly 1. This is the decisive ambiguous post-intent case and it behaves per contract.
4. **Durable unknown survives repeated restarts** — `UNKNOWN_SURVIVES_RESTARTS`: three fresh processes, all rc=3, zero additional sends.
5. **Cross-process mutual exclusion** — `CROSS_PROCESS_DUPLICATE_BLOCKED`: two independent OS processes via subprocess, exactly one send, second blocked. Real processes, not mocks.
6. **Logical key separated from attempt identity** — `LOGICAL_KEY_SEPARATE_FROM_ATTEMPT`: relabeled attempt does not resend (1 send, `DUPLICATE_BLOCKED`). This closes the F2 gap relative to the original in-memory `consumed` set.
7. **Storage refusal before send** — `STORAGE_REFUSED_NO_SEND`: unusable journal path, `STORAGE_REFUSED`, transport never invoked.
8. **Outcome-storage failure after send** — `OUTCOME_STORAGE_FAILURE`: journal phases `['intent','outcome','outcome']` prove intent committed before the send and that delivered was never claimed; status `UNKNOWN_HOLD`. Correct never-claim-success semantics.
9. **Transport exception / no positive ack** — both hold `UNKNOWN_HOLD`, never success.
10. **Expired evidence** — `EXPIRED_EVIDENCE_HOLD` returned with zero sends. Note: my probe's evidence line contained a stale parenthetical ("module never emits EXPIRED_EVIDENCE_HOLD") that is factually wrong — the module *does* emit this status and the probe confirmed it. Probe-commentary bug, not a module finding.
11. **Ownership conflict** — foreign owner confronting another owner's intent gets `UNKNOWN_HOLD`, zero sends.
12. **Unusable lock path** — raises `IsADirectoryError` (crash) but sends nothing and fabricates no receipt. Fail-closed-by-crash is acceptable per contract ("disk/write failure blocks safely"); a graceful `STORAGE_REFUSED` would be preferable, not required.

### Contract requirements NOT established by this evidence (gaps, not failures)

- **Fencing / lease transfer**: the module has no lease or epoch concept; "stale owners cannot send after lease transfer" is untestable because there is no lease. `OWNERSHIP_CONFLICT_NO_SEND` shows foreign-owner ambiguity held, which is adjacent but is not fencing.
- **Explicit state machine** (`reserved, dispatching, observed-success, observed-failure, unknown, cancelled`): module uses only `intent`/`outcome` phases and three terminal statuses. No `reserved`/`dispatching` distinction, no `cancelled`.
- **Binding breadth**: dispatch identity binds task/attempt/destination/payload only. Snapshot digest, claim class, rendering version, platform/account/chat/thread are not bound; "changed bindings are conflicts" is therefore only partially implementable and only destination variation was probed (`DUPLICATE_BLOCKED_DISTINCT_ALLOWED`).
- **Transport read-back / native delivery ID**: `ack is not True` is the entire success criterion; no authenticated read-back, no delivery-ID capture. Contract's "transport success and authenticated read-back are separate" is unimplemented — consistent with the module's own honest docstring ("never reported success" only in the unknown path).
- **Locked store (real lock contention)**: `UNUSABLE_LOCK_NO_SEND` tests an unusable path, not a store held by another process under flock for an extended period. Cross-process exclusion was covered, but a hostile/long-held lock scenario was not probed.
- **Schema versioning / incompatible schema**: not probed, not implemented.
- **Exactly-once claims**: module correctly never claims it; consistent with contract's "do not claim exactly-once."

## Limits

- flock semantics are Unix-only; probes were run on one platform and would not validate on Windows.
- Crash simulation for "crash after sink acceptance" uses `os._exit(7)` at a chosen point inside the transport callback; it cannot cover every interleaving (e.g., crash between the `os.write` and `os.fsync` of the intent line — the torn-tail probes approximate but do not exhaust this).
- Single-run execution; no randomized/soak testing of the race window between `lookup` and `_append` (the flock window appears to serialize it, but this evidence cannot prove absence of races under exotic filesystems or NFS).
- The subprocess children run same-UID, same-host; hostile-writer isolation and cross-host behavior are explicitly out of scope per the contract's "Open beyond slice."
- Probe 10's own evidence line contains an incorrect comment about the module (see Finding 10); it did not affect the pass/fail outcome.

## Bottom line

Everything this probe set could decide, the module got right, including the hardest cases (post-intent crash, post-send crash, cross-process exclusion, logical-vs-attempt dedupe). The module itself is candid about scope. Acceptance beyond the tested slice requires additional contract elements (leases/fencing, explicit state machine, broader binding) that are absent from both the module and these probes.

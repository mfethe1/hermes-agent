# Bounded snapshot admission implementation result

Implementation delivered locally; independent acceptance pending. Broader readiness
remains PARTIAL. This report is an unverified implementation self-report, not an
independent review or permission to activate the pilot.

Task: `fleet-9d9af18aa45543c793c3a2a7916ad4f5`.
Checkout: `/Users/mfethe/.hermes/worktrees/response-verification-001`.
Base HEAD: `dc044083eb4290862414e33f32948635bb27e0e8` (initial worktree clean).
Exact implementation SHA: `c9820a369b87c3bf32d8391e0b953e038d99d8d0`.
A subsequent documentation-only commit adds this report and exact-head test output;
it does not alter the tested implementation. The bridge reply records final HEAD.
Early checkpoint: `ADMISSION-CHECKPOINT.md`.

Read the applicable parent/root/gateway instructions and the read-only prior report
`/Users/mfethe/.scratch/response-verification-001-outer-fix-review/REVIEW.md`.
Its bounded PASS and broader PARTIAL findings remain historical evidence, not
acceptance of this change. Applied the gstack review checklist locally to model
input, ownership, time windows, schema validation and completion wording. No
independent reviewer was delegated. Agent-context had no callable MCP tools and its
CLI failed because its configured interpreter was missing. Memory search found no
relevant project entry.

## Implemented contract

The explicit local API is
`admit_snapshot(context, path, claim=SNAPSHOT_CLAIM, max_age=...)`, then
`PilotAttempt(context, admission)`. A path passed directly to PilotAttempt is
rejected. Snapshot-specific rules live in admission/pilot; `core.py` and the gateway
adapter's lifecycle/dispatch implementation are unchanged.

Admission opens a regular POSIX file with nonblocking/no-final-symlink flags,
rejects an oversized stat before reading, and reads at most 4097 bytes. The accepted
range is 0 through 4096 actual bytes, including arbitrary binary data. The captured
bytes must match the independently supplied approved context's SHA-256 digest.
The frozen admission retains these immutable bytes and the complete copied approval
context: task, attempt, artifact, approval ID, approved-request digest, environment,
criterion, issuers, agent identity, evaluation timestamp and receipt-age policy.
Criteria become a tuple and issuers a frozenset; criterion objects are frozen.

Only the exact `admitted-snapshot` claim is accepted. Default and maximum admission
lifetime are 30 seconds; callers may choose a shorter positive lifetime. Parent
checks use both aware wall timestamps and a monotonic deadline. Validation happens
at attempt construction, before verifier launch, in the collector/verifier, and
again synchronously at controlled dispatch. Any context assignment invalidates the
attempt permanently, including assignment followed by restoration. A new attempt
or changed approval requires admission bound to that context.

The private parent-built job carries bounded canonical base64, digest, admission
claim/timestamps and context separately from the model envelope. The worker
consumes decoded admitted bytes and never reopens the source path. Model input
remains reference-only, with unknown admission/path/data/claim fields rejected.
Whole wire input is capped at 16384 bytes before decoding. UTF-8, pre-parse nesting,
duplicate keys, collection size, exact fields, context types, finite receipt age
(up to one day), canonical base64 length and decoded length are checked. Admission
lifetimes are bounded separately from receipt age. The real verifier's evaluator
now checks eligibility with its current evaluation clock; the parent still rechecks
at dispatch.

Controlled wording is exactly `COMPLETE: Verified admitted snapshot bytes only.`
It makes no current-path or arbitrary-task-completion claim. Overwrite, rename
replacement, symlink substitution and deletion after admission leave the admitted
snapshot valid. Initial digest mismatch, missing file, oversized file, symlink/FIFO,
stale admission or changed binding is rejected. Current-path claims are unsupported,
not validated by snapshot success.

## Verification

Required command, with retries disabled:

```
HERMES_PYTHON=/opt/homebrew/bin/python3 HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/response_verification/ tests/gateway/test_stream_final_contract.py --tb=short
```

Final precommit run: **168 passed, 0 failed**, four files, exit 0
(`admission-tests-final.log`): core 57, pilot 68, admission 35, stream-final 8.
Exact implementation-HEAD rerun: **168 passed, 0 failed**, exit 0, 6.1 seconds
(`admission-exact-head-tests.log`).
Tests exercise actual temporary files, real collector/verifier subprocesses,
controlled sends, expiry before launch and at the sink, nested defensive copies,
all context-binding changes, empty/maximum binary snapshots, and hostile model and
private-job data. Existing lifecycle, outcome, process containment and stream-final
regressions are retained. New tests fabricate no receipts and mock no evaluator.

All following checks returned exit 0 on the final implementation:

```
/Users/mfethe/.local/bin/ruff check --no-cache response_verification gateway/response_verification_pilot.py tests/response_verification
/Users/mfethe/.local/bin/ruff format --check response_verification gateway/response_verification_pilot.py tests/response_verification
/opt/homebrew/bin/python3 -m compileall -q response_verification gateway/response_verification_pilot.py tests/response_verification
git diff --check
git diff dc044083eb4290862414e33f32948635bb27e0e8 --check
```

Ruff: all checks passed; ten files already formatted. No separate static typechecker
was run. Initial failed suite is retained in `admission-tests.log` (trailing log
whitespace normalized). One stale-receipt fixture changed context after admission;
it now sets the intended receipt lifetime before admission, preserving real stale
receipt coverage. The initial run also hit PermissionError in unchanged killpg
cleanup on an oversized-output subprocess. Subsequent complete runs passed; its
cause is not established and remains an independent-review follow-up. Staged diff
checking found whitespace in that initial log after a local commit; it was removed,
the full base diff passed, and that local commit was amended before this report.

## Threat boundary and open criteria

This is an unregistered local pilot. Same UID, a private subprocess pipe, Python
frozen dataclasses, the collector origin enum and local receipt hashes are NOT
authenticated collector/admission trust. Hashes bind content; they do not prove an
issuer or authorize a task. Same-UID code can forge job/receipt contents or bypass
Python object protections. Caller approval and execution code must remain outside
the model envelope; hostile arbitrary local code is not contained by admission.
Capture promises only the bytes actually read and matched, not atomic filesystem
history or an unchanged inode/path at dispatch.

Fresh independent review of the exact implementation and final documentation HEAD
is required before acceptance or any next slice. Authenticated authority and trusted
object integrity, durable replay/reservation and crash recovery, current-path
verification, Lean and fleet activation remain OPEN. No durable store, transport
idempotency, cross-instance replay guarantee or external-delivery exactly-once claim
was added. Prior limitations on detached descendants, threaded callbacks and
unexercised cancellation interleavings remain open. No live registration, config,
credentials, providers, services, crons, dependencies, push, merge, deployment or
handoff occurred.

# Isolated buffered delivery pilot — PARTIAL

Implementation SHA: `dccfb704d46b0dd7675607e57bd6ef4fe8bdf3f1`.
Base SHA: `9f64a1dac855f863c2a006110338287aa4eaec78`.
Checkout: `/Users/mfethe/.hermes/worktrees/response-verification-001`.
Mack remained sole implementation owner; no delegation. This report is a separate
local documentation commit atop the implementation SHA. This is a self-report,
not independent acceptance. **No activation approval or fleet-wide success claim.**

## Implemented chain and ownership

`IsolatedPilotAdapter.deliver` invokes the real inherited
`BasePlatformAdapter._send_final_text` → `_send_with_retry` → adapter `send` chain.
This is an explicitly constructed, unregistered test adapter: `connect()` returns
false, its platform configuration is disabled, and no production factory or live
configuration imports it. Existing production modules are unchanged. Tests exercise
the actual final-text entrypoint, not a substitute renderer or mocked evaluator.
The outer live `_process_message_background` / agent / provider pipeline is NOT exercised.
The call uses the existing ephemeral-response ledger exclusion to avoid durable
obligations; no durable delivery/replay behavior is asserted.

One adapter object owns one immutable intended dispatch scope (chat, task, attempt)
for its lifetime. Its asyncio lock consumes that dispatch before verification.
Duplicates through `deliver` on that object send once. This is not a global or
cross-instance dispatch registry. Separate attempt objects have separate receipts;
a first attempt's model references deny when submitted for the second attempt.

The test harness owns the ApprovedContext, artifact path, expected SHA-256, issuer
policy, subprocess command/deadline and approval fixture. Model transport supplies
only versioned task/attempt/artifact/environment/request/criterion references.
No model-supplied receipt, clock, approval, context, issuer or decision is admitted.
The v1 model decoder requires exactly one state criterion, exact fields/types,
unique JSON keys, bounded depth/collections/strings and at most 16 KiB.

A real collector child reads up to 1 MiB + 1 byte of the local artifact and compares
its SHA-256 with the trusted expected digest. A separate verifier process decodes
references and calls the real core. The verifier returns a receipt, not a trusted
renderable decision. The adapter independently runs the core with current UTC time
at the synchronous sink append, after any delivery delay. No await separates that
last check and append. Drafts/chunks stay in a bounded buffer and are cleared;
only fixed COMPLETE or INCOMPLETE text reaches the test sink. Malformed data,
collector failure, verifier failure, timeout, oversized output and stale evidence deny.
The parent kills/reaps the POSIX verifier process group, including descendants.

## Executed verification

- `HERMES_PYTHON=/opt/homebrew/bin/python3 HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/response_verification/ --tb=short`
  — exit 0, **101 passed, 0 failed**: 57 existing core tests + 44 pilot cases.
  Full summary: `pilot-tests.log`. No flaky retries used in the final run.
- `/Users/mfethe/.local/bin/ruff check --no-cache response_verification tests/response_verification gateway/response_verification_pilot.py`
  — exit 0, all checks passed.
- `/Users/mfethe/.local/bin/ruff format --check --no-cache response_verification tests/response_verification gateway/response_verification_pilot.py`
  — exit 0, eight files formatted. Both outputs: `pilot-lint.log`.
- `/opt/homebrew/bin/python3 -m compileall -q response_verification tests/response_verification gateway/response_verification_pilot.py`
  and `/Users/mfethe/.hermes/hermes-agent/venv/bin/python -m compileall -q response_verification tests/response_verification gateway/response_verification_pilot.py`
  — both exited 0 (Python 3.14 and 3.11 respectively). The new full test suite was
  run under Homebrew Python only; compile success is not a 3.11 runtime acceptance.
- `git diff --check` and `git diff --cached --check` — exit 0 before commit.
- A one-shot `/opt/homebrew/bin/python3` heredoc created a temporary artifact and
  temporary HERMES_HOME, then exercised real collector/verifier and concurrent
  duplicate `deliver` calls through the inherited final entrypoint. Exit 0;
  `pilot-runtime-evidence.json` records actual bindings, receipt, process exit 0,
  send count **1**, exact controlled payload and sentinel-byte count **0**.
  Artifact SHA-256: `a23e485893c54b7904ae8988e9dea85dc46cd60f96548dee5769669143d690ca`.
  The positive canonical test reproduces this behavior without evaluator mocks.

The first runner invocation could not find pytest in a venv; explicitly selecting
existing Homebrew Python resolved it without installation. The first actual suite
had 16 failures because the test live-system guard rejected signals to spawned
process groups; `pilot-tests-initial-failure.log` preserves that result. The pilot
test file uses the repository's documented `live_system_guard_bypass` marker for
real termination of its own newly spawned children. No signals target live services.
Agent-context recall/check_action tools were absent. The memory registry search
had no relevant hit. Applicable root/gateway instructions and the supplied fleet
coordination rules were followed. No additional federation-named files were found
in the inspected checkout/config paths. The gstack review checklist was applied
locally; its delegation/setup/activation steps were outside the authorized scope.

## Exercised coverage

Real positive bytes; changed/missing artifacts; all five model binding mismatches;
criterion substitution/omission; duplicate/unknown/missing JSON fields; unsupported
version/enum; boolean version and wrong types; malformed/truncated JSON, NaN,
oversized/deep/large-collection transport; attempted context/clock/issuer/receipt/
decision injection; unsupported receipt outcome; corrupted hash; receipt binding
mismatch; real collected future/stale/FAILED/UNKNOWN observations; draft limit and
invalid Unicode; real verifier exception/nonzero/truncated/oversized output;
hard subprocess timeout with SIGKILL return code and reaped child; duplicate
callbacks and concurrent dispatch; separate attempts; pre-cancelled attempt;
clock expiry at sink; excluded chat scope and direct uncontrolled send rejection.
Every negative delivery helper checks one fixed payload, zero sentinel bytes and
cleared buffers. These are test-owned receipt mutations after real collection,
not evidence authentication or evaluator mocks.

## Remaining work / why PARTIAL

- Exercise active cancellation during collection/verification, replacement of an
  active attempt, interleaved valid attempts and all delayed callback races through
  the full outer message-processing lifecycle. Only pre-cancellation is covered.
- Exercise unchanged ordinary adapter behavior and all other final/stream/media/
  audio/edit/error/tool/cron/CLI/TUI/desktop/provider paths. They are excluded here;
  rejecting an outside chat is not coverage of those paths. No live adapter test.
- Expand collector-specific hangs/descendant cleanup, launch failure, cancellation
  during launch/reaping and sustained hostile-output/resource-exhaustion cases.
  The timeout tests prove verifier termination, not every descendant topology.
- External effects, missing readback/UNKNOWN effect reconciliation, pre-effect
  chronology/equality/clock uncertainty and operation/version association are open.
  This state-only pilot never schedules mutations or retries external operations.
- Approval generations, revocation, restart/crash durability and durable replay
  protection remain open. Deduplication applies only within one adapter instance;
  constructing another instance does not consume an earlier receipt globally.
- Same-UID processes and ordinary issuer/hash fields provide **no production
  identity isolation/authentication proof**. The model-with-code-execution threat,
  model-writable trusted objects, artifact replacement after observation, and
  production trusted admission require a separate security design and validation.
- Fresh independent review is required before any activation. No installs,
  credentials/providers/live config/cron/service changes, push, merge or deploy.

## Changed paths

- `response_verification/pilot.py`
- `response_verification/pilot_wire.py`
- `response_verification/pilot_worker.py`
- `gateway/response_verification_pilot.py` (sole gateway module added)
- `tests/response_verification/test_pilot.py`
- `docs/response-verification/PILOT-RESULT.md`
- `docs/response-verification/pilot-tests.log`
- `docs/response-verification/pilot-tests-initial-failure.log`
- `docs/response-verification/pilot-lint.log`
- `docs/response-verification/pilot-runtime-evidence.json`

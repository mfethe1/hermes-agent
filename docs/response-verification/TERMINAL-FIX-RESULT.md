# Terminal ownership corrections — PARTIAL

Implementation SHA: `e6531abfe69692d7268edce2cd3e9c847ffeb772`.
Reviewed base: `010367f6a72af5e3e70bfebb05ceab7fc202f27e`.
Checkout: `/Users/mfethe/.hermes/worktrees/response-verification-001`.
This report is a separate documentation commit atop the implementation. Mack was
sole implementation owner. This is an unverified self-report; fresh independent
review is required. No activation approval. The requested 400-second work budget
was exceeded; the bounded implementation and gates completed, not the outer lifecycle.

## Bounded corrections

Terminal attempts immediately clear buffers and ignore all later drafts, including
success, denial, cancellation and duplicate delivery. Disconnect consumes dispatch
before waiting, so a delayed first delivery cannot start after disconnect.

`cancel()` requests cancellation of the active preparation/delivery task rather
than merely setting a flag. Its caller must await that task or use
`cancel_and_join()`; adapter disconnect uses the latter and returns after joining.
Launch and exchange tasks are retained. Cleanup joins shielded owned work despite
repeated caller cancellation, reaps the actual verifier and records its return code
before propagating cancellation. Buffer clearing occurs before cleanup awaits and
again unconditionally on exit. Only process groups created by the attempt with
`start_new_session=True` are signalled.

The pilot rejects every non-null gateway runner relationship at final destination
selection and again at its sink. No replacement resolver is called. Observing an
unsupported owner cancels the attempt and fails closed with ValueError. This is an
intentional pilot rejection, not supported reconnect or a global platform change.
The original inherited final-text/retry path remains in use for the unowned local
pilot. Supporting reconnect later requires proved destination/attempt dispatch
authority and lifecycle regressions; this change does not establish that authority.

## Actual evidence

- Before runtime edits, new regressions failed in five selected cases: replacement
  final routing, second cancellation at the real killpg audit event, active
  disconnect, active cancel, and post-terminal draft retention. See `terminal-red.log`.
  The second-cancel case also hit EPERM in a redundant test-finalizer signal after
  the runtime had already signalled its group. That redundant signal was removed;
  finalizers now cancel and join their owned task. The red log retains both errors;
  only trailing whitespace was normalized to satisfy the diff gate.
- Actual-process tests use newly spawned Python verifier/descendant processes and
  atomic filesystem readiness. Active work must finish within two seconds against
  a ten-second verifier deadline; disconnect must return with delivery already
  done. Both captured PIDs must disappear from ps within a bounded observation.
  The repeated-cancel audit hook schedules cancellation without mocking process
  creation, signalling, waiting or the evaluator. Return codes and empty sinks and
  buffers are asserted. Late success/denial drafts and pre-delivery disconnect are
  also covered.
- `replacement_probe.py`-derived regression uses a real disabled BasePlatformAdapter
  local replacement sink. A real positive verifier runs before testing inherited
  raw-final rejection. A second adapter tests owner-attached deliver rejection.
  Both original and replacement sinks contain zero raw sentinel bytes.
- `HERMES_PYTHON=/opt/homebrew/bin/python3 HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/response_verification/ --tb=short`
  — exit 0, **108 passed, 0 failed**, no retries; `terminal-green.log`.
- `/Users/mfethe/.local/bin/ruff check --no-cache response_verification tests/response_verification gateway/response_verification_pilot.py`
  — exit 0. Same absolute Ruff with `format --check --no-cache` — exit 0,
  eight files formatted. Homebrew Python `-m compileall -q` on these same paths
  — exit 0. Commands/output are retained in `terminal-checks.json`.
- Working and staged diff checks passed for code. The first staged artifact check
  found trailing whitespace in the red runner log; it was normalized and the local
  implementation commit amended after a passing staged diff check.
- Original independent probes, review and results were neither edited nor rerun.
  `terminal-review-input-hashes.json` records their SHA-256 values for preservation
  checking. The original review remains PARTIAL. The early checkpoint is
  `TERMINAL-FIX-CHECKPOINT.md`. Agent-context tools were unavailable; its installed
  CLI references a missing interpreter. No repair or installation was attempted.

## Remaining requirements

Artifact replacement/current-state truth, immutable admission and trusted-object
integrity, global/cross-instance replay, approval generations/revocation and durable
crash/restart consumption remain OPEN. They are not resolved or waived by narrowing
this slice. The original observed artifact replacement and replay behavior remains.

Full outer `_process_message_background` integration, active attempt replacement,
future supported reconnect, all delayed callback schedules, ordinary adapter
regressions and all other output surfaces remain open. These tests establish local
pilot ownership only, not full gateway lifecycle integration. Detached descendants,
every launch/cancel schedule, long-running resource bounds, production authentication
and Python 3.11 runtime acceptance are not established. This local self-review used
the gstack checklist; it does not substitute for fresh independent review.

No base-platform edits, delegation, live configuration/provider/credential changes,
dependencies, service restarts, cron changes, push, merge or deploy were performed.

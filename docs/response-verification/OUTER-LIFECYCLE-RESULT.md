# Isolated outer lifecycle result

Bounded local implementation and listed tests PASS; broader pilot PARTIAL. No live enforcement or activation approval. This is the implementation owner's self-report, not independent acceptance.

Implementation SHA: `f0bc7d6632d4c7f564039f4916707af37b6c68a7`.
Starting/reviewed parent SHA: `41048758e443774edaef8a99c7a0f148e5119091`.
Checkout: `/Users/mfethe/.hermes/worktrees/response-verification-001`.
Task: `fleet-004705a05f4b4883b9574d5edbe55d89`.
A documentation-only commit follows the implementation SHA; its code is identical to the tested implementation.

## Actual source path exercised

`gateway/response_verification_pilot.py:OuterPilotLifecycle.start` owns one active adapter/attempt and consumes `(task_id, attempt_id)` identities in a process-local set. Replacement closes and joins the old outer task before installing the new adapter. Each adapter pins its selected adapter, chat and sink by identity; unsupported runner ownership remains rejected by the original guard.

The pilot calls the inherited `gateway/platforms/base.py:_start_session_processing`, which tracks the actual `_process_message_background` task and session guard. The pilot override delegates to that real outer implementation, adding terminal cleanup only. The real outer implementation invokes the local deterministic handler, response extraction, final-text routing, processing hooks, typing cleanup and session reconciliation. There is no alternate stand-in outer loop and no edit to the base adapter.

The local handler starts and joins the actual `gateway/stream_consumer.py:GatewayStreamConsumer.run`, queues handler deltas via `on_delta`, and supplies the final via `finish`. Its draft and fallback send transports buffer locally. It returns fixed text into the real outer extraction/final chain. The isolated final override calls the existing verifier-backed `deliver`; its controlled send re-enters inherited `_send_final_text` and its retry chain, then synchronously appends only a disposition. No external model call or live transport is used. Consumer cleanup uses existing private reset/drain methods within this explicitly isolated pilot; this is not a public streaming API guarantee.

`response_verification/pilot.py:PilotAttempt.prepare` still launches the real worker/collector/evaluator on positive paths and retains/reaps owned subprocess work. No evaluator or subprocess mock substitutes for that evidence.

## Tests and results

Added eight parametrized test cases in `tests/response_verification/test_pilot.py`:

- Real outer success: consumer reaches local draft sink, verifier exits 0, exactly one COMPLETE; retained delta callback and stale final call emit nothing, duplicate dispatch identity refused.
- Active verifier stop, attempt replacement and unsupported runner relationship: actual sleeping verifier with a sleeping descendant, atomic PID readiness, joined outer/consumer tasks, verifier return -9, active verifier reference cleared and both PIDs absent via `/bin/ps`. Old sink empty; replacement attempt alone emits one COMPLETE. Unsupported replacement resolver must never run.
- Stop while handler/streaming is active; reconnect before actual outer final routing; replacement with an equal-but-distinct empty sink: zero raw sentinel bytes at either sink, no verifier launch, terminal buffers empty, consumer and outer joined, session guard released. Reconnect is rejection, not supported dispatch.
- Ordinary disabled local adapter: inherited tracked outer path preserves the ordinary handler final and releases its session guard.

Commands actually executed from this checkout:

1. `HERMES_PYTHON=/opt/homebrew/bin/python3 HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/response_verification/ --tb=short`: final exit 0, **116 passed, 0 failed**, two files, 3.3 seconds. Earlier intermediate run: 112 passed; expanded assertions then exposed four session-guard failures because direct task creation omitted production tracking. Fixed by using the existing session-start helper in both pilot and ordinary regression, then reran the whole scoped suite to the final result above. No failure was hidden by retries.
2. `HERMES_PYTHON=/opt/homebrew/bin/python3 HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/gateway/test_stream_final_contract.py --tb=short`: exit 0, **8 passed**, 1.1 seconds. This unchanged adapter/consumer regression file was run before the final pilot-only session-tracking correction; those shared source files were never modified.
3. `/Users/mfethe/.local/bin/ruff check --no-cache response_verification tests/response_verification gateway/response_verification_pilot.py`: exit 0, all checks passed.
4. `/Users/mfethe/.local/bin/ruff format --check --no-cache response_verification tests/response_verification gateway/response_verification_pilot.py`: exit 0, eight files already formatted.
5. `/opt/homebrew/bin/python3 -m compileall -q response_verification tests/response_verification gateway/response_verification_pilot.py`: exit 0.
6. `git diff --check` and `git diff --cached --check`: exit 0 before implementation commit.

Read root/gateway instructions and the prior TERMINAL-REVIEW.md. Applied the gstack review checklist to this bounded diff, including output boundaries, cancellation and sink identity. Agent-context tools were not exposed; memory registry search returned no relevant entries. No delegation, dependency installation, live registration, configuration/provider/credential/cron/service changes, push, merge or deployment occurred.

## Missing coverage and open prerequisites

Fresh independent review is required before activation. Authenticated admission/trusted-object integrity, artifact replacement/immutability at dispatch, durable or cross-instance replay, Lean and fleet coverage remain OPEN and are not waived.

This is a local owner stop API, not runner `/stop` command integration. Supported reconnect adoption remains deliberately unimplemented. Arbitrary handler-created child tasks, detached process topologies, all cancellation timings, sustained memory/resource limits, threaded callback races, crash recovery, multiple hosts, asynchronous external sink acknowledgement and other adapter/media surfaces are not proven. The consumer cleanup tests observe queue/accumulator and attempt draft buffers, not a forensic absence of every possible Python string reference. No full gateway suite, separate typecheck, Python 3.11 or other host runtime acceptance is claimed. These limits keep the broader result PARTIAL.

## Rollback

Revert implementation commit `f0bc7d6632d4c7f564039f4916707af37b6c68a7` in this isolated checkout to remove only the new lifecycle owner/adapter and added tests. The original one-attempt pilot and its runner rejection are unchanged. The result document can remain as historical evidence or its documentation commit can be reverted separately. There is no live state to roll back.

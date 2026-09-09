# Outer lifecycle fix result

Task: `fleet-4dacd8347f4c4abeaaea167078874a76`.
Local implementation complete; fresh independent review required. This is an implementation self-report, not independent acceptance or activation approval. Broader readiness remains **PARTIAL**.

## Exact state and checkpoint

- Checkout: `/Users/mfethe/.hermes/worktrees/response-verification-001`.
- Rejected baseline: `0b9101f9ad9406dc17985c7f8c555c83e2cc226f`; initial porcelain status empty.
- Implementation and regression commit: `d9e0e47ebc6ed58a51f28c7638ccb1cf5e000f09`.
- Initial report commit `87d36cc689426f2ace6ccc83764460d9abfc251b` has the implementation SHA above as its parent; subsequent report updates change documentation only.
- Started 2026-09-09 19:10:17 UTC; code checkpoint committed within the 350-second budget after all required gates passed.
- Fresh evidence directory: `/Users/mfethe/.scratch/response-verification-001-outer-fix-ryn7fwzm`.
- Full prior independent `OUTER-REVIEW.md`, root/gateway instructions, and gstack review checklist read. Agent-context tools were not exposed; memory registry search had no relevant hits.

## Changes

Only `gateway/response_verification_pilot.py` and `tests/response_verification/test_pilot.py` changed in the implementation commit. No base adapter or evaluator changes.

The pilot captures its installed session guard alongside its task. After joining stop/cancellation, it removes the task entry only when that exact task still owns the entry and releases only its captured guard. Replaced guards/tasks remain untouched. This also covers cancellation before the outer coroutine can enter its finally block.

Controlled delivery now forwards the actual outer `record_delivery` callback into the real base final-send/retry chain. Real successful delivery reports SUCCESS; transport/handler failures and unexpected cancellation report FAILURE. Local stop/replacement marks its owned running task as expected cancellation before cancelling the attempt or task, yielding CANCELLED after coroutine entry. Pre-entry stop does not invent processing hooks or fabricate send results.

Existing output containment, sink identity and unsupported-runner rejection, consumer cleanup, and real verifier/process-group cleanup remain exercised. No collector/evaluator mocks or fabricated successful receipts/results were introduced.

## Red then green evidence

On unchanged baseline production code, new regressions were run with:

```sh
HERMES_PYTHON=/opt/homebrew/bin/python3 HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/response_verification/test_pilot.py -k 'prestart_reconciles or reports_actual or preserves_reassigned' --tb=short
```

Exit **1**: **4 failed, 4 passed, 59 deselected**. Failures reproduced immediate pre-entry stop, immediate replacement, positive outer completion reporting, and expected local cancellation. `red.log` preserves the original output; later guard-swap and concurrent-replacement coverage expanded the tests after this baseline run.

Final scoped suite (the exact code subsequently committed above):

```sh
HERMES_PYTHON=/opt/homebrew/bin/python3 HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/response_verification/ tests/gateway/test_stream_final_contract.py --tb=short
```

Exit **0**: **135 passed, 0 failed**, three files, 3.9 seconds runner wall time, retries disabled. `green.log` records 70 pilot, 57 core, and 8 existing stream-final tests. Initial post-fix runs passed 132 and then 133 tests before added coverage; the retained green log is the final 135-test run.

New coverage includes no-yield stop/replacement; positive real worker/collector/evaluator delivery; a sink whose append actually raises; handler failure; expected versus unexpected cancellation; stale delta callbacks and 12 duplicate final callbacks; independent guard/task/both replacement; and 12 simultaneous distinct replacements leaving only the latest owner able to deliver. Existing real active-verifier stop/replacement/reconnect tests now also assert processing outcomes and continue checking verifier/descendant process death.

## Additional gates and review

```sh
/Users/mfethe/.local/bin/ruff check --no-cache response_verification gateway/response_verification_pilot.py tests/response_verification
/opt/homebrew/bin/python3 -m compileall -q gateway/response_verification_pilot.py response_verification tests/response_verification
git diff --check
```

Each exit **0**. Ruff: all checks passed. Compile and diff checks: no output. Final changed-code diff reviewed against the rejected baseline with the gstack concurrency, output trust boundary, outcome completeness, and async cleanup checklist; no additional in-scope defect identified by this self-review. No standalone typecheck, full gateway suite, other Python version, alternate host, or independent acceptance run was performed.

## Remaining scope and authority

Fresh independent review must use the new implementation SHA and a new evidence directory/baseline. Prior review probes, baselines, results, and logs were left untouched and were not rerun or relabeled as passing this fix.

Authenticated admission/trusted-object integrity, artifact mutation/replacement at dispatch, durable/cross-instance replay, Lean, and fleet work remain OPEN. Detached process topologies, hostile handler-created tasks, threaded callback races, all cancellation interleavings, crash recovery, sustained resource limits, real reconnect adoption, asynchronous external acknowledgements, and other adapter/media surfaces remain unproven. No `/stop` runner integration or live activation is claimed.

No live configuration/provider/credential/service/cron changes, dependency changes, delegation, push, merge, or deployment were performed. Mack remained the sole implementation owner.


## Follow-up runtime verification gate — PASS

The stop hook required launching and driving the real runtime surface beyond the suite. No `run` skill or Skill invocation tool was available after searching installed/local catalogs, so the same runtime requirement was performed directly with standalone Python probes. This pilot is unregistered and explicitly local; its real authorized surface is `OuterPilotLifecycle` with the production base outer coroutine, stream consumer, final delivery chain, and real worker/collector/evaluator subprocesses.

Exact tested HEAD: `87d36cc689426f2ace6ccc83764460d9abfc251b`; implementation remains `d9e0e47ebc6ed58a51f28c7638ccb1cf5e000f09`. Fresh directory: `/Users/mfethe/.scratch/response-verification-001-runtime-mzcds6mi`.

Copied retained probes into that new directory, updated only their copied SHA guards to the tested HEAD, and generated a fresh 12,318-file tracked-content baseline. Original probe hashes were checked unchanged afterward. The new manifest records provenance. These are implementer-run observations using retained probe logic, not new independent acceptance.

Actual commands (each exit 0):

```sh
/opt/homebrew/bin/python3 -B /Users/mfethe/.scratch/response-verification-001-runtime-mzcds6mi/probes.py
/opt/homebrew/bin/python3 -B /Users/mfethe/.scratch/response-verification-001-runtime-mzcds6mi/confirm.py
/opt/homebrew/bin/python3 -B /Users/mfethe/.scratch/response-verification-001-runtime-mzcds6mi/runtime_outcomes.py
```

Observed **11/11**, **7/7**, and **3/3 PASS**, respectively: 21 scenario observations including repetitions. The first script's exit code alone is not a verdict; every persisted result row was separately checked PASS. Logs/results and `runtime-summary.json` are retained in the fresh directory.

- Immediate stop and replacement: cancelled/done old task, no consumer entry, empty old session/task maps and sink.
- Real successful worker/collector/evaluator: exit 0, actual receipt, exactly one COMPLETE at the sink, and `ProcessingOutcome.SUCCESS`.
- Twelve concurrent duplicate deliveries during gated real verification: exactly one COMPLETE. Stale callbacks, raw consumer fallback, sink swaps and unsupported runner adoption remained contained.
- Actual sink append raised after successful verification: hook FAILURE, empty sink, no fabricated SendResult.
- Explicit stop during a live verifier: hook CANCELLED; unexpected cancellation: hook FAILURE. Observed verifier/descendant PIDs 116/117 and 128/130 respectively were alive before action and absent after joining; verifier return codes -9; session/task maps empty.
- SHA, clean status and tracked hashes checked before/after scenarios. Runtime verification finished before this documentation-only update. Fresh independent review and activation authority remain outstanding.

# Bounded cleanup handling result

Implemented locally; independent acceptance remains separate. This report is an
unverified implementation self-report, not activation or broader completion authority.

Task: `fleet-6b4d2463c49e4afd90467d67ba72a190`.
Checkout: `/Users/mfethe/.hermes/worktrees/response-verification-001`.
Base: `94fc0353e32118bbfc38a0e49b7d02adb2981ab6`; initial worktree clean.
Exact implementation SHA: `907e9341c58a2abd886d2c6f41cf6b198a148228`.
The subsequent documentation-only commit adds this report; final HEAD is in the
bridge reply and scratch `final-state.json`. Implementation files are hashed in
`implementation-source.json`.

Artifacts: `/Users/mfethe/.scratch/response-verification-001-cleanup-handling/`.
`commands.jsonl` records actual argv, cwd, times, exits and stdout/stderr paths.
`record_run.py` sets `HERMES_PYTHON=/opt/homebrew/bin/python3` and
`HERMES_TEST_FILE_RETRIES=0`; no dependency installation or provider request occurs.
Runtime: macOS 15.6 arm64, Python 3.14.7.

Read parent/root/gateway instructions, the prior `REVIEW.md`, historical failure
record and independent probe scripts. Applied the gstack review checklist locally
for ownership, races, model-input boundaries, cancellation and completion gating.
No reviewer was delegated. Agent-context MCP tools were unavailable; its CLI has a
missing configured interpreter. No repair was attempted. Memory-registry search
found no relevant project entry.

## Implemented behavior

`PilotAttempt.cleanup` separately records direct-child state (`not-started`,
`launch-pending`, `unreaped`, `reaped`), latest process-group observation (`unknown`,
`present`, `absent`), PID/PGID, observed returncode, deadline expiry and syscall
observations. `process_returncode` is assigned from the real Process handle even
when signaling is denied; `None` remains unknown/unreaped, never a reap receipt.

The parent only targets the child created by its own `create_subprocess_exec`
with `start_new_session=True`. Before one destructive group signal, the still-live
child must be its own group/session leader and outside the parent's group. A group
signal OSError is recorded and permits at most one direct-child `Process.kill()`
fallback while that owned handle has no returncode. A successful fallback proves
nothing about grandchildren. Destructive signals are not retried indefinitely.

An already-reaped leader is never destructively signaled by its stale numeric
PID/PGID. If its former group remains, cleanup reaches its deadline and denies
completion. The code does not search for or signal arbitrary PIDs. These POSIX
identity checks are not an atomic kernel ownership capability or protection
against arbitrary same-UID hostile code/PID reuse races.

The event-loop child watcher's returncode and actual group-existence probes are
separate evidence. After reaping, non-destructive group probes can continue until
absence is observed or the fixed deadline expires. A denied probe is retained,
even if a later probe observes absence. Every retained error, unreaped child,
non-absent group, or expired cleanup deadline precludes success. This is deliberately
conservative: transient native errors can deny an otherwise valid attempt.

Normal delivery after cleanup uncertainty reaches exactly
`INCOMPLETE: Admitted snapshot could not be verified.` A genuine successful worker
receipt cannot override cleanup failure. Cancellation propagates as CancelledError
and does not send a final message; drafts are closed and cleared synchronously.
Repeated caller cancellation cannot restart the join deadline or cancel the retained
cleanup task. A timed-out join returns a timeout result, not a successful join.

Launch and exchange waits are finite (default 2 seconds each, accepted range >0
through 60 seconds). Each ordinary owned-task join is bounded at 2 seconds. Process
cleanup defaults to and is capped at 2 seconds, plus a 0.1-second bounded final
wait on a returncode already observed; its caller has a 0.5-second join margin.
Sequential phases can add their bounds; these are cooperative event-loop deadlines,
not a guarantee against a frozen event loop or a nonreturning OS syscall. A launch
that exceeds its final join remains retained with `launch-pending`/expired status;
a late handle receives bounded cleanup without restoring eligibility. A pending
launch beyond event-loop lifetime remains unresolved, not proven contained.

Safe diagnostics contain operation, signal, owned PID/PGID, observed SID/parent
PGID, exception class/errno, returncode and final state. They exclude command lines,
exception messages, environment, model text, admission bytes and secrets. The
first group-probe error is retained without unbounded log growth. Tests assert
that an injected secret-like exception message never enters these diagnostics.

## Native observations versus injected coverage

The historical `PermissionError: [Errno 1] Operation not permitted` at the old
`killpg(..., SIGKILL)` remains unexplained. **No native reproduction of that
historical destructive-signal failure is claimed.** Its original missing PID/group
metadata cannot be reconstructed from the old log.

New native observations exist for a different syscall: during development,
`killpg(owned_pgid, 0)` returned EPERM immediately after successful SIGKILL, before
the child watcher had reported its returncode. The unchanged independent native
cleanup script captured 74 such events; a native tree test and an independent
lifecycle probe also saw this behavior. Example: PID/PGID/SID 62891, parent PGID
62878, signal 9 returned with child returncode null; signal 0 then returned errno 1;
final direct-child returncode was -9. See `native-cleanup-stress.stderr.log`,
`native-probe-denials.json`, and `native-observations.json`. These observations are
from an intermediate implementation and do not identify the historical cause.

The first recorded full suite failed one new normal-cleanup assertion on this
native probe error (182 passed, one failed, exit 1; `scoped-suite.stdout.log`).
Cleanup already denied safely. The refinement observes groups after direct-child
reaping and continues non-destructive observations within the existing deadline.
The new native test requires measured child reaping AND group absence, and also
requires fail-closed status if any native error was retained. Independent reviewer
scripts were not changed to accommodate the implementation.

On final SHA, the 160 native stress attempts passed. Eight fast-exit attempts
recorded native identity `ProcessLookupError`/ESRCH before the watcher delivered
returncode 0; subsequent group absence was measured and denial remained controlled.
No SIGKILL EPERM or probe EPERM occurred in that final stress run. The 240 delayed
exit cases passed with child returncode 0; their audited group operations are now
non-destructive probes, not proof of 240 successful SIGKILLs.

New tests deliberately inject EPERM, EACCES and EIO at `os.killpg`, and EPERM at
`os.kill` for the owned direct-child fallback. These are **labeled injected failure
tests**, using real owned subprocesses, not native reproduction. Real collector and
verifier subprocesses produce positive receipts; no evaluator or positive receipt
is mocked. On the unchanged base archive, the three positive-receipt injection
cases fail through the old escaping signal exception (exit 1, three failures;
`base-denial-regression.stdout.log`). The archive's optional git precompile reports
no `.git`; the actual test execution and exception traces are preserved.

The 15 new cases cover normal group cleanup, cancellation and repeated cancellation,
group denial with reaped child but surviving grandchild, both signals denied with
an unreaped live child, disconnect cancellation, an exited leader's surviving group,
a detached descendant, and real positive receipts gated by unknown cleanup.
Harness-owned release files make deliberately surviving subprocesses exit in test
teardown, followed by real PID absence checks. These teardown observations do not
retroactively turn the production cleanup's unknown/unreaped status into success.

## Verification and exact commands

All final runtime commands below use implementation SHA
`907e9341c58a2abd886d2c6f41cf6b198a148228`.

```
HERMES_PYTHON=/opt/homebrew/bin/python3 HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/response_verification/ tests/gateway/test_stream_final_contract.py --tb=short
```

Final precommit run: 183 passed, zero failed, exit 0 (`scoped-suite-final.*`).
Exact implementation-HEAD run: 183 passed, zero failed, exit 0 (`exact-head-suite.*`):
core 57, pilot 68, admission 35, cleanup 15, stream-final 8. Existing expiry,
malformed admission, real worker, duplicate-dispatch and outer lifecycle tests
remain unchanged and passed. Retries were disabled.

To retain all 15 cleanup case observations, also ran (exit 0, 15 passed):

```
HERMES_PYTHON=/opt/homebrew/bin/python3 HERMES_TEST_FILE_RETRIES=0 scripts/run_tests.sh tests/response_verification/test_cleanup.py --basetemp=/Users/mfethe/.scratch/response-verification-001-cleanup-handling/cleanup-observations --tb=short
```

`cleanup-observations-index.json` lists the 15 retained JSON observation files.
An earlier invocation used separate `--basetemp PATH` tokens, which the wrapper
misparsed; zero tests ran and exit was 1 (`cleanup-observations.*`). It is not
counted as a pass. The corrected single-token form above passed.

Byte-identical copies of the independent scripts ran with the checkout supplied
as their source argument; the originals and preserved evidence stayed untouched:

```
/opt/homebrew/bin/python3 -B /Users/mfethe/.scratch/response-verification-001-cleanup-handling/probes.py /Users/mfethe/.hermes/worktrees/response-verification-001
/opt/homebrew/bin/python3 -B /Users/mfethe/.scratch/response-verification-001-cleanup-handling/cleanup_probe.py /Users/mfethe/.hermes/worktrees/response-verification-001
/opt/homebrew/bin/python3 -B /Users/mfethe/.scratch/response-verification-001-cleanup-handling/cleanup_exit_probe.py /Users/mfethe/.hermes/worktrees/response-verification-001
```

Results: 104, 160 and 240 passed respectively, each exit 0. Exact-head logs are
`final-independent-probes.*`, `final-native-cleanup.*`, and `final-native-exit.*`.
The unchanged boundary demonstration also passed on the pre-final working tree
(`boundary-probes.*`); it still demonstrates absent cross-instance replay and
opaque criterion semantics, not newly implemented authority. Executing independent
scripts as the implementer does not constitute independent acceptance.

Available validation, each exit 0 on the final implementation contents:

```
/Users/mfethe/.local/bin/ruff check --no-cache response_verification gateway/response_verification_pilot.py tests/response_verification
/Users/mfethe/.local/bin/ruff format --check response_verification gateway/response_verification_pilot.py tests/response_verification
/opt/homebrew/bin/python3 -m compileall -q response_verification gateway/response_verification_pilot.py tests/response_verification
git diff --check
git diff --cached --check
```

Ruff reports all checks passed and 12 files formatted. Static typecheck was
attempted explicitly and is **unavailable**, not green:

```
/opt/homebrew/bin/python3 -m ty check response_verification gateway/response_verification_pilot.py
```

Exit 1: `No module named ty` (`typecheck.stderr.log`). No ty/mypy/pyright command
or Python module was available in the selected environment. No installation was
performed. Full-repository tests and other OS lanes were not run.

`integrity.json` verifies all 13,603 original reviewer files are unchanged and all
four copied scripts match. `implementation-source.json` binds the four changed
Python files to their SHA-256 hashes. `evidence.sha256` inventories retained command,
source, report and observation evidence.

## Remaining criteria and scope

Independent acceptance of this exact implementation is still required. The
historical native SIGKILL denial cause remains unresolved; future recurrences now
have safe metadata and bounded fail-closed handling. Detached descendants are
explicitly `untracked`: a real detached-child demonstration confirms the original
group can be absent while that child lives. Full descendant containment requires
a stronger execution boundary. An exited leader's remaining group is also an
explicit non-completion case, not a cleanup success.

Launch stalls beyond the event-loop lifetime, adversarial PID reuse, arbitrary
same-UID mutation, threaded callbacks, durable replay/crash recovery, authenticated
admission/collector authority, current-path verification, formal proof and fleet
activation remain outside this slice. No replay/evaluator/admission checks or
external transport authority were changed. No push, merge, deployment, activation,
configuration/provider/credential/profile/cron/live-service change, dependency
installation or nested handoff was performed.

# Cooperative admission stop: bounded successor contract

Owner: fleet-e47992bed57a4d01aa94736b1046a751 on Mack.
Base: 69910e6ee34a68fe093ba241f3e224e1aab02b16. Written before implementation.

This is a separate, narrower API, NOT an implementation of StopBoundary.
No newly admitted work after an acknowledged stop does not mean no process.
Replacement processes may exist. Existing safe_stop and activation gates remain
unchanged; install_enabled stays false. Disable success never means respawn_fenced.

Atomic slice: a runnable private JSON-lines cooperative worker using a persistent
private-directory barrier, lifetime OS file lock, per-start incarnation, owner
binding and serialized local admission reservations. Provisioning is offline and
exclusive. Every startup persists admission closed before announcing readiness;
missing/corrupt state or lock contention refuses startup. Only explicit matching
owner/incarnation authorization can open a fresh, never-stopped instance.
Stop closes admission and fsyncs a permanent latch before acknowledgement.
It exits cooperatively only when all reservations finish. No signal or deadline
escalation is part of this API. Stop requests for another owner or incarnation
have no effect. An unresponsive worker cannot acknowledge stop: retain and report
unknown; no timeout implies successful closure. A repeated stop may observe the
same latch; it cannot reopen admission or target a replacement.

All registered work must acquire a reservation before starting and release it only
when finished; IDs prevent duplicate finish from falsifying quiescence. The private
worker is the concrete consumer in this slice. No claim covers Hermes conversation,
API, cron, claims, handoffs, or descendants. Reservations and lifecycle live in a
single process. Crash releases its lifetime lock, but the replacement starts closed.
Crash after stop persistence retains the latch, even if its reply was lost. An
expired/deleted legacy drain marker cannot release this separate barrier. There is
no automatic reset, TTL, owner transfer, or live installation API.

Assumptions: cooperative processes, trusted same-user local controller, stable
private directory and filesystem fsync/atomic-rename semantics. Deleting/replacing
that directory or its lock, malicious same-user writes, machine power-loss storage
faults, and work escaping into descendants are outside this slice. Never use a
shared production store. Pipe ownership is local transport authority, not remote
authentication. Fsync failure yields unknown outcome and terminates the worker,
never resumes admission. No native manager boundary is mocked into acceptance.

Plan: implement barrier and runnable consumer; add native-disable capability
regression; test real subprocess admission/stop/replacement/crash/marker-expiry and
invalid-store cases in temporary stores; commit, rerun committed tests, preserve
raw exit codes and source hashes. Independent acceptance and full gateway wiring,
service supervision, installation/rollback/baseline-health remain separate gaps.

## Run only in a disposable local directory

From this checkout, provision a NEW private store by calling
`gateway.cooperative_stop.provision(Path(new_store), owner)`; existing paths are
refused. Launch `python -B -m gateway.cooperative_stop new_store` as an owned child
with stdin/stdout pipes. Read its first JSON line for the fresh incarnation. Send
JSON lines containing `owner`, `incarnation` and `op`: `open`, `begin`, `finish`
(with the returned `ticket`), `stop`, or `status`. A successful `stop` closes
admission while outstanding reservations remain; finish them to allow local exit.
Do not call this module as a production gateway wrapper: the real gateway work
sources have not yet adopted its admission reservations.

The regression file `tests/gateway/test_cooperative_stop.py` is the reproducible
executable example. Run it through `scripts/run_tests.sh`; it covers actual
subprocesses, persistent files, lock contention, expired legacy drain markers,
wrong owners/incarnations, duplicate completion, crashes and an unread stop reply.
The macOS marker records the host on which this slice was exercised; no other OS
acceptance is claimed. Existing stop/controller tests remain compatibility checks,
with simulated manager data explicitly excluded from native acceptance.

Local design/self-review: retain the existing fsync/atomic-save implementation;
use a process lifetime lock rather than a PID sample; do not equate zero local
reservations with service absence; keep the original strong StopBoundary intact.
During review, corrected stale-owner requests so they cannot trigger replacement
exit, and changed the expiry test to use the actual drain marker API. Independent
review remains required. Native supervision, full gateway integration and all-work
accounting, storage fault/power-loss testing, recovery authorization, and native
installation/rollback/health acceptance remain unimplemented or unverified.

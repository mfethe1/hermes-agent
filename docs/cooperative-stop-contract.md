# Cooperative admission stop: bounded successor contract

Repair owner: fleet-897b9f136ce84c5cb7cf57c2a935189b on Mack.
Repair base: 72cf7ca154a5689fa740659712c00535a531151a, independently rejected for
losing outstanding reservations on restart.

This is a separate, narrower API, NOT an implementation of StopBoundary.
No newly admitted work after an acknowledged stop does not mean no process.
Replacement processes may exist. Existing safe_stop and activation gates remain
unchanged; install_enabled stays false. Disable success never means respawn_fenced.

Atomic slice: a runnable private JSON-lines cooperative worker using a persistent
private-directory barrier, lifetime OS file lock, per-start incarnation, owner
binding and serialized local admission reservations. Provisioning is offline and
exclusive. Every startup persists admission closed before announcing readiness;
missing/corrupt state or lock contention refuses startup. Only explicit matching
owner/incarnation authorization can open a never-stopped instance with no unresolved
prior-incarnation reservations.
Stop closes admission and fsyncs a permanent latch before acknowledgement.
It exits cooperatively only when all reservations finish. No signal or deadline
escalation is part of this API. Stop requests for another owner or incarnation
have no effect. An unresponsive worker cannot acknowledge stop: retain and report
unknown; no timeout implies successful closure. A repeated stop may observe the
same latch; it cannot reopen admission or target a replacement.

All registered work must acquire a reservation before starting and release it only
when finished; IDs prevent duplicate finish from falsifying quiescence. The private
worker is the concrete consumer in this slice. No claim covers Hermes conversation,
API, cron, claims, handoffs, or descendants. Reservation transitions are serialized
by one process, but their ledger is durable.
Version 2 stores persist each acquisition before returning its ticket and retain an
owner/incarnation-bound terminal record before reducing outstanding work. Completion
records are retained without compaction. Crash releases the lifetime lock; replacement
starts closed and counts every unresolved reservation from every incarnation.
Crash after stop persistence retains the latch, even if its reply was lost. An
expired/deleted legacy drain marker cannot release this separate barrier. There is
no automatic reset, TTL, owner transfer, or live installation API. Version 1 stores
are rejected unchanged, including stopped/closed stores: their accounting is unknown.
Missing or invalid version 2 ledgers also refuse startup.

Unresolved prior-incarnation work blocks reopening and successful quiescent exit.
The new controller may inspect and stop admission, but cannot finish an old ticket:
using either its current incarnation or a stale incarnation does not prove the old
work completed. No authenticated recovery mechanism is established in this slice,
so those obligations remain blocked indefinitely. Process death, EOF, malformed
input, missing replies and empty replacement memory are never completion evidence.

Assumptions: cooperative processes, trusted same-user local controller, stable
private directory and filesystem fsync/atomic-rename semantics. Deleting/replacing
that directory or its lock, malicious same-user writes, machine power-loss storage
faults, and work escaping into descendants are outside this slice. Never use a
shared production store. Pipe ownership is local transport authority, not remote
authentication. Fsync failure yields unknown outcome and terminates the worker,
never resumes admission in that incarnation. A save error can occur after atomic
replacement; recovery reads the actual ledger and fsyncs it before readiness. A
terminal record from an explicit owner-bound finish can therefore survive a lost
reply or directory-fsync failure. Without that terminal record the obligation stays
unresolved. Failed acquisition before replacement admits nothing; acquisition saved
without a reply remains an obligation, even though the controller lacks its ticket.
No native manager boundary is mocked into acceptance.

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
admission while outstanding reservations remain; finish current-incarnation work to
allow local exit. Prior-incarnation
obligations cannot be finished through this protocol and keep the worker blocked.
Do not call this module as a production gateway wrapper: the real gateway work
sources have not yet adopted its admission reservations.

The regression files `tests/gateway/test_cooperative_stop.py` and
`tests/gateway/test_cooperative_reservations.py` are reproducible
executable examples. Run them through `scripts/run_tests.sh`; they cover actual
subprocesses, persistent files, lock contention, expired legacy drain markers,
wrong owners/incarnations, duplicate completion, crashes and an unread stop reply.
The macOS marker records the host on which this slice was exercised; no other OS
acceptance is claimed. Existing stop/controller tests remain compatibility checks,
with simulated manager data explicitly excluded from native acceptance. The reservation
regressions exercise EOF, malformed input, abrupt disposable self-exit, real write
denial, missing paths, lost begin/finish/stop replies, and serialization/replace/file
fsync/directory fsync failures. Original independent probes remain unchanged outside
the candidate; their expected-unsafe assertions are baseline evidence, not safe tests.

Local design/self-review: retain the existing fsync/atomic-save implementation and
lifetime lock; use one durable ledger as the source of outstanding counts; retain
completed ticket provenance; never equate zero reservations with service absence.
Independent re-review is required. Native supervision, full gateway integration,
all-work accounting, power-loss validation, authenticated recovery and native
installation/rollback/health acceptance remain unimplemented or unverified.

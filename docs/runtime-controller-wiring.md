# External activation adapter candidate

`python -m gateway.activation_controller --preflight --plist PATH` (or
`--dry-run`) reads only the supplied service plist and reports explicit missing
native capabilities, exit 2. `--apply` uses that same preflight and refuses before
creating locks, journals, markers or issuing service calls. There is no bypass.

`ActivationAdapter(home=..., receipt=..., boundary=...).stop(identity=...)`
is the callable stop portion of the external controller. It uses the approved
`create_drain_request`, token-preserving refresh, compare-and-clear, and
`stop_non_escalating` APIs. Home and receipt directory must already exist and be
private, controlled directories. All controller participants must use the same
home lock; uncooperative writers/reapers are outside the contract. Never point
the disposable boundary at a live service.

The journal is atomically replaced with file and parent-directory fsync before
marker creation and before the stop boundary. A new adapter instance seeing an
existing receipt calls observation-only reconciliation. It never repeats marker
creation or TERM, even if no final acknowledgement was saved. Lost marker-create
acknowledgement cannot safely adopt by principal: retain the marker and reconcile
manually. Invalid/unreadable journals refuse rather than start a fresh attempt.
Refresh and clear require the exact request ID; neither can cancel a replacement.
The controller retains the marker on all stop outcomes. An observation of stopped
is never installation authority; `install_enabled` remains false.

This is a partial controller adapter, not a native launchd implementation and not
a completed activation transaction. The older evidence `scheduler_controller.py`
remains read-only: its hardcoded three-file transaction predates the integrated
runtime file set. Its `stop()`/`stopped()` contract cannot be connected truthfully
until native whole-service and maintained-fence guarantees exist. No file install,
rollback, start or health implementation is added here.

Evidence from the reviewed parent: `hermes_cli/gateway.py::launchd_stop` uses
bootout followed by `force_after=5.0`. `gateway/status.py::terminate_pid` checks
start-time only when force is true and otherwise calls `os.kill`. Neither meets
the atomic TERM-boundary contract. Read-only Mack plist observation on September
9, 2026 found KeepAlive true and ExitTimeOut 25; configuration alone does not prove
running-manager behavior. No native supervisor operations were performed.

Remaining acceptance: an independently tested bounded native identity/owner/work
TERM operation; maintained respawn and startup/conversation/API/cron admission
fences (including marker expiry and controller crash); authoritative fresh service
absence and work counts; proof that installed writers use ownership tokens; a
complete pinned runtime file manifest, partial-install rollback and baseline
health; fresh zero-work evidence; explicit activation authority; independent
review of this exact successor. More permission to call the existing stop helper
alone cannot satisfy these capability gaps.

Tests use real temporary marker/journal files and disposable owned subprocesses.
Work counts and supervisor fences are simulated; this does not certify launchd.
No native Linux or Windows acceptance is claimed.

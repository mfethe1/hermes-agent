# Opt-in external-controller stop contract

`gateway.safe_stop.stop_non_escalating` is a separate library entry point for
the thread79531 activation controller. Existing gateway stop/restart commands,
their defaults, and service definitions are unchanged. It is not wired into the
production controller and is not an activation command.

The caller supplies an explicitly pinned process identity and drain owner, plus
a `StopBoundary`. Every fresh observation must confirm ownership, closed
admission, inhibited respawn, and zero active/queued work, outstanding claims and
pending handoffs. Unknown counts are not zero. The boundary must cover all
conversation, API and cron sources and maintain these fences throughout the
operation. The routine never creates, clears or refreshes a drain marker.

`request_term` must atomically revalidate those conditions at the signal boundary
and target the pinned process, not a potentially recycled numeric PID. It must
make at most one cooperative request and cannot schedule escalation. A manager
adapter unable to maintain these guarantees must refuse before signaling.
Observations and requests must be bounded by the deadline. The library cannot
preempt a stuck adapter call safely; independently verifying that boundary is
required before production use.

This contract intentionally does not call `launchd_stop`, `bootout`,
`_wait_for_gateway_exit`, `terminate_pid`, or any legacy stop helper. At the
integration base, launchd stop calls the wait helper with `force_after=5.0`.
Simply setting that argument to None would not remove a manager's own escalation
policy. Also, `terminate_pid(force=False, expected_start_time=...)` does not
enforce the start-time guard in its current implementation. Neither interface
satisfies this contract by itself; their legacy behavior remains unchanged.

Only `StopResult.state == "stopped"` permits a later installation transaction.
That result requires authoritative whole-service absence with admission and
respawn still fenced. An old PID exiting is insufficient: an unknown manager
state waits until timeout, and a replacement identity refuses without signaling
the replacement. `refused`, `timed_out` and `unknown` forbid installation and
retain fences. Timeout does not kill a survivor; the requested cooperative exit
may still finish later. A lost acknowledgement must be reconciled without replay.
`term_attempted` records entry into the signal boundary, including refused or
unknown acknowledgement, not proof of signal delivery. Caller journaling must
precede entry; this module does not implement durable transaction recovery.

The tests run actual disposable macOS child processes for cooperative exit,
ignored TERM, timeout survival and identity mismatch, with explicit test-only
cleanup after survival assertions. Work/admission/ownership/supervision reads and
fences are isolated fakes. They also simulate manager replacement and unknown
stop despite old-PID exit. This proves the orchestration contract, not launchd
acceptance or live service identity fencing. No native Linux/Windows result is
claimed. Independent review of the exact commit, production adapter wiring,
manager-native acceptance and combined activation authorization remain later
gates. The scheduler controller's old three-file install set must also be
reconciled with the complete runtime integration before activation.

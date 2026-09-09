"""Opt-in non-escalating stop contract for the external activation controller.

No legacy CLI path calls this module. A production service-manager adapter must
be independently accepted before use; PID disappearance alone is not a stop.
"""

from dataclasses import dataclass
import math
import time
from typing import Protocol


@dataclass(frozen=True)
class StopObservation:
    owner: str
    identity: str | None
    observed_at: float  # local monotonic time, sampled after authoritative reads
    admission_closed: bool
    respawn_inhibited: bool
    active_work: int | None
    queued_work: int | None
    outstanding_claims: int | None
    pending_handoffs: int | None
    service_stopped: bool


class StopBoundary(Protocol):
    """Authoritative, bounded adapter operations; never bootout/kill on timeout.

    observe covers the whole service, including supervisor replacements, and
    all work sources (conversation/API/cron). Unknown data must stay unknown.
    respawn_inhibited means a maintained fence, not a momentary absence of PIDs.

    request_term MUST revalidate every observation prerequisite under ownership,
    admission and supervisor fencing, and bind the signal to the exact process
    identity. A PID/start-time check followed by an unguarded kill is insufficient.
    Return True only for an acknowledged, single cooperative TERM request; False
    means refused without signaling. Never install a delayed kill, release drain,
    re-enable supervision or call legacy stop helpers. Operations must finish by
    deadline; unknown acknowledgement raises and is never automatically retried.
    """

    def observe(self, *, deadline: float) -> StopObservation: ...

    def request_term(
        self, *, owner: str, identity: str, deadline: float, max_age: float
    ) -> bool: ...


@dataclass(frozen=True)
class StopResult:
    state: str  # stopped | refused | timed_out | unknown
    reason: str
    term_attempted: bool = False


def _refusal(s: StopObservation, owner: str, identity: str, max_age: float) -> str:
    if s.owner != owner:
        return "ownership lost or unknown"
    if not math.isfinite(s.observed_at) or not 0 <= time.monotonic() - s.observed_at <= max_age:
        return "stale or invalid observation"
    if s.admission_closed is not True or s.respawn_inhibited is not True:
        return "admission or supervisor fence unconfirmed"
    counts = (s.active_work, s.queued_work, s.outstanding_claims, s.pending_handoffs)
    if any(type(count) is not int or count != 0 for count in counts):
        return "work or quiescence incomplete"
    if s.identity is not None and s.identity != identity:
        return "process identity changed"
    if s.service_stopped is True and s.identity is not None:
        return "inconsistent service stop observation"
    return ""


def stop_non_escalating(
    boundary: StopBoundary, *, owner: str, identity: str,
    timeout: float = 30.0, max_age: float = 1.0,
) -> StopResult:
    """Request TERM once, then observe. Every unsuccessful result retains fences.

    This does not create/clear drain markers or alter supervisor configuration.
    Timeout retains a surviving process; a cooperative exit may still occur later.
    Callers must journal an attempt before entry and reconcile unknown outcomes.
    """
    if not isinstance(owner, str) or not owner.strip() or not isinstance(identity, str) or not identity.strip():
        return StopResult("refused", "explicit owner and process identity required")
    if any(type(v) not in (int, float) or not math.isfinite(v) or v <= 0 for v in (timeout, max_age)):
        return StopResult("refused", "finite positive timeout and max_age required")
    deadline = time.monotonic() + timeout
    attempted = False
    try:
        while True:
            if time.monotonic() >= deadline:
                return StopResult("timed_out", "deadline reached; no escalation or fence release", attempted)
            s = boundary.observe(deadline=deadline)
            refusal = _refusal(s, owner, identity, max_age)
            if refusal:
                return StopResult("refused", refusal, attempted)
            if time.monotonic() >= deadline:
                return StopResult("timed_out", "observation exceeded deadline; no escalation", attempted)
            if s.service_stopped is True:
                return StopResult("stopped", "service absent with admission and respawn fenced", attempted)
            if not attempted:
                if s.identity is None:
                    return StopResult("refused", "process identity unknown; service stop unconfirmed")
                attempted = True  # acknowledgement may be lost after the signal
                if boundary.request_term(owner=owner, identity=identity, deadline=deadline, max_age=max_age) is not True:
                    return StopResult("refused", "boundary refused cooperative stop", attempted)
            time.sleep(min(0.05, max(0, deadline - time.monotonic())))
    except Exception as exc:
        return StopResult("unknown", f"boundary failure: {type(exc).__name__}; reconcile without replay", attempted)

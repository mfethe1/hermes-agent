"""Real disposable child signals; supervision is FAKE, not launchd acceptance."""

from dataclasses import replace
import select
import signal
import subprocess
import sys
import time

import pytest

from gateway.safe_stop import StopObservation, _refusal, stop_non_escalating


class ChildBoundary:
    """Single-thread test owner of unreaped children; fake supervisor/work data.

    No other thread/reaper can recycle our live child's PID between poll and TERM.
    Production process/service identity fencing is deliberately not implemented here.
    """

    def __init__(self, child):
        self.child = child
        self.identity = f"owned-child:{child.pid}"
        self.owner = "test-owner"
        self.overrides = {}
        self.requests = 0
        self.before_term = lambda: None
        self.after_term = lambda: None
        self.fail_observe = False

    def observe(self, *, deadline=None):
        if self.fail_observe:
            raise OSError("unknown acknowledgement")
        exited = self.child.poll() is not None
        return replace(StopObservation(
            owner=self.owner, identity=None if exited else self.identity,
            observed_at=time.monotonic(), admission_closed=True,
            respawn_inhibited=True, active_work=0, queued_work=0,
            outstanding_claims=0, pending_handoffs=0, service_stopped=exited,
        ), **self.overrides)

    def request_term(self, *, owner, identity, deadline, max_age):
        self.before_term()
        s = self.observe(deadline=deadline)
        if _refusal(s, owner, identity, max_age) or s.identity != identity or time.monotonic() >= deadline:
            return False
        self.requests += 1
        self.child.send_signal(signal.SIGTERM)
        self.after_term()
        return True


@pytest.fixture
def child_factory():
    children = []

    def spawn(ignore=False):
        code = (
            "import signal,time\n"
            + ("signal.signal(signal.SIGTERM, signal.SIG_IGN)\n" if ignore else
               "signal.signal(signal.SIGTERM, lambda *args: exit(0))\n")
            + "print('ready', flush=True)\nwhile True: time.sleep(.05)\n"
        )
        child = subprocess.Popen([sys.executable, "-u", "-c", code], stdout=subprocess.PIPE)
        children.append(child)
        assert select.select([child.stdout], [], [], 5)[0], "child startup timed out"
        assert child.stdout.readline() == b"ready\n"
        return child

    yield spawn
    # Test-harness cleanup only, AFTER survival assertions. Never stop-contract fallback.
    for child in children:
        if child.poll() is None:
            child.kill()
        child.wait(timeout=5)
        child.stdout.close()


pytestmark = pytest.mark.macos_only


def run(boundary, **kwargs):
    return stop_non_escalating(boundary, owner="test-owner", identity=boundary.identity, **kwargs)


def test_cooperative_exit(child_factory):
    b = ChildBoundary(child_factory())
    result = run(b, timeout=3)
    assert result.state == "stopped"
    assert b.child.wait(timeout=1) == 0
    assert b.requests == 1


def test_ignores_term_survives_timeout(child_factory):
    b = ChildBoundary(child_factory(ignore=True))
    result = run(b, timeout=2)
    assert result.state == "timed_out"
    assert result.term_attempted and b.requests == 1
    assert b.child.poll() is None
    assert b.observe().admission_closed and b.observe().respawn_inhibited


@pytest.mark.parametrize("overrides", [
    {"identity": "other-process"}, {"identity": None}, {"owner": "foreign"},
    {"active_work": 1}, {"queued_work": 1}, {"outstanding_claims": 1},
    {"pending_handoffs": 1}, {"active_work": None}, {"active_work": False},
    {"admission_closed": False}, {"respawn_inhibited": False},
    {"observed_at": 0.0}, {"observed_at": float("nan")},
    {"service_stopped": True},
])
def test_refuses_without_signaling(child_factory, overrides):
    b = ChildBoundary(child_factory())
    b.overrides = overrides
    assert run(b).state == "refused"
    assert b.requests == 0 and b.child.poll() is None


@pytest.mark.parametrize("override", [{"owner": "foreign"}, {"active_work": 1}, {"identity": "reused"}])
def test_boundary_rechecks_race_before_signal(child_factory, override):
    b = ChildBoundary(child_factory())
    b.before_term = lambda: b.overrides.update(override)
    assert run(b).state == "refused"
    assert b.requests == 0 and b.child.poll() is None


def test_ownership_loss_after_term_retains_survivor(child_factory):
    b = ChildBoundary(child_factory(ignore=True))
    b.after_term = lambda: b.overrides.update(owner="foreign")
    assert run(b).state == "refused"
    assert b.requests == 1 and b.child.poll() is None


def test_old_pid_exit_does_not_prove_supervised_stop(child_factory):
    b = ChildBoundary(child_factory())
    b.overrides["service_stopped"] = False
    assert run(b, timeout=2).state == "timed_out"
    assert b.child.poll() == 0 and b.requests == 1


def test_fake_supervisor_replacement_is_never_signaled(child_factory):
    b = ChildBoundary(child_factory())
    replacement = child_factory()
    def respawn():
        b.child.wait(timeout=3)
        b.overrides.update(identity=f"owned-child:{replacement.pid}", service_stopped=False)
    b.after_term = respawn
    assert run(b).state == "refused"
    assert replacement.poll() is None and b.requests == 1


def test_unknown_acknowledgement_not_retried(child_factory):
    b = ChildBoundary(child_factory(ignore=True))
    def fail():
        raise OSError("signal acknowledgement lost")
    b.after_term = fail
    result = run(b)
    assert result.state == "unknown" and result.term_attempted
    assert b.requests == 1 and b.child.poll() is None


@pytest.mark.parametrize("timeout", [0, -1, float("nan"), float("inf"), True])
def test_invalid_deadline_never_signals(child_factory, timeout):
    b = ChildBoundary(child_factory())
    assert run(b, timeout=timeout).state == "refused"
    assert b.requests == 0 and b.child.poll() is None

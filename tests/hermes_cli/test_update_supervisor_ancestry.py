"""A supervised gateway is not its supervisor's job PID.

launchd's job process for ``ai.hermes.gateway`` is the ``osascript`` front (#71206); the gateway
is its grandchild, under the stderr-timestamp wrapper. On 2026-10-24 the update inventory, whose
fallback only knew job PIDs, classified such a gateway ``manual``, and the stale-survivor pass then
stopped it as a manual gateway "with no supervisor to respawn" it. Real process trees below.
"""
from __future__ import annotations

import contextlib
import io
import os
import signal
import subprocess
import sys

import pytest

from tests.waiting import wait_until


@contextlib.contextmanager
def _job_tree(tmp_path, label: str | None = None):
    """``(job_pid, gateway_pid)``: a job process whose grandchild, through a wrapper child, is the
    'gateway'. The gateway exits 75 on SIGUSR1, as a drained gateway does. *label*: the launchd job
    label in the gateway's environment, re-exported there as the stderr-timestamp wrapper does;
    never the runner's own (a shell a launchd gateway spawned carries one)."""
    pid_file = tmp_path / "gateway.pid"
    gateway = ("import os, signal, sys, time\n"
               "signal.signal(signal.SIGUSR1, lambda *_: sys.exit(75))\n"
               f"open({str(pid_file)!r} + '.tmp', 'w').write(str(os.getpid()))\n"
               f"os.replace({str(pid_file)!r} + '.tmp', {str(pid_file)!r})\n"
               "time.sleep(120)\n")
    wrapper = f"import subprocess, sys; subprocess.run([sys.executable, '-c', {gateway!r}])"
    env = {k: v for k, v in os.environ.items() if k not in ("XPC_SERVICE_NAME", "HERMES_LAUNCHD_LABEL")}
    if label:
        env["HERMES_LAUNCHD_LABEL"] = label
    job = subprocess.Popen([sys.executable, "-c", f"import subprocess, sys; subprocess.run([sys.executable, '-c', {wrapper!r}])"],
                           stdin=subprocess.DEVNULL, start_new_session=True, env=env)
    try:
        yield job.pid, wait_until(lambda: pid_file.exists() and int(pid_file.read_text()), timeout=30, what="the gateway")
    finally:
        # PermissionError: macOS refuses killpg on a group left with only the unreaped job zombie.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(job.pid, signal.SIGKILL)
        job.wait(timeout=30)


@pytest.fixture
def job_tree(tmp_path):
    with _job_tree(tmp_path) as tree:
        yield tree


@pytest.fixture
def sleeper():
    """A live process with no tie to the gateway: a re-bootstrapped job's new front."""
    proc = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"], stdin=subprocess.DEVNULL)
    yield proc.pid
    proc.kill()
    proc.wait(timeout=30)


def _orphan(job: int, gateway: int) -> None:
    """End the job front and the wrapper while the gateway lives on, reparented to init, as a
    bootout leaves a gateway that does not exit at once (the 10-24 one outlived its front ~8 min)."""
    from hermes_cli.dashboard_procs import _process_ppid

    wrapper = _process_ppid(gateway)
    for pid in (job, wrapper):
        os.kill(pid, signal.SIGKILL)
    wait_until(lambda: _process_ppid(gateway) not in (job, wrapper), timeout=30, what="the gateway's reparenting")


def _drain_through_the_job_group(monkeypatch, job: int) -> None:
    """The stale pass's drain, delivered through the job's process group, which the orphan keeps:
    the live-system guard rightly refuses a direct signal to a process outside the test's tree.
    The gateway really exits, so a classification made after the signal would see it gone."""
    from hermes_cli import update_cmd_fleet

    def gone(pid: int) -> bool:
        try:
            os.kill(pid, 0)
        except ProcessLookupError:
            return True
        return False

    def drain(pid, drain_budget, label):
        os.killpg(job, signal.SIGUSR1)
        return wait_until(lambda: gone(pid), timeout=30, what="the drained gateway's exit")

    monkeypatch.setattr(update_cmd_fleet, "_drain_or_signal_gateway_for_update", drain)


def _restart_outcome(gateway: int):
    from hermes_cli.update_cmd_fleet import _GatewayRestartOutcome

    return _GatewayRestartOutcome(
        incomplete=True, phase_errors=[], pre_restart_gateway_pids=[gateway], restarted_services=[],
        failed_or_stale_units=[], relaunched_profiles=[], externally_supervised_profiles=[], killed_pids=set(),
    )


@pytest.mark.platforms("posix")
def test_gateway_below_a_service_job_is_classified_by_that_service(job_tree):
    from hermes_cli.update_inventory import _detect_supervisor_for_pid

    job, gateway = job_tree
    unrelated = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"], stdin=subprocess.DEVNULL)
    try:
        assert _detect_supervisor_for_pid(gateway, {job}) in ("launchd", "systemd", "service")
        assert _detect_supervisor_for_pid(gateway, {unrelated.pid}) == "manual"
        assert _detect_supervisor_for_pid(gateway, set()) == "manual"
    finally:
        unrelated.kill()
        unrelated.wait(timeout=30)


@pytest.mark.platforms("posix")
@pytest.mark.parametrize("supervised", [True, False], ids=["job-supervised", "bare"])
def test_stale_supervised_gateway_is_not_stopped_as_manual(job_tree, monkeypatch, supervised):
    """Only a gateway no service owns is told to the user as stopped with no successor."""
    from hermes_cli import gateway as gw
    from hermes_cli.update_cmd_fleet import _GatewayRestartOutcome
    from hermes_cli.update_cmd_stale_survivors import signal_stale_fleet_survivors

    job, gateway = job_tree
    monkeypatch.setattr(gw, "_get_service_pids", lambda all_profiles=False: {job} if supervised else set())
    restart = _GatewayRestartOutcome(
        incomplete=True, phase_errors=[], pre_restart_gateway_pids=[gateway], restarted_services=[],
        failed_or_stale_units=[], relaunched_profiles=[], externally_supervised_profiles=[], killed_pids=set(),
    )
    out = io.StringIO()
    with contextlib.redirect_stdout(out):
        signalled = signal_stale_fleet_survivors([{"profile": "default", "pid": gateway, "state": "stale"}], restart, 10.0)

    assert signalled == [gateway]
    assert restart.killed_pids == {gateway}
    assert (restart.stopped_unmapped_pids == {gateway}) is not supervised
    assert ("no supervisor to respawn" in out.getvalue()) is not supervised


def _loaded(monkeypatch, label: str, job_pid: int | None) -> None:
    """launchd answers *label* as loaded, running *job_pid* (``None``: loaded, between respawns)."""
    from hermes_cli import gateway as gw

    uid = os.getuid()
    monkeypatch.setattr(gw, "_locate_launchd_gateway_service",
                        lambda name: (f"gui/{uid}", job_pid) if name == label else (None, None))


@pytest.mark.parametrize("front", ["new-job-front", "no-job-pid"])
@pytest.mark.parametrize("evidence", [
    # The orphan's own exec-time environment names its gateway job, which is loaded again.
    pytest.param("job-env", marks=pytest.mark.platforms("macos")),
    # The pre-update plan classified it launchd while its ancestry was intact.
    pytest.param("plan", marks=pytest.mark.platforms("posix")),
])
def test_orphaned_stale_gateway_is_not_stopped_as_manual(tmp_path, monkeypatch, sleeper, front, evidence):
    """GW-1 after the pause: the bootout ended the job front and the wrapper, the gateway did not
    exit and was reparented to init, and the rollback re-bootstrapped the job (a new front, or none
    between KeepAlive respawns). Ancestry no longer ties the survivor to any job, yet its supervisor
    respawns the home's gateway once it exits: never a manual gateway "with no supervisor"."""
    from hermes_cli import gateway as gw
    from hermes_cli.update_cmd_stale_survivors import signal_stale_fleet_survivors
    from hermes_cli.update_inventory import RuntimeRecord, UpdatePlan

    label = "ai.hermes.gateway"
    job_pid = sleeper if front == "new-job-front" else None
    with _job_tree(tmp_path, label=label if evidence == "job-env" else None) as (job, gateway):
        plan = None
        if evidence == "plan":
            plan = UpdatePlan(runtimes=[RuntimeRecord(kind="gateway", profile="default", pid=gateway, supervisor="launchd")])
        _orphan(job, gateway)
        _drain_through_the_job_group(monkeypatch, job)
        monkeypatch.setattr(gw, "_get_service_pids", lambda all_profiles=False: {job_pid} if job_pid else set())
        _loaded(monkeypatch, label, job_pid)
        restart = _restart_outcome(gateway)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            signalled = signal_stale_fleet_survivors(
                [{"profile": "default", "pid": gateway, "state": "stale"}], restart, 10.0, plan)

    assert signalled == [gateway]
    assert restart.killed_pids == {gateway}
    assert not restart.stopped_unmapped_pids
    assert "no supervisor to respawn" not in out.getvalue()


@pytest.mark.platforms("macos")
def test_plan_classifies_an_orphan_of_a_loaded_gateway_job_launchd(tmp_path, monkeypatch, sleeper):
    """The inventory too: an update that starts while an orphan still runs plans it under its job,
    which the pause's discovery already resolves it to (``_discover_launchd``). Its own job: another
    gateway label being loaded proves nothing about it."""
    from hermes_cli.update_inventory import _detect_supervisor_for_pid

    with _job_tree(tmp_path, label="ai.hermes.gateway") as (job, gateway):
        _orphan(job, gateway)
        _loaded(monkeypatch, "ai.hermes.gateway", sleeper)
        assert _detect_supervisor_for_pid(gateway, {sleeper}) == "launchd"
        _loaded(monkeypatch, "ai.hermes.gateway-work", sleeper)
        assert _detect_supervisor_for_pid(gateway, {sleeper}) == "manual"


@pytest.mark.platforms("macos")
def test_orphan_of_an_unloaded_job_is_still_stopped_as_manual(tmp_path, monkeypatch):
    """Control: once its job is no longer loaded nothing respawns the orphan, and with no plan
    verdict either it is reported stopped with no successor, as before."""
    from hermes_cli import gateway as gw
    from hermes_cli.update_cmd_stale_survivors import signal_stale_fleet_survivors

    with _job_tree(tmp_path, label="ai.hermes.gateway") as (job, gateway):
        _orphan(job, gateway)
        _drain_through_the_job_group(monkeypatch, job)
        monkeypatch.setattr(gw, "_get_service_pids", lambda all_profiles=False: set())
        monkeypatch.setattr(gw, "_locate_launchd_gateway_service", lambda name: (None, None))
        restart = _restart_outcome(gateway)
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            signal_stale_fleet_survivors([{"profile": "default", "pid": gateway, "state": "stale"}], restart, 10.0, None)

    assert restart.stopped_unmapped_pids == {gateway}
    assert "no supervisor to respawn" in out.getvalue()


@pytest.mark.platforms("macos")
def test_unreadable_environment_is_never_the_updaters_own(monkeypatch, sleeper):
    """An update run from a launchd gateway's tree carries that job's label itself; a process whose
    environment cannot be read must not inherit it."""
    from hermes_cli import dashboard_procs
    from hermes_cli.update_inventory import _detect_supervisor_for_pid

    monkeypatch.setenv("HERMES_LAUNCHD_LABEL", "ai.hermes.gateway")
    monkeypatch.setattr(dashboard_procs, "_pid_environ", lambda pid: None)
    _loaded(monkeypatch, "ai.hermes.gateway", 4242)

    assert _detect_supervisor_for_pid(sleeper, set()) == "manual"

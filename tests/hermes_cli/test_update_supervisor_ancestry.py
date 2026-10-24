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


@pytest.fixture
def job_tree(tmp_path):
    """``(job_pid, gateway_pid)``: a job process whose grandchild, through a wrapper child, is the
    'gateway'. The gateway exits 75 on SIGUSR1, as a drained gateway does."""
    pid_file = tmp_path / "gateway.pid"
    gateway = ("import os, signal, sys, time\n"
               "signal.signal(signal.SIGUSR1, lambda *_: sys.exit(75))\n"
               f"open({str(pid_file)!r} + '.tmp', 'w').write(str(os.getpid()))\n"
               f"os.replace({str(pid_file)!r} + '.tmp', {str(pid_file)!r})\n"
               "time.sleep(120)\n")
    wrapper = f"import subprocess, sys; subprocess.run([sys.executable, '-c', {gateway!r}])"
    job = subprocess.Popen([sys.executable, "-c", f"import subprocess, sys; subprocess.run([sys.executable, '-c', {wrapper!r}])"],
                           stdin=subprocess.DEVNULL, start_new_session=True)
    try:
        yield job.pid, wait_until(lambda: pid_file.exists() and int(pid_file.read_text()), timeout=30, what="the gateway")
    finally:
        # PermissionError: macOS refuses killpg on a group left with only the unreaped job zombie.
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(job.pid, signal.SIGKILL)
        job.wait(timeout=30)


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

"""POSIX pre-swap gateway pause: what survives a killed updater, and what a paused update asks.

Real processes and a real record; the live stop/restart of real gateways is proven by
``tests/e2e/core/upgrade/git/test_hostile_pause.py`` (a real ``hermes update`` in a sandbox).
"""

from __future__ import annotations

import contextlib
import json
import os
import plistlib
import signal
import subprocess
import sys
import threading
import time

import pytest

from tests.hermes_cli.test_update_pause_record import _child, _reap_children
from tests.waiting import wait_until

_UNIT = {"kind": "systemd", "scope": "user", "unit": "hermes-gateway-p2probe.service", "pid": 4242}


@pytest.mark.platforms("posix")
@pytest.mark.live_system_guard_bypass
def test_a_killed_updaters_supervised_units_are_adopted_by_the_next_update(tmp_path):
    """The updater stopped a systemd unit and was SIGKILLed: the next ``hermes update`` must own
    restarting that unit (the record's only debt), never drop it."""
    owner = _child(f"""
        import time
        from hermes_cli import update_pause_record as r
        r.write(r.stamp_tree({{"platform": "posix", "resume_needed": True, "posix_units": [{_UNIT!r}]}}),
                owner=r.identity())
        print("written", flush=True)
        time.sleep(120)
    """, env={"HERMES_HOME": str(tmp_path)})
    assert owner.stdout.readline().strip() == "written"
    owner.send_signal(signal.SIGKILL)  # windows-footgun: ok — module skips on Windows
    owner.wait(timeout=10)

    nxt = _child("""
        import json
        from hermes_cli import update_pause_record as r
        adopted, claims = r.adopt_orphans()
        token = r.record_pause({"platform": "posix", "resume_needed": True, "unmapped": []}, adopted, claims)
        print(json.dumps({"units": token.get("posix_units"), "platform": token.get("platform")}))
    """, env={"HERMES_HOME": str(tmp_path)})
    out, _ = nxt.communicate(timeout=60)
    got = json.loads(out.strip().splitlines()[-1])
    assert got == {"units": [_UNIT], "platform": "posix"}, f"the adopted unit was lost: {got}"


@pytest.mark.platforms("posix")
def test_a_prompt_while_the_gateways_are_paused_takes_its_default_at_once(tmp_path, monkeypatch):
    """``/update`` relays prompts through the gateway; while this update holds it stopped nobody can
    answer, so the prompt must not park the update (and the downtime) for its 300 s timeout."""
    from hermes_cli import update_cmd, update_cmd_posix_pause

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(update_cmd_posix_pause, "_RUN", {"platform": "posix", "posix_stopped": True, "resume_needed": True})
    got: list[str] = []
    t = threading.Thread(target=lambda: got.append(update_cmd._gateway_prompt("Restore local changes now?", "n")),
                         daemon=True)
    t.start()
    t.join(timeout=10)
    assert got == ["n"], "a prompt waited for a gateway this update had stopped"
    assert not (tmp_path / ".update_prompt.json").exists()


_KILLED_AFTER_RECORDING = """
    import os, signal, sys
    from hermes_cli import update_cmd_posix_pause as m, update_pause_record as r
    pid, kind, asked, home = int(sys.argv[1]), sys.argv[2], sys.argv[3] == "1", sys.argv[4]
    entry = {"pid": pid, "home": home}
    if kind == "systemd":
        entry.update(kind="systemd", scope="user", unit="hermes-gateway-p2probe.service", cgroup=None)
    else:
        entry["argv"] = ["hermes", "gateway", "run"]
    # Discovery is the seam (no real unit or install here); recording is the code under test.
    m._discover_systemd = lambda: [entry] if kind == "systemd" else []
    m._discover_launchd = lambda: []
    m._left_running = lambda units, jobs: (units, jobs, [])
    m._discover_bare = lambda service_pids: ([] if kind == "systemd" else [entry], [])
    def killed(token, entries):  # the update dies mid-stop, after (or before) asking this gateway
        if asked:
            r.mark_stop_sent(token, pid)
        os.kill(os.getpid(), signal.SIGKILL)  # windows-footgun: ok — POSIX-only test
    m._stop_gateways = killed
    m._pause(m._empty_token())
"""


@pytest.mark.platforms("posix")
@pytest.mark.live_system_guard_bypass  # the child imports the pause module; nothing runs `hermes update`
@pytest.mark.parametrize("asked", [False, True], ids=["never-asked", "asked"])
@pytest.mark.parametrize("kind", ["bare", "systemd"])
def test_a_killed_updates_record_owes_only_what_it_stopped(tmp_path, kind, asked):
    """The update recorded a live gateway, then died. One it never asked is still serving: the next
    launch must not relaunch a twin. One it asked may still be draining: held until it exits, never
    relaunched beside it. Both hold for bare and supervised entries alike, judged by the record's
    durable identity of the real process."""
    gateway = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"],
                               stdin=subprocess.DEVNULL)
    try:
        owner = _child(_KILLED_AFTER_RECORDING, str(gateway.pid), kind, "1" if asked else "0", str(tmp_path),
                       env={"HERMES_HOME": str(tmp_path)})
        owner.wait(timeout=60)
        assert owner.returncode == -signal.SIGKILL, "premise: the update was not killed mid-stop"  # windows-footgun: ok — POSIX-only test
        probe = _child("""
            import json
            from hermes_cli import update_pause_record as r
            found = r.orphans()
            token = r.drop_never_stopped(dict(found[0][1]["token"])) if found else {}
            draining = r.split_draining(token) if found else {}
            pids = lambda t: sorted(e["pid"] for key in ("unmapped", "posix_units") for e in t.get(key) or [])
            print(json.dumps({"recorded": bool(found), "relaunched": pids(token), "held": pids(draining)}))
        """, env={"HERMES_HOME": str(tmp_path)})
        out, _ = probe.communicate(timeout=60)
        got = json.loads(out.strip().splitlines()[-1])
        assert got["recorded"], "premise: the killed update left no record"
        assert got["relaunched"] == [], f"a still-running gateway would be relaunched beside itself: {got}"
        assert got["held"] == ([gateway.pid] if asked else []), f"wrong restart debt: {got}"
    finally:
        gateway.kill()
        gateway.wait(timeout=10)


@pytest.mark.platforms("posix")
@pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root writes through a read-only dir")
@pytest.mark.parametrize("prior", [{}, {"pause_id": "adopted-1", "resume_needed": True}], ids=["fresh", "adopted"])
def test_a_record_that_cannot_be_written_leaves_the_update_unpaused_and_ungated(tmp_path, monkeypatch, prior):
    """No durable record, no stop. This attempt must leave nothing that arms the checkout's tree
    gate (its required record write would then refuse the update), and an adopted obligation must
    survive exactly as it was."""
    from hermes_cli import update_cmd_posix_pause as m
    gateway = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(120)"], stdin=subprocess.DEVNULL)
    home = tmp_path / "home"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    entry = {"pid": gateway.pid, "argv": ["hermes", "gateway", "run"], "home": str(home)}
    monkeypatch.setattr(m, "_discover_systemd", list)
    monkeypatch.setattr(m, "_discover_launchd", list)
    monkeypatch.setattr(m, "_discover_bare", lambda service_pids: ([dict(entry)], []))
    monkeypatch.setattr(m, "_stop_gateways", lambda *a: pytest.fail("a gateway was stopped without a record"))
    token = {**m._empty_token(), **prior}
    before = json.loads(json.dumps(token))
    home.chmod(0o500)
    try:
        assert m._pause(token) is None
    finally:
        home.chmod(0o700)
        gateway.kill()
        gateway.wait(timeout=10)
    assert token == before, f"the failed attempt left state behind: {token}"


@pytest.mark.platforms("posix")
def test_a_replayed_gateway_keeps_its_own_exported_token_and_no_one_elses(tmp_path):
    """A named profile's hand-started gateway, replayed by an update launched under that profile,
    starts as the restart watcher started it: with the launcher's environment (an exported bot
    token may be its only credential). Any other profile, and the multiplexing root, never get it."""
    root = tmp_path / ".hermes"
    for name in ("alpha", "beta"):
        (root / "profiles" / name).mkdir(parents=True)
    probe = _child("""
        import json, sys
        from hermes_cli.update_cmd_posix_pause import _replay_env
        root = sys.argv[1]
        print(json.dumps({who: _replay_env(home).get("TELEGRAM_BOT_TOKEN") for who, home in (
            ("alpha", root + "/profiles/alpha"), ("beta", root + "/profiles/beta"), ("root", root))}))
    """, str(root), env={"HOME": str(tmp_path), "HERMES_HOME": str(root / "profiles" / "alpha"),
                         "TELEGRAM_BOT_TOKEN": "exported-alpha-token"})
    out, _ = probe.communicate(timeout=60)
    got = json.loads(out.strip().splitlines()[-1])
    assert got == {"alpha": "exported-alpha-token", "beta": None, "root": None}, got


@pytest.mark.platforms("posix")
def test_unknown_cgroup_membership_never_authorizes_stopping_the_unit():
    """A unit whose stop could kill this update stays running unless the update is PROVEN outside
    its cgroup; unreadable membership is unknown, never outside."""
    from hermes_cli.update_cmd_posix_pause import _escape_cgroup
    assert _escape_cgroup({"unit": "hermes-gateway-p2probe.service", "pid": 4242, "cgroup": None}) is False


@pytest.mark.platforms("macos")
@pytest.mark.live_system_guard_bypass
def test_a_launchd_job_is_paused_through_launchd_and_restarted_through_it(tmp_path):
    """A KeepAlive job merely killed is respawned by launchd on the old code mid-update; the pause
    boots it out (no respawn) and the restart bootstraps the same plist (a fresh PID)."""
    from hermes_cli.gateway import _launchd_print_service_pid
    from hermes_cli.update_cmd_posix_pause import _alive, _start_job, _stop_job
    label = f"ai.hermes.p2probe-{os.getpid()}"
    plist = tmp_path / f"{label}.plist"
    plist.write_bytes(plistlib.dumps({"Label": label, "ProgramArguments": ["/bin/sleep", "600"],
                                      "RunAtLoad": True, "KeepAlive": True}))
    domain = f"gui/{os.getuid()}"  # windows-footgun: ok — macOS-only test (platforms("macos"))
    if subprocess.run(["launchctl", "print", domain], capture_output=True, check=False).returncode:
        domain = f"user/{os.getuid()}"  # windows-footgun: ok — macOS-only test (platforms("macos"))

    def pid() -> int | None:
        return _launchd_print_service_pid(domain, label)[1]

    subprocess.run(["launchctl", "bootstrap", domain, str(plist)], check=True, timeout=30)
    try:
        deadline = time.monotonic() + 15
        while not pid() and time.monotonic() < deadline:
            time.sleep(0.2)
        first = pid()
        assert first, "premise: launchd never started the probe job"
        job = {"kind": "launchd", "label": label, "domain": domain, "plist": str(plist), "pid": first}
        _stop_job(job)
        time.sleep(3.0)  # KeepAlive would have respawned a merely-killed job by now
        assert not _alive(first, None) and not pid(), "the paused job is running again under launchd"
        _start_job(job)
        assert pid() and pid() != first, "the restart did not bring the job back under launchd"
    finally:
        subprocess.run(["launchctl", "bootout", f"{domain}/{label}"], capture_output=True, check=False)


@pytest.mark.platforms("macos")
def test_a_launchd_entry_targets_the_gateway_behind_its_job(tmp_path, monkeypatch):
    """launchd's job PID is the ``osascript`` front; the gateway serving the job's home is its
    grandchild. The pause marks, drains and waits for that gateway (2026-10-24: it marked and waited
    for the job PID, and the gateway outlived the "pause")."""
    from hermes_cli import gateway, update_cmd_posix_pause, update_fleet_scope
    import gateway.status as gateway_status

    job_pid, gateway_pid = 33058, 33066
    monkeypatch.setattr(gateway, "launchd_gateway_labels_for_install", lambda: ["ai.hermes.gateway"])
    monkeypatch.setattr(gateway, "legacy_launchd_labels_for_install", lambda exclude=(): [])
    monkeypatch.setattr(gateway, "_locate_launchd_gateway_service", lambda label: ("gui/501", job_pid))
    monkeypatch.setattr(gateway, "get_launchd_plist_path", lambda: tmp_path / "ai.hermes.gateway.plist")
    monkeypatch.setattr(update_fleet_scope, "launchd_label_foreign_home", lambda label: None)
    monkeypatch.setattr(update_cmd_posix_pause, "_home_of", lambda pid: str(tmp_path) if pid == job_pid else None)
    monkeypatch.setattr(gateway_status, "live_gateway_pid_for_home",
                        lambda home: gateway_pid if home == tmp_path else None)

    [job] = update_cmd_posix_pause._discover_launchd()
    assert (job["pid"], job["job_pid"], job["home"]) == (gateway_pid, job_pid, str(tmp_path))


def _launchd_job_tree(tmp_path, drain_s):
    """``(job, gateway_pid)``: a job process (the osascript front) whose grandchild, under a wrapper,
    is the gateway. A SIGTERM ends the job and wrapper at once; the gateway drains for *drain_s*
    seconds first (``None``: only the ``stop`` file the test leaves behind ends it, since an
    orphaned gateway is outside the test's process tree)."""
    pid_file, stop = tmp_path / "gateway.pid", tmp_path / "stop"
    on_term = "lambda *_: None" if drain_s is None else f"lambda *_: (time.sleep({drain_s}), sys.exit(0))"
    gateway = ("import os, signal, sys, time\n"
               f"signal.signal(signal.SIGTERM, {on_term})\n"
               f"open({str(pid_file)!r} + '.tmp', 'w').write(str(os.getpid()))\n"
               f"os.replace({str(pid_file)!r} + '.tmp', {str(pid_file)!r})\n"
               f"while not os.path.exists({str(stop)!r}): time.sleep(0.2)\n")
    wrapper = f"import subprocess, sys; subprocess.run([sys.executable, '-c', {gateway!r}])"
    job = subprocess.Popen([sys.executable, "-c", f"import subprocess, sys; subprocess.run([sys.executable, '-c', {wrapper!r}])"],
                           stdin=subprocess.DEVNULL, start_new_session=True)
    return job, wait_until(lambda: pid_file.exists() and int(pid_file.read_text()), timeout=30, what="the gateway")


def _fake_launchctl(tmp_path, monkeypatch, job_pgid):
    """``launchctl bootout`` that SIGTERMs the job's process group, as launchd tears a job down."""
    bin_dir = tmp_path / "bin"
    bin_dir.mkdir()
    fake = bin_dir / "launchctl"
    fake.write_text(f'#!/bin/sh\n[ "$1" = bootout ] && kill -TERM -{int(job_pgid)}\nexit 0\n', encoding="utf-8")
    fake.chmod(0o755)
    monkeypatch.setenv("PATH", f"{bin_dir}{os.pathsep}{os.environ['PATH']}")


@pytest.mark.platforms("posix")
@pytest.mark.parametrize("drain_s", [1.5, None], ids=["drains", "never-exits"])
def test_stopping_a_launchd_job_waits_for_its_gateway_not_the_job(tmp_path, monkeypatch, drain_s):
    """The job PID dies at once on bootout; its gateway may drain for a minute. The stop returns
    only once THAT gateway exited, marked as planned, and refuses the pause when it never does."""
    from gateway.status import get_process_start_time
    from hermes_cli import update_cmd_posix_pause as pause

    job, gateway_pid = _launchd_job_tree(tmp_path, drain_s)
    try:
        _fake_launchctl(tmp_path, monkeypatch, job.pid)
        monkeypatch.setattr(pause, "_STOP_TIMEOUT_S", 6.0)
        entry = {"kind": "launchd", "label": "ai.hermes.gateway", "domain": "gui/501", "home": str(tmp_path),
                 "pid": gateway_pid, "job_pid": job.pid, "ct": get_process_start_time(gateway_pid)}
        if drain_s is None:
            with pytest.raises(pause.PauseRefused, match=str(gateway_pid)):
                pause._stop_job(entry)
        else:
            pause._stop_job(entry)
            assert not pause._alive(gateway_pid, entry["ct"]), "returned while the gateway still ran"
        marker = json.loads((tmp_path / ".gateway-planned-stop.json").read_text(encoding="utf-8"))
        assert marker["target_pid"] == gateway_pid
    finally:
        (tmp_path / "stop").touch()
        with contextlib.suppress(ProcessLookupError, PermissionError):
            os.killpg(job.pid, signal.SIGKILL)  # windows-footgun: ok — posix-only test
        job.wait(timeout=30)
        wait_until(lambda: not pause._alive(gateway_pid, None), timeout=30, what="the gateway to exit")

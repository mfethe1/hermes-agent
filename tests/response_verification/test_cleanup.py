"""Real owned subprocesses; named injected cases replace only signal syscalls.

Injected errno coverage is NOT reproduction of the historical native EPERM.
Receipts in the positive cases come from the unmodified collector/verifier.
"""

import asyncio
from dataclasses import asdict
import errno
import json
import os
import signal
import sys

import pytest

from response_verification.pilot import COMPLETE, DENIED
from tests.response_verification.test_pilot import fixture, SENTINEL

pytestmark = pytest.mark.live_system_guard_bypass


async def until(predicate):
    async with asyncio.timeout(4):
        while not predicate():
            await asyncio.sleep(0.01)


def exists(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False


# The grandchild exits on a harness-owned release file even when cleanup is
# deliberately denied. It never inherits stdout, so EOF cannot stand in for reap.
TREE = """
import json, os, pathlib, subprocess, sys, time
ready, release, go, topology = map(str, sys.argv[1:])
child = subprocess.Popen([sys.executable, '-I', '-c',
    'import pathlib,sys,time; p=pathlib.Path(sys.argv[1]); '
    'exec("while not p.exists(): time.sleep(.01)")', release],
    stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    start_new_session=topology == 'detached')
path = pathlib.Path(ready)
path.with_suffix('.tmp').write_text(json.dumps([os.getpid(), child.pid]))
path.with_suffix('.tmp').replace(path)
while not pathlib.Path(go).exists() and not pathlib.Path(release).exists():
    time.sleep(.01)
if topology in ('exited', 'detached'):
    os._exit(0)
os.write(1, b'x' * 20000)
while not pathlib.Path(release).exists(): time.sleep(.01)
child.wait(timeout=3)
"""


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "denial,deny_child,action,topology",
    [
        (None, False, "deliver", "group"),
        (None, False, "repeat-cancel", "group"),
        (errno.EPERM, False, "deliver", "group"),
        (errno.EACCES, False, "deliver", "group"),
        (errno.EIO, False, "deliver", "group"),
        (errno.EPERM, True, "deliver", "group"),
        (errno.EPERM, True, "cancel", "group"),
        (errno.EPERM, True, "repeat-cancel", "group"),
        (errno.EPERM, True, "disconnect", "group"),
        (None, False, "deliver", "exited"),
        (None, False, "deliver", "detached"),
    ],
    ids=[
        "native-group",
        "native-repeat-cancel",
        "injected-EPERM-group",
        "injected-EACCES-group",
        "injected-EIO-group",
        "injected-EPERM-both",
        "injected-EPERM-cancel",
        "injected-EPERM-repeat-cancel",
        "injected-EPERM-disconnect",
        "native-exited-leader",
        "native-detached",
    ],
)
async def test_owned_cleanup_is_bounded_and_truthful(
    tmp_path, monkeypatch, caplog, denial, deny_child, action, topology
):
    ready, release, go = (tmp_path / name for name in ("ready", "release", "go"))
    adapter, event, wire, _ = fixture(
        tmp_path,
        timeout=5,
        cleanup_timeout=0.2,
        command=[
            sys.executable,
            "-I",
            "-c",
            TREE,
            str(ready),
            str(release),
            str(go),
            topology,
        ],
    )
    attempt = adapter.attempt
    task = asyncio.create_task(
        adapter.deliver(event, json.dumps(wire).encode(), SENTINEL)
    )
    original_group, original_kill = os.killpg, os.kill
    signalled = asyncio.Event()
    signals = []
    ids = []

    def group_signal(pgid, sig):
        if sig != 0:
            assert pgid == ids[0] and pgid != os.getpgrp()
            signals.append(("group", pgid, int(sig)))
            signalled.set()
            if denial is not None:
                raise OSError(denial, "INJECTED signal denial SECRET_MUST_NOT_LOG")
        return original_group(pgid, sig)

    def child_signal(pid, sig):
        if sig != 0:
            assert pid == ids[0]
            signals.append(("child", pid, int(sig)))
            if deny_child:
                raise PermissionError(
                    errno.EPERM, "INJECTED child denial SECRET_MUST_NOT_LOG"
                )
        return original_kill(pid, sig)

    try:
        await until(ready.exists)
        ids = json.loads(ready.read_text())
        monkeypatch.setattr(os, "killpg", group_signal)
        monkeypatch.setattr(os, "kill", child_signal)
        started = asyncio.get_running_loop().time()
        if action in ("cancel", "repeat-cancel"):
            task.cancel()
            if action == "repeat-cancel":
                await asyncio.wait_for(signalled.wait(), 2)
                # Actual caller cancellation while the real tree is cleaning up.
                for _ in range(12):
                    task.cancel()
                    await asyncio.sleep(0.005)
        elif action == "disconnect":
            disconnect = asyncio.create_task(adapter.disconnect())
            await asyncio.wait_for(signalled.wait(), 2)
            for _ in range(12):
                disconnect.cancel()
                await asyncio.sleep(0.005)
            done, _ = await asyncio.wait({disconnect}, timeout=3)
            assert done
            with pytest.raises(asyncio.CancelledError):
                await disconnect
        else:
            go.touch()
        done, _ = await asyncio.wait({task}, timeout=3)
        assert done, "cleanup must finish even if both syscalls deny signaling"
        assert asyncio.get_running_loop().time() - started < 3
        if action == "deliver":
            await task
            assert adapter.sink == [DENIED.encode()]
        else:
            with pytest.raises(asyncio.CancelledError):
                await task
            assert attempt.cancelled and adapter.sink == []
        status = attempt.cleanup
        (tmp_path / "cleanup-observation.json").write_text(
            json.dumps(
                {
                    "coverage": "injected errno" if denial else "native subprocess",
                    "denial": denial,
                    "deny_child": deny_child,
                    "action": action,
                    "topology": topology,
                    "owned_pids": ids,
                    "signals": signals,
                    "cleanup": asdict(status),
                    "cancelled": attempt.cancelled,
                    "sink": [item.decode() for item in adapter.sink],
                },
                indent=2,
            )
        )
        assert attempt._active_task is None and attempt._cleanup_task.done()
        assert attempt.terminal and not attempt.drafts
        assert attempt.disposition() == DENIED
        assert status.detached_descendants == "untracked"
        assert len(signals) <= 2  # one group signal and at most one child fallback
        assert "SECRET_MUST_NOT_LOG" not in caplog.text
        if denial:
            errors = [x for x in status.observations if x.get("error")]
            assert any(
                x["operation"] == "killpg" and x["errno"] == denial for x in errors
            )
            assert not status.verified and attempt.failed
            assert status.group == "present" and status.deadline_expired
            assert exists(ids[1]), "reaped direct child is not descendant containment"
            if deny_child:
                assert status.child == "unreaped" and status.returncode is None
                assert attempt.process_returncode is None and exists(ids[0])
                assert any(
                    x["operation"] == "kill-child" and x["errno"] == errno.EPERM
                    for x in errors
                )
            else:
                assert status.child == "reaped" and status.returncode == -signal.SIGKILL
                assert attempt.process_returncode == -signal.SIGKILL
            assert '"pid":' in caplog.text and '"returncode":' in caplog.text
        elif topology == "exited":
            assert status.child == "reaped" and status.group == "present"
            assert not status.verified and not signals and exists(ids[1])
        elif topology == "detached":
            assert status.child == "reaped" and status.group == "absent"
            assert exists(
                ids[1]
            )  # explicit boundary demonstration, never "all descendants reaped"
        else:
            assert status.child == "reaped" and status.group == "absent"
            if any(x.get("error") for x in status.observations):
                # A native observation denial is still retained and fail-closed,
                # even if later probes establish that the group disappeared.
                assert attempt.failed and not status.verified
            else:
                assert status.verified
            await until(lambda: not any(exists(pid) for pid in ids))
        await adapter.send_draft(event.source.chat_id, 2, SENTINEL)
        await adapter.deliver(event, json.dumps(wire).encode())
        assert not attempt.drafts and COMPLETE.encode() not in adapter.sink
    finally:
        monkeypatch.setattr(os, "killpg", original_group)
        monkeypatch.setattr(os, "kill", original_kill)
        release.touch()
        if not task.done():
            task.cancel()
        await asyncio.wait({task}, timeout=3)
        if task.done() and not task.cancelled():
            task.exception()
        if ids:
            await until(lambda: not any(exists(pid) for pid in ids))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "denial",
    [None, errno.EPERM, errno.EACCES, errno.EIO],
    ids=[
        "native-positive",
        "injected-EPERM-probe",
        "injected-EACCES-probe",
        "injected-EIO-probe",
    ],
)
async def test_real_positive_receipt_cannot_override_unknown_cleanup(
    tmp_path, monkeypatch, denial
):
    adapter, event, wire, _ = fixture(tmp_path)
    original = os.killpg

    def signal_boundary(pgid, sig):
        if denial is not None:
            raise OSError(denial, "INJECTED group observation denial")
        return original(pgid, sig)

    monkeypatch.setattr(os, "killpg", signal_boundary)
    await asyncio.wait_for(adapter.deliver(event, json.dumps(wire).encode()), 4)
    attempt = adapter.attempt
    (tmp_path / "cleanup-observation.json").write_text(
        json.dumps(
            {
                "coverage": "injected errno" if denial else "native subprocess",
                "denial": denial,
                "cleanup": asdict(attempt.cleanup),
                "real_receipt": attempt.receipt is not None,
                "sink": [item.decode() for item in adapter.sink],
            },
            indent=2,
        )
    )
    assert attempt.receipt is not None  # unmodified real worker output
    assert attempt.receipt.artifact_digest == wire["artifact_digest"]
    assert attempt.process_returncode == 0 and attempt.cleanup.child == "reaped"
    if denial is not None:
        assert adapter.sink == [DENIED.encode()]
        assert not attempt.cleanup.verified and attempt.cleanup.group == "unknown"
        assert any(x.get("errno") == denial for x in attempt.cleanup.observations)
    else:
        assert adapter.sink == [COMPLETE.encode()] and attempt.cleanup.verified

"""A search engine that dies must surface an error, never "0 matches".

Exit codes 0/1 are the only success signals (match / no match); 2 is rg/grep's
error code, tolerated only when a usable payload survived (one unreadable file).
Anything else — SIGKILL/OOM, 127 missing binary, an unexpected status — used to
parse as an empty, successful search: the caller concluded the target was absent.
Each case runs a REAL process (a stand-in ``rg`` script) on both the native and
shell lanes.
"""
from __future__ import annotations

import os
import subprocess

import pytest

from tools.environments.local import LocalEnvironment, _kill_process_group_posix, _record_process_group
from tools.file_operations import ShellFileOperations


@pytest.fixture(params=["1", "0"], ids=["native", "shell"])
def ops_for(request, tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_NATIVE_FILE_READ", request.param)
    root = tmp_path / "root"
    root.mkdir()
    (root / "a.txt").write_text("needle\n")

    def make(body: str):
        rg = tmp_path / "rg"
        rg.write_text("#!/bin/sh\n" + body.replace("{root}", str(root)) + "\n")
        rg.chmod(0o755)
        ops = ShellFileOperations(LocalEnvironment(cwd=str(root)), cwd=str(root))
        return ops, str(root), str(rg)

    return make


def _search(ops, root, rg, mode="content"):
    return ops._search_with_rg("needle", root, None, 50, 0, mode, 0, rg_executable=rg)


@pytest.mark.parametrize("body,code", [
    ("kill -9 $$", None),            # OOM-killer / SIGKILL, nothing printed
    ("exit 127", "127"),             # engine missing at exec time
    ("echo boom >&2; exit 3", "3"),  # unexpected status
])
@pytest.mark.parametrize("mode", ["content", "files_only", "count"])
def test_dead_engine_is_an_error_not_zero_matches(ops_for, body, code, mode):
    r = _search(*ops_for(body), mode=mode)
    assert r.error and r.error.startswith("Search failed"), r
    if code:
        assert f"exit {code}" in r.error
    # merged stderr can pass the path-shape check in files_only; such rows must be disowned
    assert r.total_count == 0 or "unreliable" in r.error


def test_no_match_exit_1_is_still_a_clean_empty_result(ops_for):
    r = _search(*ops_for("exit 1"))
    assert r.error is None and r.total_count == 0


def test_partial_exit_2_keeps_payload(ops_for):
    r = _search(*ops_for('echo "{root}/a.txt:1:needle"; echo "rg: x: Permission denied" >&2; exit 2'))
    assert r.error is None and r.total_count == 1


def test_engine_dying_after_output_is_an_error_that_keeps_the_partial_rows(ops_for):
    r = _search(*ops_for('echo "{root}/a.txt:1:needle"; kill -9 $$'))
    assert r.error and "unreliable" in r.error
    assert r.total_count == 1 and r.truncated


@pytest.mark.skipif(not hasattr(os, "waitid"), reason="needs waitid(WNOWAIT) to hold a zombie")
def test_killing_an_exited_unreaped_engine_never_raises():
    """rg hitting the limit and exiting between poll() and the kill leaves an unreaped
    leader; macOS answers getpgid() on it with ESRCH. The spawn-time pgid record is
    what lets the kill proceed instead of crashing the whole search."""
    proc = subprocess.Popen(["sh", "-c", "exit 0"], start_new_session=True)
    _record_process_group(proc)
    os.waitid(os.P_PID, proc.pid, os.WEXITED | os.WNOWAIT)  # exited, not reaped
    _kill_process_group_posix(proc)
    assert proc.wait() == 0


def test_native_lane_records_the_group_before_it_kills(ops_for, monkeypatch):
    """The native rg lane must record the pgid at spawn, or the kill above has nothing to fall back on."""
    if os.environ["HERMES_NATIVE_FILE_READ"] != "1":
        pytest.skip("only the native lane spawns rg itself")
    import tools.environments.local as local
    seen, real_kill = [], local._kill_process_group_posix
    monkeypatch.setattr(local, "_kill_process_group_posix",
                        lambda proc: (seen.append(getattr(proc, "_hermes_pgid", None)), real_kill(proc)))
    r = _search(*ops_for('yes "{root}/a.txt:1:needle"'))  # never exits: the fetch bound forces the kill
    assert r.total_count and seen and all(seen), (r, seen)

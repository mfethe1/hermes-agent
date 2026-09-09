"""Expected-safe counterparts of the independent review's restart counterexamples.

Every worker runs the real main in an owned disposable process. Only named fault
primitives are injected; EOF teardown has no signal or kill fallback.
"""

import json
import os
from pathlib import Path
import selectors
import subprocess
import sys

import pytest

from gateway.cooperative_stop import provision

ROOT = Path(__file__).resolve().parents[2]
pytestmark = pytest.mark.macos_only

FAULT_CHILD = """
import errno, os, sys
import gateway.cooperative_stop as consumer
import gateway.activation_controller as persistence
mode = sys.argv[2]
if mode == 'crash':
    dispatch = consumer.CooperativeWorker.dispatch
    def crash(self, request):
        if request.get('op') == '__crash__':
            os._exit(71)
        return dispatch(self, request)
    consumer.CooperativeWorker.dispatch = crash
else:
    primitive, nth = mode.split(':')
    obj = persistence.json if primitive == 'dump' else persistence.os
    original = getattr(obj, primitive)
    count = 0
    def fail(*args, **kwargs):
        global count
        count += 1
        if count == int(nth):
            print('INJECTED ' + mode, file=sys.stderr, flush=True)
            raise OSError(errno.EIO, 'reservation test write fault')
        return original(*args, **kwargs)
    setattr(obj, primitive, fail)
raise SystemExit(consumer.main([sys.argv[1]]))
"""


class Child:
    def __init__(self, store, mode=None):
        command = [sys.executable, "-B", "-m", "gateway.cooperative_stop", str(store)]
        if mode:
            command = [sys.executable, "-B", "-c", FAULT_CHILD, str(store), mode]
        self.process = subprocess.Popen(
            command,
            cwd=ROOT,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            env={
                "PATH": os.defpath,
                "HERMES_HOME": str(store.parent),
                "PYTHONDONTWRITEBYTECODE": "1",
            },
        )
        print("SPAWN", self.process.pid, str(store), mode, flush=True)
        self.ready = self.read()

    def read(self):
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ)
            assert selector.select(5), "reply deadline; no kill fallback"
        line = self.process.stdout.readline()
        print("RECV", self.process.pid, line.rstrip(), flush=True)
        return json.loads(line) if line else None

    def send(self, op, *, read=True, **extra):
        request = {
            "op": op,
            "owner": self.ready["owner"],
            "incarnation": self.ready["incarnation"],
            **extra,
        }
        print("SEND", self.process.pid, request, flush=True)
        self.process.stdin.write(json.dumps(request) + "\n")
        self.process.stdin.flush()
        return self.read() if read else None

    def join(self, expected, *, eof=False):
        if eof and not self.process.stdin.closed:
            self.process.stdin.close()
        code = self.process.wait(timeout=5)
        stderr = self.process.stderr.read()
        print("EXIT", self.process.pid, code, "STDERR", repr(stderr), flush=True)
        assert code == expected
        return stderr


@pytest.fixture
def spawn():
    children = []

    def start(store, mode=None):
        child = Child(store, mode)
        children.append(child)
        return child

    yield start
    for child in children:
        if child.process.poll() is None:
            child.join(2, eof=True)
        for stream in (child.process.stdin, child.process.stdout, child.process.stderr):
            stream.close()


def saved(store):
    return json.loads((store / "barrier.json").read_text())


def assert_unresolved(spawn, store, ticket, origin):
    replacement = spawn(store)
    assert replacement.ready["closed"]
    assert replacement.ready["active_work"] == 1
    assert replacement.ready["unresolved_prior_work"] == 1
    record = replacement.ready["reservations"][ticket]
    assert record == {
        "owner": origin["owner"],
        "incarnation": origin["incarnation"],
        "completed": False,
    }
    assert not replacement.send("open")["accepted"]
    assert not replacement.send("begin")["accepted"]
    assert not replacement.send("finish", ticket=ticket)["accepted"]
    assert not replacement.send(
        "finish", ticket=ticket, incarnation=origin["incarnation"]
    )["accepted"]
    assert not replacement.send("stop", owner="foreign")["accepted"]
    stop = replacement.send("stop")
    assert stop["accepted"] and stop["active_work"] == 1
    assert stop["install_enabled"] is False
    assert replacement.process.poll() is None
    replacement.join(2, eof=True)
    return replacement


@pytest.mark.parametrize("end", ["eof", "malformed", "crash"])
@pytest.mark.parametrize("latch", [False, True])
def test_outstanding_survives_review_counterexamples(tmp_path, spawn, end, latch):
    store = tmp_path / "store"
    provision(store, "owner")
    child = spawn(store, "crash" if end == "crash" else None)
    assert child.send("open")["accepted"]
    ticket = child.send("begin")["ticket"]
    assert saved(store)["reservations"][ticket]["completed"] is False
    if latch:
        assert child.send("stop")["active_work"] == 1
    if end == "malformed":
        child.process.stdin.write("{bad-json\n")
        child.process.stdin.flush()
        assert child.read() is None
    elif end == "crash":
        assert child.send("__crash__") is None
    child.join(71 if end == "crash" else 2, eof=end == "eof")
    assert_unresolved(spawn, store, ticket, child.ready)
    # A second replacement must retain the original acquisition provenance too.
    assert_unresolved(spawn, store, ticket, child.ready)


@pytest.mark.parametrize("op", ["begin", "finish", "stop"])
@pytest.mark.parametrize("broken_pipe", [False, True])
def test_lost_reply_keeps_durable_disposition(tmp_path, spawn, op, broken_pipe):
    store = tmp_path / "store"
    provision(store, "owner")
    child = spawn(store)
    assert child.send("open")["accepted"]
    ticket = None if op == "begin" else child.send("begin")["ticket"]
    if broken_pipe:
        child.process.stdout.close()
    child.send(op, read=False, ticket=ticket)
    # CPython reports 120 when shutdown flushes the already broken stdout.
    child.join(120 if broken_pipe else 2, eof=True)
    records = saved(store)["reservations"]
    assert len(records) == 1
    ticket = next(iter(records))
    assert records[ticket]["incarnation"] == child.ready["incarnation"]
    if op == "finish":
        assert records[ticket]["completed"] is True
        replacement = spawn(store)
        assert replacement.ready["active_work"] == 0
        assert not replacement.send("finish", ticket=ticket)["accepted"]
        assert replacement.send("stop")["active_work"] == 0
        replacement.join(0)
    else:
        assert_unresolved(spawn, store, ticket, child.ready)


@pytest.mark.parametrize("phase", ["startup", "open", "begin", "finish", "stop"])
@pytest.mark.parametrize(
    "primitive,offset", [("dump", 0), ("replace", 0), ("fsync", 0), ("fsync", 1)]
)
def test_write_faults_never_lose_acknowledged_obligations(
    tmp_path,
    spawn,
    phase,
    primitive,
    offset,
):
    store = tmp_path / "store"
    provision(store, "owner")
    save_number = {"startup": 1, "open": 2, "begin": 3, "finish": 4, "stop": 4}[phase]
    nth = 2 * save_number - 1 + offset if primitive == "fsync" else save_number
    mode = f"{primitive}:{nth}"
    child = spawn(store, mode)
    ticket = None
    if phase == "startup":
        assert child.ready is None
    else:
        if phase != "open":
            assert child.send("open")["accepted"]
        if phase in ("finish", "stop"):
            ticket = child.send("begin")["ticket"]
        assert child.send(phase, ticket=ticket) is None
    assert "INJECTED " + mode in child.join(2)
    records = saved(store)["reservations"]
    if ticket:
        assert ticket in records  # completion retains provenance, never deletes
    if records:
        ticket, record = next(iter(records.items()))
        if not record["completed"]:
            assert_unresolved(spawn, store, ticket, child.ready)
            return
        # Replace succeeded before a directory-fsync error: a real owner-bound
        # finish was recorded. Restart fsyncs it before reporting zero work.
        assert phase == "finish" and primitive == "fsync" and offset == 1
        assert record["incarnation"] == child.ready["incarnation"]
    else:
        assert phase in ("startup", "open", "begin")  # never acknowledged a ticket
    replacement = spawn(store)
    assert replacement.ready["closed"] and replacement.ready["active_work"] == 0
    assert replacement.send("stop")["accepted"]
    replacement.join(0)


@pytest.mark.parametrize("op", ["begin", "finish", "stop"])
def test_actual_write_denial_preserves_obligation(tmp_path, spawn, op):
    store = tmp_path / "store"
    provision(store, "owner")
    child = spawn(store)
    assert child.send("open")["accepted"]
    ticket = child.send("begin")["ticket"]
    store.chmod(0o500)
    try:
        assert child.send(op, ticket=ticket) is None
        assert "PermissionError" in child.join(2)
    finally:
        store.chmod(0o700)
    assert_unresolved(spawn, store, ticket, child.ready)


def test_real_path_loss_retains_accounting(tmp_path, spawn):
    store = tmp_path / "store"
    provision(store, "owner")
    child = spawn(store)
    assert child.send("open")["accepted"]
    ticket = child.send("begin")["ticket"]
    moved = tmp_path / "moved"
    store.rename(moved)
    assert child.send("stop") is None
    child.join(2)
    assert_unresolved(spawn, moved, ticket, child.ready)


@pytest.mark.parametrize("mode", ["dump:1", "replace:1", "fsync:1", "fsync:2"])
def test_replacement_write_fault_preserves_original_obligation(tmp_path, spawn, mode):
    store = tmp_path / "store"
    provision(store, "owner")
    child = spawn(store)
    assert child.send("open")["accepted"]
    ticket = child.send("begin")["ticket"]
    child.join(2, eof=True)
    failed = spawn(store, mode)
    assert failed.ready is None
    assert "INJECTED " + mode in failed.join(2)
    assert_unresolved(spawn, store, ticket, child.ready)


def test_completed_and_unresolved_work_remain_distinct(tmp_path, spawn):
    store = tmp_path / "store"
    provision(store, "owner")
    child = spawn(store)
    assert child.send("open")["accepted"]
    done = child.send("begin")["ticket"]
    outstanding = child.send("begin")["ticket"]
    assert child.send("finish", ticket=done)["active_work"] == 1
    child.join(2, eof=True)
    replacement = assert_unresolved(spawn, store, outstanding, child.ready)
    assert replacement.ready["reservations"][done] == {
        "owner": child.ready["owner"],
        "incarnation": child.ready["incarnation"],
        "completed": True,
    }


@pytest.mark.parametrize("latch", [False, True])
def test_legacy_store_is_unknown_even_if_closed(tmp_path, spawn, latch):
    store = tmp_path / "store"
    provision(store, "owner")
    legacy = {
        "version": 1,
        "owner": "owner",
        "incarnation": "previous",
        "closed": True,
        "stop_latched": latch,
    }
    (store / "barrier.json").write_text(json.dumps(legacy))
    child = spawn(store)
    assert child.ready is None
    assert "Blocked" in child.join(2)
    assert saved(store) == legacy  # no destructive migration or invented empty ledger


@pytest.mark.parametrize(
    "bad",
    [
        None,
        [],
        {},
        {"ticket": {}},
        {"ticket": {"owner": "foreign", "incarnation": "old", "completed": False}},
    ],
)
def test_missing_or_invalid_ledger_refuses_startup(tmp_path, spawn, bad):
    store = tmp_path / "store"
    provision(store, "owner")
    body = saved(store)
    if bad == {}:
        body.pop("reservations")
    else:
        body["reservations"] = bad
    (store / "barrier.json").write_text(json.dumps(body))
    child = spawn(store)
    assert child.ready is None
    child.join(2)
    assert saved(store) == body

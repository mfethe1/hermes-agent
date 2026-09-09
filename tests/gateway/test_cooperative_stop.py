"""Real pipe consumer and disposable processes/stores; no manager simulation."""

import json
import os
import plistlib
from pathlib import Path
import selectors
import subprocess
import sys

import pytest

from gateway import drain_control
from gateway.activation_controller import native_preflight
from gateway.cooperative_stop import provision

ROOT = Path(__file__).resolve().parents[2]
CHILDREN = []
pytestmark = pytest.mark.macos_only


@pytest.fixture(autouse=True)
def join_owned_children():
    yield
    for child in CHILDREN:
        if child.process.poll() is None:
            child.process.stdin.close()
            child.process.wait(timeout=5)
        for stream in (child.process.stdin, child.process.stdout, child.process.stderr):
            stream.close()
    CHILDREN.clear()


class Child:
    def __init__(self, store):
        self.process = subprocess.Popen(
            [sys.executable, "-B", "-m", "gateway.cooperative_stop", str(store)],
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
        CHILDREN.append(self)
        self.ready = self.read()

    def read(self):
        with selectors.DefaultSelector() as selector:
            selector.register(self.process.stdout, selectors.EVENT_READ)
            assert selector.select(5), "child reply deadline"
        line = self.process.stdout.readline()
        return json.loads(line) if line else None

    def send(self, op, **extra):
        self.process.stdin.write(
            json.dumps({
                "op": op,
                "owner": self.ready["owner"],
                "incarnation": self.ready["incarnation"],
                **extra,
            })
            + "\n"
        )
        self.process.stdin.flush()
        return self.read()

    def join(self, expected):
        assert self.process.wait(timeout=5) == expected
        self.process.stdin.close()
        self.process.stdout.close()
        self.process.stderr.close()


def test_bound_stop_survives_replacement_and_expired_marker(tmp_path):
    store = tmp_path / "store"
    provision(store, "owner-a")
    child = Child(store)
    assert child.ready["closed"]
    assert not child.send("begin")["accepted"]
    assert not child.send("open", owner="foreign")["accepted"]
    assert child.send("open")["accepted"]
    ticket = child.send("begin")["ticket"]
    second_ticket = child.send("begin")["ticket"]
    assert not child.send("stop", incarnation="stale")["accepted"]
    assert not child.send("stop", owner="foreign")["accepted"]
    assert child.send("status")["closed"] is False
    result = child.send("stop")
    assert result["closed"] and result["stop_latched"] and result["active_work"] == 2
    assert child.process.poll() is None  # no process-stop claim while work remains
    assert not child.send("begin")["accepted"]
    assert not child.send("finish", ticket="unknown")["accepted"]
    assert child.send("finish", ticket=ticket)["active_work"] == 1
    duplicate = child.send("finish", ticket=ticket)
    assert not duplicate["accepted"] and duplicate["active_work"] == 1
    assert child.send("finish", ticket=second_ticket)["active_work"] == 0
    child.join(0)
    marker = drain_control.drain_request_path(home=store)
    marker.write_text(
        json.dumps({
            "requested_at": "2000-01-01T00:00:00+00:00",
            "request_id": "owner-a",
        })
    )
    assert not drain_control.drain_requested(home=store)
    marker.unlink()  # legacy expiry/removal cannot clear the distinct durable latch
    replacement = Child(store)
    assert replacement.ready["incarnation"] != child.ready["incarnation"]
    assert replacement.ready["closed"] and replacement.ready["stop_latched"]
    assert not replacement.send("open")["accepted"]
    assert not replacement.send("stop", incarnation=child.ready["incarnation"])[
        "accepted"
    ]
    assert replacement.process.poll() is None
    assert replacement.send("stop")["accepted"]
    replacement.join(0)
    plist = tmp_path / "disposable.plist"
    plist.write_bytes(
        plistlib.dumps({"Label": "test-only", "KeepAlive": True, "ExitTimeOut": 0})
    )
    for exit_code in (None, 0, 1):
        result = native_preflight(plist, reported_disable_exit_code=exit_code)
        assert result["respawn_fenced"] is False
        assert result["install_enabled"] is False and result["ready"] is False


def test_crash_controller_loss_and_invalid_store_fail_closed(tmp_path):
    store = tmp_path / "store"
    provision(store, "owner-b")
    first = Child(store)
    assert first.send("open")["accepted"]
    assert first.send("begin")["accepted"]
    contender = Child(store)
    assert contender.ready is None
    contender.join(2)
    # Abrupt death of this owned disposable process only. No live gateway signal.
    first.process.kill()
    first.join(-9)
    second = Child(store)
    assert second.ready["closed"] and not second.ready["stop_latched"]
    assert not second.send("begin")["accepted"]
    assert not second.send("open", incarnation=first.ready["incarnation"])["accepted"]
    assert second.ready["active_work"] == 1
    assert second.ready["unresolved_prior_work"] == 1
    assert not second.send("open")["accepted"]
    assert second.send("stop")["active_work"] == 1
    second.process.stdin.close()  # EOF is not completion
    second.join(2)
    third = Child(store)
    assert third.ready["closed"] and third.ready["stop_latched"]
    assert third.send("stop")["active_work"] == 1
    third.process.stdin.close()
    third.join(2)
    (store / "barrier.json").write_text('{"closed": false}')
    invalid = Child(store)
    assert invalid.ready is None
    invalid.join(2)
    (store / "barrier.json").unlink()
    missing = Child(store)
    assert missing.ready is None
    missing.join(2)

    lost_store = tmp_path / "lost-reply"
    provision(lost_store, "owner-c")
    lost = Child(lost_store)
    assert lost.send("open")["accepted"]
    assert lost.send("begin")["accepted"]
    # Deliver the actual stop request, then lose the controller without reading
    # its reply. EOF exits 2 with unfinished work; the replacement must stay shut.
    lost.process.stdin.write(
        json.dumps({
            "op": "stop",
            "owner": "owner-c",
            "incarnation": lost.ready["incarnation"],
        })
        + "\n"
    )
    lost.process.stdin.close()
    lost.join(2)
    recovered = Child(lost_store)
    assert recovered.ready["stop_latched"] and recovered.ready["closed"]
    assert not recovered.send("open")["accepted"]
    assert recovered.send("stop")["active_work"] == 1
    recovered.process.stdin.close()
    recovered.join(2)

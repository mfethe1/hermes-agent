"""Real artifact/process/entrypoint tests; no evaluator mocks."""

import asyncio
from dataclasses import asdict, replace
from datetime import datetime, timedelta, timezone
import hashlib
import json
import sys

import pytest

from gateway.config import Platform
from gateway.platforms.event import MessageEvent
from gateway.response_verification_pilot import IsolatedPilotAdapter
from gateway.session import SessionSource
from response_verification.core import (
    ApprovedContext,
    CriterionKind,
    CriterionReference,
    approved_request_digest,
    criterion_digest,
)
from response_verification.pilot import COMPLETE, DENIED, PilotAttempt

# Real process-group signals are required; all targets are newly spawned pilot children.
pytestmark = pytest.mark.live_system_guard_bypass

SENTINEL = "DRAFT_SENTINEL_899cab_DO_NOT_SEND"


def fixture(tmp_path, attempt_id="attempt-1", **kwargs):
    path = tmp_path / attempt_id
    path.write_bytes(b"actual isolated artifact bytes\x00\xff")
    digest = "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()
    criterion = CriterionReference(
        "bytes-match",
        criterion_digest(
            "bytes-match", CriterionKind.STATE, "artifact bytes match approved sha256"
        ),
        CriterionKind.STATE,
    )
    context = ApprovedContext(
        "pilot-task",
        attempt_id,
        digest,
        "isolated-local",
        "test-approval",
        approved_request_digest((criterion,)),
        (criterion,),
        frozenset({"pilot-collector"}),
        "model",
        datetime.now(timezone.utc),
        timedelta(seconds=30),
    )
    wire = {
        k: getattr(context, k)
        for k in (
            "task_id",
            "attempt_id",
            "artifact_digest",
            "environment",
            "approved_request_digest",
        )
    }
    wire.update(schema_version=1, criteria=[asdict(criterion)])
    attempt = PilotAttempt(context, path, **kwargs)
    sink = []
    adapter = IsolatedPilotAdapter(attempt, sink)
    event = MessageEvent(
        text="verify", source=SessionSource(Platform.LOCAL, "isolated-pilot")
    )
    return adapter, event, wire, path


async def deliver(adapter, event, wire):
    await adapter.send_draft(event.source.chat_id, 1, SENTINEL)
    assert adapter.sink == []
    raw = wire if isinstance(wire, bytes) else json.dumps(wire).encode()
    await adapter.deliver(event, raw, SENTINEL)
    assert len(adapter.sink) == 1
    assert SENTINEL.encode() not in b"".join(adapter.sink)
    assert not adapter.attempt.drafts


@pytest.mark.asyncio
async def test_real_positive_duplicate_and_distinct_attempts(tmp_path):
    adapter, event, wire, _ = fixture(tmp_path)
    await asyncio.gather(
        deliver(adapter, event, wire),
        adapter.deliver(event, json.dumps(wire).encode(), SENTINEL),
    )
    assert adapter.sink == [COMPLETE.encode()]
    assert adapter.attempt.receipt.artifact_digest == wire["artifact_digest"]
    other, event2, wire2, _ = fixture(tmp_path, "attempt-2")
    await deliver(
        other, event2, wire
    )  # First attempt's raw references cannot authorize second.
    assert other.sink == [DENIED.encode()]
    assert other.attempt.context.attempt_id == wire2["attempt_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    [
        "artifact",
        "missing-artifact",
        "task_id",
        "attempt_id",
        "artifact_digest",
        "environment",
        "approved_request_digest",
        "criterion",
        "omitted",
        "duplicate",
        "unknown",
        "missing",
        "version",
        "bool-version",
        "type",
        "size",
        "depth",
        "decision",
        "context",
        "clock",
        "issuer",
        "receipt",
        "enum",
        "nan",
        "truncated",
        "collection",
    ],
)
async def test_negative_transport_and_artifact(tmp_path, case):
    adapter, event, wire, path = fixture(tmp_path)
    if case == "artifact":
        path.write_bytes(b"changed")
    elif case == "missing-artifact":
        path.unlink()
    elif case in ("task_id", "attempt_id", "environment"):
        wire[case] = "wrong"
    elif case in ("artifact_digest", "approved_request_digest"):
        wire[case] = "sha256:" + "0" * 64
    elif case == "criterion":
        wire["criteria"][0]["criterion_digest"] = "sha256:" + "0" * 64
    elif case == "omitted":
        wire["criteria"] = []
    elif case == "duplicate":
        wire = json.dumps(wire).encode().replace(b"{", b'{"schema_version":1,', 1)
    elif case in ("unknown", "decision", "context", "clock", "issuer", "receipt"):
        wire[case] = "COMPLETE"
    elif case == "missing":
        del wire["task_id"]
    elif case == "version":
        wire["schema_version"] = 2
    elif case == "bool-version":
        wire["schema_version"] = True
    elif case == "type":
        wire["task_id"] = 1
    elif case == "size":
        wire = b" " * 16385
    elif case == "depth":
        wire = b"[" * 100 + b"0" + b"]" * 100
    elif case == "enum":
        wire["criteria"][0]["kind"] = "external_effect"
    elif case == "nan":
        wire = b'{"schema_version":NaN}'
    elif case == "truncated":
        wire = b"{"
    elif case == "collection":
        wire["criteria"] *= 25
    await deliver(adapter, event, wire)
    assert adapter.sink == [DENIED.encode()]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "program",
    [
        'raise RuntimeError("crash")',
        "import sys; sys.exit(4)",
        'print("{")',
        'print("{\\"schema_version\\":1,\\"decision\\":\\"COMPLETE\\"}")',
        'print("x" * 20000)',
        'import time; time.sleep(5); print("late COMPLETE")',
    ],
)
async def test_real_verifier_failures(tmp_path, program):
    adapter, event, wire, _ = fixture(
        tmp_path, command=[sys.executable, "-I", "-c", program], timeout=0.3
    )
    await deliver(adapter, event, wire)
    assert adapter.sink == [DENIED.encode()]
    await adapter.deliver(event, json.dumps(wire).encode())
    assert len(adapter.sink) == 1


@pytest.mark.asyncio
async def test_dispatch_clock_cancel_and_scope(tmp_path):
    adapter, event, wire, _ = fixture(tmp_path)
    await adapter.attempt.prepare(json.dumps(wire).encode())
    assert adapter.attempt.disposition() == COMPLETE
    adapter.attempt.context = replace(
        adapter.attempt.context, receipt_max_age=timedelta(microseconds=1)
    )
    adapter._sending = True
    await adapter.send("isolated-pilot", "COMPLETE")
    assert adapter.sink == [DENIED.encode()]
    other, event2, wire2, _ = fixture(tmp_path, "attempt-2")
    other.attempt.cancel()
    await deliver(other, event2, wire2)
    assert other.sink == [DENIED.encode()]
    outside = MessageEvent(
        text="normal", source=SessionSource(Platform.LOCAL, "outside")
    )
    with pytest.raises(ValueError):
        await other.deliver(outside, json.dumps(wire2).encode())
    with pytest.raises(ValueError):
        await other.send("isolated-pilot", SENTINEL)
    assert len(other.sink) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    ["stale", "future", "unknown", "failed", "hash", "binding", "malformed-outcome"],
)
async def test_real_collected_receipt_negatives_at_sink(tmp_path, case):
    # Trusted harness mutates actual collector evidence, never the evaluator.
    from pathlib import Path

    root = str(Path(__file__).resolve().parents[2])
    program = """
import sys, json, subprocess, time
from dataclasses import replace
from datetime import timedelta
sys.path.insert(0, sys.argv[1])
from response_verification.pilot_wire import decode_receipt, receipt_wire
from response_verification.core import EvidenceOutcome, with_receipt_hash
raw = sys.stdin.buffer.read()
out = subprocess.check_output([sys.executable, '-I', sys.argv[1] + '/response_verification/pilot_worker.py', 'collect'], input=raw)
r = decode_receipt(out)
case = sys.argv[2]
if case == 'stale':
    time.sleep(0.2)
elif case == 'future':
    r = with_receipt_hash(replace(r, observed_at=r.observed_at + timedelta(minutes=5)))
elif case in ('unknown', 'failed'):
    r = with_receipt_hash(replace(r, outcome=EvidenceOutcome(case)))
elif case == 'hash':
    r = replace(r, content_hash='sha256:' + '0' * 64)
elif case == 'binding':
    r = with_receipt_hash(replace(r, attempt_id='other-attempt'))
out = receipt_wire(r)
if case == 'malformed-outcome':
    obj = json.loads(out)
    obj['receipt']['outcome'] = 'COMPLETE'
    out = json.dumps(obj).encode()
sys.stdout.buffer.write(out)
"""
    adapter, event, wire, _ = fixture(
        tmp_path, command=[sys.executable, "-I", "-c", program, root, case]
    )
    if case == "stale":
        adapter.attempt.context = replace(
            adapter.attempt.context, receipt_max_age=timedelta(seconds=0.1)
        )
    await deliver(adapter, event, wire)
    assert adapter.sink == [DENIED.encode()]
    assert adapter.attempt.process_returncode == 0


@pytest.mark.asyncio
async def test_timeout_reaps_child_and_prevents_late_send(tmp_path):
    marker = tmp_path / "late"
    program = 'import pathlib,sys,time; time.sleep(3); pathlib.Path(sys.argv[1]).write_text("late"); print("COMPLETE")'
    adapter, event, wire, _ = fixture(
        tmp_path,
        timeout=0.1,
        command=[sys.executable, "-I", "-c", program, str(marker)],
    )
    await deliver(adapter, event, wire)
    assert adapter.attempt.process_returncode == -9
    assert not marker.exists()
    await adapter.deliver(event, json.dumps(wire).encode())
    assert adapter.sink == [DENIED.encode()]


@pytest.mark.asyncio
async def test_buffer_limit_denies_without_releasing_bytes(tmp_path):
    adapter, event, wire, _ = fixture(tmp_path)
    await adapter.send_draft("isolated-pilot", 1, SENTINEL * 1000)
    await deliver(adapter, event, wire)
    assert adapter.sink == [DENIED.encode()]


@pytest.mark.asyncio
async def test_invalid_unicode_draft_is_controlled_denial(tmp_path):
    adapter, event, wire, _ = fixture(tmp_path)
    await adapter.send_draft("isolated-pilot", 1, "\ud800")
    await deliver(adapter, event, wire)
    assert adapter.sink == [DENIED.encode()]


@pytest.mark.asyncio
async def test_replacement_sinks_reject_owner(tmp_path):
    from types import SimpleNamespace
    from gateway.config import PlatformConfig
    from gateway.platforms.base import BasePlatformAdapter, SendResult

    class ReplacementSink(BasePlatformAdapter):
        def __init__(self):
            super().__init__(PlatformConfig(enabled=False), Platform.LOCAL)
            self.sink = []

        async def connect(self, *, is_reconnect=False):
            return False

        async def disconnect(self):
            pass

        async def get_chat_info(self, chat_id):
            return {"type": "dm"}

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            self.sink.append(content.encode())
            return SendResult(success=True, message_id="replacement")

    adapter, event, wire, _ = fixture(tmp_path)
    await deliver(adapter, event, wire)
    replacement = ReplacementSink()
    adapter.gateway_runner = SimpleNamespace(_adapter_for_source=lambda _: replacement)
    with pytest.raises(ValueError, match="unsupported pilot owner"):
        await adapter._send_final_text(
            event, "test", SENTINEL, {}, True, 0, lambda _: None
        )
    assert not replacement.sink
    assert SENTINEL.encode() not in b"".join(adapter.sink + replacement.sink)

    other, event, wire, _ = fixture(tmp_path, "replacement")
    other.gateway_runner = adapter.gateway_runner
    with pytest.raises(ValueError, match="unsupported pilot owner"):
        await other.deliver(event, json.dumps(wire).encode(), SENTINEL)
    assert not other.attempt.drafts
    assert not other.sink and not replacement.sink


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["second-cancel", "disconnect", "cancel"])
async def test_active_terminal_owns_real_process_tree(tmp_path, mode):
    import subprocess

    ready = tmp_path / "ready"
    program = """
import subprocess, sys, os, json, pathlib, time
child = subprocess.Popen([sys.executable, '-I', '-c', 'import time; time.sleep(60)'])
ready = pathlib.Path(sys.argv[1])
staging = ready.with_suffix('.tmp')
staging.write_text(json.dumps([os.getpid(), child.pid]))
staging.replace(ready)
time.sleep(60)
"""
    adapter, event, wire, _ = fixture(
        tmp_path,
        timeout=10,
        command=[sys.executable, "-I", "-c", program, str(ready)],
    )
    task = asyncio.create_task(
        adapter.deliver(event, json.dumps(wire).encode(), SENTINEL)
    )
    ids = []
    armed = [False]

    def audit(name, args):
        if armed[0] and name == "os.killpg":
            armed[0] = False
            asyncio.get_running_loop().call_soon(task.cancel)

    sys.addaudithook(audit)
    try:
        async with asyncio.timeout(3):
            while not ready.exists():
                await asyncio.sleep(0.01)
        ids = json.loads(ready.read_text())
        if mode == "second-cancel":
            armed[0] = True
            task.cancel()
        elif mode == "disconnect":
            async with asyncio.timeout(2):
                await adapter.disconnect()
            assert task.done(), "disconnect must return only after joining delivery"
        else:
            adapter.attempt.cancel()
        done, _ = await asyncio.wait({task}, timeout=2)
        assert done, "terminal action must join active work promptly"
        await asyncio.gather(task, return_exceptions=True)
        assert adapter.attempt.process_returncode is not None
        assert not adapter.attempt.drafts
        await adapter.send_draft("isolated-pilot", 3, SENTINEL)
        assert not adapter.attempt.drafts
        assert not adapter.sink
        async with asyncio.timeout(3):
            while any(
                subprocess.run(
                    ["/bin/ps", "-p", str(pid), "-o", "pid="],
                    capture_output=True,
                    text=True,
                ).stdout.strip()
                for pid in ids
            ):
                await asyncio.sleep(0.02)
    finally:
        armed[0] = False
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("denied", [False, True])
async def test_terminal_late_drafts(tmp_path, denied):
    adapter, event, wire, _ = fixture(tmp_path)
    if denied:
        wire["task_id"] = "wrong"
    await deliver(adapter, event, wire)
    await adapter.send_draft("isolated-pilot", 2, SENTINEL)
    await adapter.deliver(event, json.dumps(wire).encode(), SENTINEL)
    assert not adapter.attempt.drafts
    assert adapter.sink == [(DENIED if denied else COMPLETE).encode()]


@pytest.mark.asyncio
async def test_disconnect_before_delivery_consumes_dispatch(tmp_path):
    adapter, event, wire, _ = fixture(tmp_path)
    await adapter.send_draft("isolated-pilot", 1, SENTINEL)
    await adapter.disconnect()
    await adapter.send_draft("isolated-pilot", 2, SENTINEL)
    await adapter.deliver(event, json.dumps(wire).encode(), SENTINEL)
    assert not adapter.sink and not adapter.attempt.drafts
    assert adapter.attempt.process_returncode is None

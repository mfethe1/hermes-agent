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
from response_verification.admission import admit_snapshot, SNAPSHOT_CLAIM

# Real process-group signals are required; all targets are newly spawned pilot children.
pytestmark = pytest.mark.live_system_guard_bypass

SENTINEL = "DRAFT_SENTINEL_899cab_DO_NOT_SEND"


def fixture(
    tmp_path, attempt_id="attempt-1", *, receipt_max_age=timedelta(seconds=30), **kwargs
):
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
        receipt_max_age,
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
    attempt = PilotAttempt(
        context, admit_snapshot(context, path, claim=SNAPSHOT_CLAIM), **kwargs
    )
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
    if case in ("task_id", "attempt_id", "environment"):
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
        tmp_path,
        command=[sys.executable, "-I", "-c", program, root, case],
        receipt_max_age=timedelta(seconds=0.1 if case == "stale" else 30),
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


@pytest.mark.asyncio
async def test_real_outer_lifecycle_success_and_stale_callback(tmp_path):
    from gateway.response_verification_pilot import OuterPilotLifecycle

    base, event, wire, _ = fixture(tmp_path)
    owner = OuterPilotLifecycle()
    callbacks = []

    async def handler(event, delta):
        callbacks.append(delta)
        delta(SENTINEL)
        await owner.active.draft_seen.wait()
        return SENTINEL

    adapter = await owner.start(
        base.attempt, base.sink, event, json.dumps(wire).encode(), handler
    )
    await asyncio.wait_for(adapter.outer_task, 4)
    assert base.sink == [COMPLETE.encode()]
    assert adapter.consumer_task.done()
    assert adapter.attempt._active_task is None
    assert adapter.attempt.process_returncode == 0
    callbacks[0](SENTINEL)
    await adapter._send_final_text(
        event, "stale", SENTINEL, {}, True, 0, lambda _: None
    )
    assert adapter.consumer._queue.empty()
    assert not adapter.consumer._accumulated and not adapter.attempt.drafts
    assert base.sink == [COMPLETE.encode()]
    with pytest.raises(ValueError, match="identity already consumed"):
        await owner.start(base.attempt, [], event, json.dumps(wire).encode(), handler)


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["stop", "replace", "reconnect"])
async def test_outer_active_verifier_lifecycle(tmp_path, action):
    import subprocess
    from types import SimpleNamespace
    from gateway.platforms.event import ProcessingOutcome
    from gateway.response_verification_pilot import OuterPilotLifecycle

    ready = tmp_path / "outer-ready"
    program = """
import subprocess, sys, os, json, pathlib, time
child = subprocess.Popen([sys.executable, '-I', '-c', 'import time; time.sleep(60)'])
p = pathlib.Path(sys.argv[1]); stage = p.with_suffix('.tmp')
stage.write_text(json.dumps([os.getpid(), child.pid])); stage.replace(p)
time.sleep(60)
"""
    base, event, wire, _ = fixture(
        tmp_path, command=[sys.executable, "-I", "-c", program, str(ready)], timeout=10
    )
    owner = OuterPilotLifecycle()
    replacement_sink = []
    callbacks = []

    async def handler(event, delta):
        callbacks.append(delta)
        delta(SENTINEL)
        await owner.active.draft_seen.wait()
        return SENTINEL

    adapter = await owner.start(
        base.attempt, base.sink, event, json.dumps(wire).encode(), handler
    )
    outcomes = []

    async def completed(event, outcome):
        outcomes.append(outcome)

    adapter.on_processing_complete = completed
    try:
        async with asyncio.timeout(4):
            while not ready.exists():
                await asyncio.sleep(0.01)
        pids = json.loads(ready.read_text())
        if action == "replace":
            other, event2, wire2, _ = fixture(tmp_path, "outer-second")
            new = await owner.start(
                other.attempt,
                replacement_sink,
                event2,
                json.dumps(wire2).encode(),
                handler,
            )
            await asyncio.wait_for(new.outer_task, 4)
            assert replacement_sink == [COMPLETE.encode()]
            assert new.consumer_task.done() and not new.attempt.drafts
        elif action == "reconnect":

            def resolve(_):
                pytest.fail("unsupported resolver must never be invoked")

            adapter.gateway_runner = SimpleNamespace(_adapter_for_source=resolve)
            # Reject routing while the real outer task owns an active verifier.
            with pytest.raises(ValueError, match="unsupported pilot owner"):
                adapter._final_delivery_adapter(event.source)
            await asyncio.gather(adapter.outer_task, return_exceptions=True)
            assert not replacement_sink
        else:
            await owner.stop()
        assert adapter.outer_task.done() and adapter.consumer_task.done()
        assert adapter.attempt._active_task is None
        assert adapter.attempt.process_returncode == -9
        assert adapter.attempt.terminal and not adapter.attempt.drafts
        callbacks[0](SENTINEL)
        await adapter._send_final_text(
            event, "stale", SENTINEL, {}, True, 0, lambda _: None
        )
        assert adapter.consumer._queue.empty() and not adapter.consumer._accumulated
        assert not base.sink
        assert outcomes == [
            ProcessingOutcome.FAILURE
            if action == "reconnect"
            else ProcessingOutcome.CANCELLED
        ]
        assert SENTINEL.encode() not in b"".join(base.sink + replacement_sink)
        async with asyncio.timeout(3):
            while any(
                subprocess.run(
                    ["/bin/ps", "-p", str(pid), "-o", "pid="],
                    capture_output=True,
                    text=True,
                ).stdout.strip()
                for pid in pids
            ):
                await asyncio.sleep(0.02)
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["stop", "reconnect", "sink-swap"])
async def test_outer_streaming_stop_and_reconnect_refusal(tmp_path, action):
    from types import SimpleNamespace
    from gateway.response_verification_pilot import OuterPilotLifecycle

    base, event, wire, _ = fixture(tmp_path)
    owner = OuterPilotLifecycle()
    release = asyncio.Event()
    entered = asyncio.Event()
    replacement_sink = []
    callbacks = []

    async def handler(event, delta):
        callbacks.append(delta)
        delta(SENTINEL)
        await owner.active.draft_seen.wait()
        entered.set()
        await release.wait()
        return SENTINEL

    adapter = await owner.start(
        base.attempt, base.sink, event, json.dumps(wire).encode(), handler
    )
    try:
        await asyncio.wait_for(entered.wait(), 3)
        if action == "stop":
            await owner.stop()
        else:
            if action == "reconnect":

                def resolve(_):
                    pytest.fail("replacement transport is unsupported")

                adapter.gateway_runner = SimpleNamespace(_adapter_for_source=resolve)
            else:
                adapter.sink = replacement_sink
            release.set()
            await asyncio.wait_for(adapter.outer_task, 3)
        callbacks[0](SENTINEL)
        await adapter._send_final_text(
            event, "stale", SENTINEL, {}, True, 0, lambda _: None
        )
        assert not base.sink and not replacement_sink
        assert adapter.attempt.process_returncode is None
        assert adapter.consumer_task.done() and adapter.outer_task.done()
        assert adapter.consumer._queue.empty() and not adapter.consumer._accumulated
        assert adapter.attempt.terminal and not adapter.attempt.drafts
        assert not adapter._active_sessions
    finally:
        await owner.stop()


@pytest.mark.asyncio
async def test_ordinary_outer_adapter_keeps_handler_final(tmp_path):
    from gateway.config import PlatformConfig
    from gateway.platforms.base import BasePlatformAdapter, EphemeralReply, SendResult

    class OrdinaryLocalAdapter(BasePlatformAdapter):
        def __init__(self):
            super().__init__(
                PlatformConfig(enabled=False, typing_indicator=False), Platform.LOCAL
            )
            self.sink = []

        async def connect(self, *, is_reconnect=False):
            return False

        async def disconnect(self):
            pass

        async def get_chat_info(self, chat_id):
            return {"type": "dm"}

        async def send(self, chat_id, content, reply_to=None, metadata=None):
            self.sink.append(content)
            return SendResult(success=True, message_id="ordinary")

    adapter = OrdinaryLocalAdapter()

    async def handler(event):
        return EphemeralReply("Ordinary local handler final", ttl_seconds=0)

    adapter._message_handler = handler
    event = MessageEvent(text="local", source=SessionSource(Platform.LOCAL, "ordinary"))
    adapter._start_session_processing(event, "ordinary-session")
    task = adapter._session_tasks["ordinary-session"]
    await task
    assert adapter.sink == ["Ordinary local handler final"]
    assert not adapter._active_sessions


@pytest.mark.asyncio
@pytest.mark.parametrize("action", ["stop", "replace"])
async def test_outer_prestart_reconciles_exact_ownership(tmp_path, action):
    from gateway.response_verification_pilot import OuterPilotLifecycle

    owner = OuterPilotLifecycle()
    base, event, wire, _ = fixture(tmp_path)
    other, event2, wire2, _ = fixture(tmp_path, "prestart-new")
    entered = []
    release = asyncio.Event()

    async def handler(event, delta):
        entered.append(event)
        await release.wait()
        return SENTINEL

    old = await owner.start(
        base.attempt, base.sink, event, json.dumps(wire).encode(), handler
    )
    # Deliberately no yield between start and stop/replacement.
    if action == "stop":
        await owner.stop()
    else:
        new = await owner.start(
            other.attempt, other.sink, event2, json.dumps(wire2).encode(), handler
        )
    try:
        assert not old._active_sessions and not old._session_tasks
        assert old.outer_task.cancelled() and old.consumer is None
        assert base.attempt.terminal and base.attempt._active_task is None
        assert not base.sink and not base.attempt.drafts
        assert event not in entered
        if action == "replace":
            guard = new._active_sessions["isolated-outer"]
            await asyncio.gather(old.stop(), old.stop())
            assert owner.active is new
            assert new._active_sessions["isolated-outer"] is guard
            assert new._session_tasks["isolated-outer"] is new.outer_task
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode", ["success", "sink-failure", "handler-failure", "stop", "unexpected-cancel"]
)
async def test_outer_reports_actual_delivery_and_cancellation(tmp_path, mode):
    from gateway.platforms.event import ProcessingOutcome
    from gateway.response_verification_pilot import OuterPilotLifecycle

    class FailingSink(list):
        def append(self, value):
            raise OSError("controlled local transport failed")

    base, event, wire, _ = fixture(tmp_path)
    owner = OuterPilotLifecycle()
    entered = asyncio.Event()
    release = asyncio.Event()
    outcomes, callbacks = [], []

    async def handler(event, delta):
        callbacks.append(delta)
        delta(SENTINEL)
        entered.set()
        await release.wait()
        if mode == "handler-failure":
            raise RuntimeError("local handler failed")
        return SENTINEL

    sink = FailingSink() if mode == "sink-failure" else base.sink
    adapter = await owner.start(
        base.attempt, sink, event, json.dumps(wire).encode(), handler
    )

    async def completed(event, outcome):
        outcomes.append(outcome)

    adapter.on_processing_complete = completed
    try:
        await asyncio.wait_for(entered.wait(), 3)
        if mode == "stop":
            await owner.stop()
        elif mode == "unexpected-cancel":
            adapter.outer_task.cancel()
        else:
            release.set()
        await asyncio.gather(adapter.outer_task, return_exceptions=True)
        expected = {
            "success": ProcessingOutcome.SUCCESS,
            "stop": ProcessingOutcome.CANCELLED,
        }.get(mode, ProcessingOutcome.FAILURE)
        assert outcomes == [expected]
        if mode in ("success", "sink-failure"):
            assert base.attempt.process_returncode == 0
            assert base.attempt.receipt is not None
        assert sink == ([COMPLETE.encode()] if mode == "success" else [])
        callbacks[0](SENTINEL)
        duplicate_results = []
        await asyncio.gather(
            *(
                adapter._send_final_text(
                    event,
                    "isolated-outer",
                    SENTINEL,
                    {},
                    True,
                    0,
                    duplicate_results.append,
                )
                for _ in range(12)
            )
        )
        assert duplicate_results == [] and outcomes == [expected]
        assert not adapter._active_sessions and not adapter._session_tasks
        assert adapter.consumer._queue.empty() and not base.attempt.drafts
    finally:
        await owner.stop()


@pytest.mark.asyncio
@pytest.mark.parametrize("swap", ["guard", "task", "both"])
async def test_outer_stop_preserves_reassigned_session_entries(tmp_path, swap):
    from gateway.response_verification_pilot import OuterPilotLifecycle

    base, event, wire, _ = fixture(tmp_path)
    owner = OuterPilotLifecycle()

    async def handler(event, delta):
        pytest.fail("prestart cancellation must not enter handler")

    old = await owner.start(base.attempt, [], event, json.dumps(wire).encode(), handler)
    replacement_guard = asyncio.Event()
    replacement_task = asyncio.create_task(asyncio.Event().wait())
    original_guard = old._active_sessions["isolated-outer"]
    if swap in ("guard", "both"):
        old._active_sessions["isolated-outer"] = replacement_guard
    if swap in ("task", "both"):
        old._session_tasks["isolated-outer"] = replacement_task
    try:
        await old.stop()
        assert old._active_sessions["isolated-outer"] is (
            original_guard if swap == "task" else replacement_guard
        )
        if swap == "guard":
            assert not old._session_tasks
        else:
            assert old._session_tasks["isolated-outer"] is replacement_task
        assert not replacement_task.done()
    finally:
        replacement_task.cancel()
        await asyncio.gather(replacement_task, return_exceptions=True)


@pytest.mark.asyncio
async def test_outer_concurrent_replacements_keep_only_latest_owner(tmp_path):
    from gateway.response_verification_pilot import OuterPilotLifecycle

    owner = OuterPilotLifecycle()
    release = asyncio.Event()
    callbacks = []

    async def handler(event, delta):
        callbacks.append(delta)
        delta(SENTINEL)
        await release.wait()
        return SENTINEL

    bases = [fixture(tmp_path, f"concurrent-{i}") for i in range(12)]
    adapters = await asyncio.gather(
        *(
            owner.start(
                base.attempt, base.sink, event, json.dumps(wire).encode(), handler
            )
            for base, event, wire, _ in bases
        )
    )
    latest = owner.active
    try:
        retired = [adapter for adapter in adapters if adapter is not latest]
        assert len(retired) == 11
        assert all(adapter.outer_task.done() for adapter in retired)
        assert all(
            not adapter._active_sessions and not adapter._session_tasks
            for adapter in retired
        )
        guard = latest._active_sessions["isolated-outer"]
        await asyncio.gather(*(adapter.stop() for adapter in retired))
        assert owner.active is latest
        assert latest._active_sessions["isolated-outer"] is guard
        assert latest._session_tasks["isolated-outer"] is latest.outer_task
        assert not latest.outer_task.done()
        release.set()
        await asyncio.wait_for(latest.outer_task, 4)
        for callback in callbacks:
            callback(SENTINEL)
        assert latest.sink == [COMPLETE.encode()]
        assert latest.attempt.process_returncode == 0
        assert all(
            not adapter.sink and not adapter.attempt.drafts for adapter in retired
        )
        assert latest.consumer._queue.empty()
    finally:
        await owner.stop()

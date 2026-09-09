"""Real byte admission, subprocess collection and controlled delivery regressions."""

import asyncio
import base64
from dataclasses import FrozenInstanceError, replace
from datetime import timedelta
import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from response_verification.admission import (
    MAX_SNAPSHOT_BYTES,
    SNAPSHOT_CLAIM,
    admit_snapshot,
    context_wire,
    snapshot_digest,
)
from response_verification.pilot import COMPLETE, DENIED, PilotAttempt
from response_verification.pilot_wire import LIMIT, strict_json
from tests.response_verification.test_pilot import deliver, fixture

pytestmark = pytest.mark.live_system_guard_bypass


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["overwrite", "rename", "symlink", "missing"])
async def test_admitted_bytes_survive_original_path_changes(tmp_path, change):
    adapter, event, wire, path = fixture(tmp_path)
    captured = adapter.attempt.admission.data
    if change == "overwrite":
        path.write_bytes(b"changed after admission")
    elif change == "rename":
        replacement = tmp_path / "replacement"
        replacement.write_bytes(b"replacement bytes")
        replacement.replace(path)
    elif change == "symlink":
        path.unlink()
        replacement = tmp_path / "target"
        replacement.write_bytes(b"symlink bytes")
        path.symlink_to(replacement)
    else:
        path.unlink()
    await deliver(adapter, event, wire)
    assert adapter.sink == [COMPLETE.encode()]
    assert adapter.attempt.receipt.artifact_digest == snapshot_digest(captured)
    assert "admitted snapshot bytes only" in COMPLETE
    # The same admission cannot authorize a current-file interpretation.
    with pytest.raises(ValueError, match="claim"):
        admit_snapshot(adapter.attempt.context, path, claim="current-path")


def test_capture_bounds_digest_and_nested_defensive_copy(tmp_path):
    adapter, _, _, path = fixture(tmp_path)
    original = adapter.attempt.context
    criteria = list(original.criteria)
    issuers = set(original.trusted_issuers)
    mutable = replace(original, criteria=criteria, trusted_issuers=issuers)
    admission = admit_snapshot(mutable, path, claim=SNAPSHOT_CLAIM)
    criteria.clear()
    issuers.clear()
    admission.validate(original)
    assert admission.context.criteria == original.criteria
    assert admission.context.trusted_issuers == original.trusted_issuers
    with pytest.raises(FrozenInstanceError):
        admission.data = b"changed"
    with pytest.raises(FrozenInstanceError):
        admission.context.criteria[0].criterion_id = "changed"
    with pytest.raises(AttributeError):
        adapter.attempt.admission = admission
    with pytest.raises(ValueError, match="explicit"):
        PilotAttempt(original, path)
    path.write_bytes(b"x" * (MAX_SNAPSHOT_BYTES + 1))
    context = replace(original, artifact_digest=snapshot_digest(path.read_bytes()))
    with pytest.raises(ValueError, match="size"):
        admit_snapshot(context, path, claim=SNAPSHOT_CLAIM)
    path.write_bytes(b"wrong approved bytes")
    with pytest.raises(ValueError, match="digest"):
        admit_snapshot(original, path, claim=SNAPSHOT_CLAIM)
    path.unlink()
    with pytest.raises(FileNotFoundError):
        admit_snapshot(original, path, claim=SNAPSHOT_CLAIM)
    path.symlink_to(tmp_path / "missing-target")
    with pytest.raises(OSError):
        admit_snapshot(original, path, claim=SNAPSHOT_CLAIM)
    path.unlink()
    os.mkfifo(path)
    with pytest.raises(ValueError, match="file"):
        admit_snapshot(original, path, claim=SNAPSHOT_CLAIM)


@pytest.mark.asyncio
async def test_maximum_binary_snapshot_and_expiration(tmp_path):
    adapter, event, wire, path = fixture(tmp_path)
    path.write_bytes(bytes(range(256)) * (MAX_SNAPSHOT_BYTES // 256))
    context = replace(
        adapter.attempt.context, artifact_digest=snapshot_digest(path.read_bytes())
    )
    admission = admit_snapshot(context, path, claim=SNAPSHOT_CLAIM)
    adapter.attempt = PilotAttempt(context, admission)
    wire["artifact_digest"] = context.artifact_digest
    await deliver(adapter, event, wire)
    assert adapter.sink == [COMPLETE.encode()]
    stale = admit_snapshot(
        context, path, claim=SNAPSHOT_CLAIM, max_age=timedelta(milliseconds=10)
    )
    attempt = PilotAttempt(context, stale)
    await asyncio.sleep(0.02)
    await attempt.prepare(json.dumps(wire).encode())
    assert attempt.disposition() == DENIED
    assert attempt.process_returncode is None
    with pytest.raises(ValueError, match="stale"):
        PilotAttempt(context, stale)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [
        "task_id",
        "attempt_id",
        "environment",
        "approval_id",
        "approved_request_digest",
        "artifact_digest",
        "agent_identity",
        "criteria",
        "trusted_issuers",
        "receipt_max_age",
        "evaluated_at",
    ],
)
async def test_context_changes_invalidate_even_if_reverted(tmp_path, field):
    adapter, _, wire, _ = fixture(tmp_path)
    attempt = adapter.attempt
    await attempt.prepare(json.dumps(wire).encode())
    assert attempt.disposition() == COMPLETE
    original = attempt.context
    value = getattr(original, field)
    if field == "criteria":
        changed = ()
    elif field == "trusted_issuers":
        changed = frozenset({"other"})
    elif field == "receipt_max_age":
        changed = value + timedelta(seconds=1)
    elif field == "evaluated_at":
        changed = value + timedelta(seconds=1)
    else:
        changed = "changed"
    changed_context = replace(original, **{field: changed})
    with pytest.raises(ValueError):
        PilotAttempt(changed_context, attempt.admission)
    attempt.context = changed_context
    assert attempt.disposition() == DENIED
    attempt.context = original
    assert attempt.disposition() == DENIED


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["admission", "artifact_path", "data_base64", "claim"]
)
async def test_model_cannot_select_admission(tmp_path, field):
    adapter, event, wire, _ = fixture(tmp_path)
    wire[field] = "attacker-selected"
    await deliver(adapter, event, wire)
    assert adapter.sink == [DENIED.encode()]
    assert adapter.attempt.process_returncode is None


@pytest.mark.parametrize(
    "case",
    [
        "oversize",
        "base64",
        "noncanonical",
        "digest",
        "expired",
        "claim",
        "extra",
        "context",
        "utf16",
        "depth",
        "duplicate",
    ],
)
def test_real_worker_rejects_malicious_private_job(tmp_path, case):
    adapter, _, wire, _ = fixture(tmp_path)
    admission = adapter.attempt.admission
    job = {
        "context": context_wire(admission.context),
        "admission": admission.wire(),
        "envelope": json.dumps(wire),
    }
    if case == "oversize":
        job["admission"]["data_base64"] = base64.b64encode(
            b"x" * (MAX_SNAPSHOT_BYTES + 1)
        ).decode()
    elif case == "base64":
        job["admission"]["data_base64"] = "!!!!"
    elif case == "noncanonical":
        job["admission"]["data_base64"] += "="
    elif case == "digest":
        job["admission"]["digest"] = "sha256:" + "0" * 64
    elif case == "expired":
        job["admission"]["expires_at"] = admission.admitted_at.isoformat()
    elif case == "claim":
        job["admission"]["claim"] = "current-path"
    elif case == "extra":
        job["artifact_path"] = "/does/not/exist"
    elif case == "context":
        job["context"]["unexpected"] = "injected"
    raw = json.dumps(job).encode()
    if case == "utf16":
        raw = json.dumps(job).encode("utf-16")
    elif case == "depth":
        raw = b"[" * 2000 + b"0" + b"]" * 2000
    elif case == "duplicate":
        raw = raw.replace(b"{", b'{"context":{},', 1)
    worker = (
        Path(__file__).resolve().parents[2] / "response_verification/pilot_worker.py"
    )
    result = subprocess.run(
        [sys.executable, "-I", str(worker), "verify"],
        input=raw,
        capture_output=True,
        timeout=3,
    )
    assert result.returncode != 0
    assert result.stdout == b""


def test_wire_rejects_before_parsing_unbounded_or_invalid_encoding():
    for raw in (b" " * (LIMIT + 1), b"[" * 2000, b'"\xff"', "{}".encode("utf-16")):
        with pytest.raises((ValueError, UnicodeError)):
            strict_json(raw)


@pytest.mark.asyncio
async def test_expiration_rechecked_at_controlled_sink(tmp_path):
    adapter, event, wire, path = fixture(tmp_path)
    context = adapter.attempt.context
    admission = admit_snapshot(
        context, path, claim=SNAPSHOT_CLAIM, max_age=timedelta(seconds=2)
    )
    adapter.attempt = PilotAttempt(context, admission)
    await adapter.attempt.prepare(json.dumps(wire).encode())
    assert adapter.attempt.disposition() == COMPLETE
    await asyncio.sleep(2.01)
    adapter._sending = True
    await adapter.send(event.source.chat_id, "COMPLETE")
    assert adapter.sink == [DENIED.encode()]


@pytest.mark.asyncio
async def test_empty_approved_snapshot(tmp_path):
    adapter, event, wire, path = fixture(tmp_path)
    path.write_bytes(b"")
    context = replace(adapter.attempt.context, artifact_digest=snapshot_digest(b""))
    admission = admit_snapshot(context, path, claim=SNAPSHOT_CLAIM)
    adapter.attempt = PilotAttempt(context, admission)
    wire["artifact_digest"] = context.artifact_digest
    await deliver(adapter, event, wire)
    assert adapter.sink == [COMPLETE.encode()]
    assert adapter.attempt.receipt.artifact_digest == snapshot_digest(b"")

"""Local pilot subprocesses; same UID is NOT authenticated independent identity."""

from dataclasses import replace
import base64
import math
import json
from pathlib import Path
import subprocess
import sys
from datetime import datetime, timedelta, timezone

# Launched with -I; use only this checkout's package, not ambient PYTHONPATH.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from response_verification.core import (  # noqa: E402
    ApprovedContext,
    CriterionKind,
    CriterionReference,
    EvidenceKind,
    EvidenceOrigin,
    EvidenceOutcome,
    TrustedReceipt,
    evaluate_completion,
    with_receipt_hash,
)
from response_verification.pilot_wire import (  # noqa: E402
    LIMIT,
    decode_envelope,
    decode_receipt,
    receipt_wire,
    strict_json,
    fields,
)


from response_verification.admission import (  # noqa: E402
    MAX_ADMISSION_AGE,
    MAX_SNAPSHOT_BYTES,
    SNAPSHOT_CLAIM,
    freeze_context,
    snapshot_digest,
)


def context_from_job(job):
    fields(job, ("context", "admission", "envelope"))
    fields(job["context"], ApprovedContext.__dataclass_fields__)
    value = dict(job["context"])
    if type(job["envelope"]) is not str:
        raise ValueError("envelope type")
    if type(value["criteria"]) is not list or len(value["criteria"]) != 1:
        raise ValueError("criteria")
    for c in value["criteria"]:
        fields(c, ("criterion_id", "criterion_digest", "kind"))
    value["criteria"] = tuple(
        CriterionReference(
            c["criterion_id"], c["criterion_digest"], CriterionKind(c["kind"])
        )
        for c in value["criteria"]
    )
    issuers = value["trusted_issuers"]
    if type(issuers) is not list or not 1 <= len(issuers) <= 24:
        raise ValueError("issuers")
    value["trusted_issuers"] = frozenset(issuers)
    value["evaluated_at"] = datetime.fromisoformat(value["evaluated_at"])
    age = value["receipt_max_age"]
    if type(age) not in (int, float) or not math.isfinite(age) or not 0 < age <= 86400:
        raise ValueError("receipt lifetime")
    value["receipt_max_age"] = timedelta(seconds=age)
    return freeze_context(ApprovedContext(**value))


def admitted_bytes(job, context):
    admission = job["admission"]
    fields(admission, ("claim", "data_base64", "digest", "admitted_at", "expires_at"))
    encoded = admission["data_base64"]
    if type(encoded) is not str or len(encoded) > 4 * ((MAX_SNAPSHOT_BYTES + 2) // 3):
        raise ValueError("snapshot encoding size")
    data = base64.b64decode(encoded, validate=True)
    if (
        len(data) > MAX_SNAPSHOT_BYTES
        or base64.b64encode(data).decode("ascii") != encoded
    ):
        raise ValueError("snapshot encoding")
    start = datetime.fromisoformat(admission["admitted_at"])
    end = datetime.fromisoformat(admission["expires_at"])
    now = datetime.now(timezone.utc)
    if (
        admission["claim"] != SNAPSHOT_CLAIM
        or not start <= now < end
        or not timedelta(0) < end - start <= MAX_ADMISSION_AGE
        or admission["digest"] != snapshot_digest(data)
        or admission["digest"] != context.artifact_digest
    ):
        raise ValueError("snapshot binding/lifetime")
    return data


def collect(job):
    context = context_from_job(job)
    data = admitted_bytes(job, context)
    digest = snapshot_digest(data)
    criterion = context.criteria[0]
    good = digest == context.artifact_digest
    receipt = with_receipt_hash(
        TrustedReceipt(
            receipt_id="local-observation",
            issuer="pilot-collector",
            origin=EvidenceOrigin.INDEPENDENT_COLLECTOR,
            observed_at=datetime.now(timezone.utc),
            task_id=context.task_id,
            attempt_id=context.attempt_id,
            artifact_digest=digest,
            environment=context.environment,
            approved_request_digest=context.approved_request_digest,
            criterion_id=criterion.criterion_id,
            criterion_digest=criterion.criterion_digest,
            evidence_kind=EvidenceKind.OBSERVATION,
            outcome=EvidenceOutcome.VERIFIED if good else EvidenceOutcome.FAILED,
        )
    )
    return receipt_wire(receipt)


def main():
    raw = sys.stdin.buffer.read(LIMIT + 1)
    job = strict_json(raw)
    context = context_from_job(job)
    admitted_bytes(job, context)
    if sys.argv[1] == "collect":
        result = collect(job)
    else:
        envelope = decode_envelope(job["envelope"].encode())
        child = subprocess.run(
            [sys.executable, "-I", __file__, "collect"],
            input=raw,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            timeout=1,
            check=True,
        )
        result = child.stdout
        receipt = decode_receipt(result)
        # Real evaluator in the verifier process. The parent rechecks at dispatch.
        current = replace(context, evaluated_at=datetime.now(timezone.utc))
        if not evaluate_completion(current, envelope, (receipt,)).eligible:
            raise ValueError("ineligible snapshot")
    sys.stdout.buffer.write(result)


if __name__ == "__main__":
    main()

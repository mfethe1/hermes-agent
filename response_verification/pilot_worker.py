"""Local pilot subprocesses; same UID is NOT authenticated independent identity."""

import hashlib
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
)


def context_from_job(job):
    value = dict(job["context"])
    value["criteria"] = tuple(
        CriterionReference(
            c["criterion_id"], c["criterion_digest"], CriterionKind(c["kind"])
        )
        for c in value["criteria"]
    )
    value["trusted_issuers"] = frozenset(value["trusted_issuers"])
    value["evaluated_at"] = datetime.now(timezone.utc)
    value["receipt_max_age"] = timedelta(seconds=value["receipt_max_age"])
    return ApprovedContext(**value)


def collect(job):
    context = context_from_job(job)
    with open(job["artifact_path"], "rb") as artifact:
        data = artifact.read(1024 * 1024 + 1)
    digest = "sha256:" + hashlib.sha256(data).hexdigest()
    criterion = context.criteria[0]
    good = len(data) <= 1024 * 1024 and digest == context.artifact_digest
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
        evaluate_completion(context_from_job(job), envelope, (receipt,))
    sys.stdout.buffer.write(result)


if __name__ == "__main__":
    main()

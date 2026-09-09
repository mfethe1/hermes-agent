"""Bounded v1 wire format. Model transport contains references only."""

import json
from dataclasses import asdict
from datetime import datetime

from response_verification.core import (
    CompletionEnvelope,
    CriterionKind,
    CriterionReference,
    EvidenceKind,
    EvidenceOrigin,
    EvidenceOutcome,
    TrustedReceipt,
)

LIMIT = 16384
BINDINGS = (
    "task_id",
    "attempt_id",
    "artifact_digest",
    "environment",
    "approved_request_digest",
)


def strict_json(raw):
    if type(raw) is not bytes or len(raw) > LIMIT:
        raise ValueError("wire size/type")

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate key")
            result[key] = value
        return result

    value = json.loads(
        raw,
        object_pairs_hook=pairs,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError("constant")),
    )

    def bounded(item, depth=0):
        if depth > 6:
            raise ValueError("depth")
        if isinstance(item, (dict, list)):
            if len(item) > 24:
                raise ValueError("collection size")
            for child in item.values() if isinstance(item, dict) else item:
                bounded(child, depth + 1)

    bounded(value)
    return value


def fields(value, expected):
    if type(value) is not dict or set(value) != set(expected):
        raise ValueError("fields")


def strings(value, names):
    if any(type(value[k]) is not str or not 0 < len(value[k]) <= 256 for k in names):
        raise ValueError("string fields")


def decode_envelope(raw):
    value = strict_json(raw)
    fields(value, (*BINDINGS, "schema_version", "criteria"))
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("version")
    strings(value, BINDINGS)
    if type(value["criteria"]) is not list or len(value["criteria"]) != 1:
        raise ValueError("one state criterion required")
    criterion = value["criteria"][0]
    fields(criterion, ("criterion_id", "criterion_digest", "kind"))
    strings(criterion, criterion)
    if criterion["kind"] != "state":
        raise ValueError("state only")
    return CompletionEnvelope(
        **{k: value[k] for k in BINDINGS},
        criteria=(
            CriterionReference(
                criterion["criterion_id"],
                criterion["criterion_digest"],
                CriterionKind.STATE,
            ),
        ),
    )


def receipt_wire(receipt):
    value = asdict(receipt)
    value["observed_at"] = receipt.observed_at.isoformat()
    return json.dumps({"schema_version": 1, "receipt": value}).encode()


def decode_receipt(raw):
    value = strict_json(raw)
    fields(value, ("schema_version", "receipt"))
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("version")
    receipt = value["receipt"]
    fields(receipt, TrustedReceipt.__dataclass_fields__)
    strings(receipt, set(receipt) - {"related_receipt_hash"})
    if receipt["related_receipt_hash"] is not None:
        raise ValueError("state has no effect association")
    receipt["observed_at"] = datetime.fromisoformat(receipt["observed_at"])
    receipt["origin"] = EvidenceOrigin(receipt["origin"])
    receipt["outcome"] = EvidenceOutcome(receipt["outcome"])
    receipt["evidence_kind"] = EvidenceKind(receipt["evidence_kind"])
    return TrustedReceipt(**receipt)

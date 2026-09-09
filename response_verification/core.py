"""Deterministic, fail-closed eligibility checks for completion statuses.

This module deliberately has no draft-text field and no signing key. Callers must
obtain :class:`ApprovedContext` and :class:`TrustedReceipt` objects from trusted
boundaries outside the model. Receipt hashes provide integrity binding only; issuer
authentication belongs to the future evidence-collector boundary.
"""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Iterable

_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}\Z")
_DIGEST = re.compile(r"sha256:[0-9a-f]{64}\Z")


class CriterionKind(str, Enum):
    STATE = "state"
    EXTERNAL_EFFECT = "external_effect"


class EvidenceKind(str, Enum):
    OBSERVATION = "observation"
    EFFECT = "effect"
    READ_BACK = "read_back"


class EvidenceOrigin(str, Enum):
    INDEPENDENT_COLLECTOR = "independent_collector"
    MODEL_SELF_REPORT = "model_self_report"


class EvidenceOutcome(str, Enum):
    VERIFIED = "verified"
    FAILED = "failed"
    UNKNOWN = "unknown"


class CompletionStatus(str, Enum):
    COMPLETE = "complete"
    INCOMPLETE = "incomplete"
    UNKNOWN = "unknown"


class ReasonCode(str, Enum):
    TRUST_CONTEXT_INVALID = "trust_context_invalid"
    MISSING_CRITERIA = "missing_criteria"
    EMPTY_CRITERIA = "empty_criteria"
    DUPLICATE_CRITERIA = "duplicate_criteria"
    CRITERIA_MISMATCH = "criteria_mismatch"
    TASK_MISMATCH = "task_mismatch"
    ATTEMPT_MISMATCH = "attempt_mismatch"
    ARTIFACT_MISMATCH = "artifact_mismatch"
    ENVIRONMENT_MISMATCH = "environment_mismatch"
    APPROVAL_MISMATCH = "approval_mismatch"
    MISSING_EVIDENCE = "missing_evidence"
    DUPLICATE_EVIDENCE = "duplicate_evidence"
    UNEXPECTED_EVIDENCE = "unexpected_evidence"
    RECEIPT_HASH_INVALID = "receipt_hash_invalid"
    RECEIPT_TIME_INVALID = "receipt_time_invalid"
    RECEIPT_STALE = "receipt_stale"
    RECEIPT_ISSUER_UNTRUSTED = "receipt_issuer_untrusted"
    SELF_REPORTED_EVIDENCE = "self_reported_evidence"
    RECEIPT_CRITERION_MISMATCH = "receipt_criterion_mismatch"
    EVIDENCE_NOT_VERIFIED = "evidence_not_verified"
    EXTERNAL_EFFECT_UNKNOWN = "external_effect_unknown"
    READ_BACK_MISSING = "read_back_missing"
    READ_BACK_MISMATCH = "read_back_mismatch"


@dataclass(frozen=True)
class CriterionReference:
    criterion_id: str
    criterion_digest: str
    kind: CriterionKind


@dataclass(frozen=True)
class ApprovedContext:
    """Approval and execution bindings supplied outside model-controlled input."""

    task_id: str
    attempt_id: str
    artifact_digest: str
    environment: str
    approval_id: str
    approved_request_digest: str
    criteria: tuple[CriterionReference, ...]
    trusted_issuers: frozenset[str]
    agent_identity: str
    evaluated_at: datetime
    receipt_max_age: timedelta


@dataclass(frozen=True)
class CompletionEnvelope:
    """Model-side request containing references only, never releasable prose."""

    task_id: str
    attempt_id: str
    artifact_digest: str
    environment: str
    approved_request_digest: str
    criteria: tuple[CriterionReference, ...] | None


@dataclass(frozen=True)
class TrustedReceipt:
    """Record admitted by a future independently authenticated collector."""

    receipt_id: str
    issuer: str
    origin: EvidenceOrigin
    observed_at: datetime
    task_id: str
    attempt_id: str
    artifact_digest: str
    environment: str
    approved_request_digest: str
    criterion_id: str
    criterion_digest: str
    evidence_kind: EvidenceKind
    outcome: EvidenceOutcome
    related_receipt_hash: str | None = None
    content_hash: str = ""


@dataclass(frozen=True)
class CompletionDecision:
    status: CompletionStatus
    reasons: tuple[ReasonCode, ...]
    accepted_receipt_hashes: tuple[str, ...] = ()

    @property
    def eligible(self) -> bool:
        return self.status is CompletionStatus.COMPLETE


def _canonical_digest(value: object) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


def criterion_digest(
    criterion_id: str,
    kind: CriterionKind,
    approved_requirement: str,
) -> str:
    """Bind a criterion reference to approval-layer requirement text."""

    return _canonical_digest({
        "schema": "response-verification/criterion/v1",
        "criterion_id": criterion_id,
        "kind": kind.value,
        "approved_requirement": approved_requirement,
    })


def approved_request_digest(criteria: Iterable[CriterionReference]) -> str:
    """Bind exact criteria; this digest is not itself proof of approval."""

    ordered = sorted(
        (
            {
                "criterion_id": item.criterion_id,
                "criterion_digest": item.criterion_digest,
                "kind": item.kind.value,
            }
            for item in criteria
        ),
        key=lambda item: item["criterion_id"],
    )
    return _canonical_digest({
        "schema": "response-verification/approved-request/v1",
        "criteria": ordered,
    })


def _utc_text(value: datetime) -> str:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("receipt timestamps must be timezone-aware")
    return value.astimezone(timezone.utc).isoformat(timespec="microseconds")


def _receipt_payload(receipt: TrustedReceipt) -> dict[str, str | None]:
    return {
        "schema": "response-verification/receipt/v1",
        "receipt_id": receipt.receipt_id,
        "issuer": receipt.issuer,
        "origin": receipt.origin.value,
        "observed_at": _utc_text(receipt.observed_at),
        "task_id": receipt.task_id,
        "attempt_id": receipt.attempt_id,
        "artifact_digest": receipt.artifact_digest,
        "environment": receipt.environment,
        "approved_request_digest": receipt.approved_request_digest,
        "criterion_id": receipt.criterion_id,
        "criterion_digest": receipt.criterion_digest,
        "evidence_kind": receipt.evidence_kind.value,
        "outcome": receipt.outcome.value,
        "related_receipt_hash": receipt.related_receipt_hash,
    }


def with_receipt_hash(receipt: TrustedReceipt) -> TrustedReceipt:
    """Return a receipt with a canonical integrity hash (not a signature)."""

    return replace(receipt, content_hash=_canonical_digest(_receipt_payload(receipt)))


def _valid_identifier(value: str) -> bool:
    return bool(_IDENTIFIER.fullmatch(value))


def _valid_digest(value: str) -> bool:
    return bool(_DIGEST.fullmatch(value))


def _valid_context(context: ApprovedContext) -> bool:
    identifiers = (
        context.task_id,
        context.attempt_id,
        context.environment,
        context.approval_id,
        context.agent_identity,
    )
    if not all(_valid_identifier(value) for value in identifiers):
        return False
    if not context.criteria or context.receipt_max_age <= timedelta(0):
        return False
    if context.evaluated_at.tzinfo is None or context.evaluated_at.utcoffset() is None:
        return False
    if not _valid_digest(context.artifact_digest):
        return False
    if not _valid_digest(context.approved_request_digest):
        return False
    criterion_ids = [item.criterion_id for item in context.criteria]
    if len(set(criterion_ids)) != len(criterion_ids):
        return False
    if any(
        not _valid_identifier(item.criterion_id)
        or not _valid_digest(item.criterion_digest)
        or not isinstance(item.kind, CriterionKind)
        for item in context.criteria
    ):
        return False
    if approved_request_digest(context.criteria) != context.approved_request_digest:
        return False
    return bool(context.trusted_issuers) and all(
        _valid_identifier(issuer) for issuer in context.trusted_issuers
    )


def _binding_reasons(
    context: ApprovedContext,
    task_id: str,
    attempt_id: str,
    artifact_digest: str,
    environment: str,
    approval_digest: str,
) -> set[ReasonCode]:
    reasons: set[ReasonCode] = set()
    comparisons = (
        (task_id, context.task_id, ReasonCode.TASK_MISMATCH),
        (attempt_id, context.attempt_id, ReasonCode.ATTEMPT_MISMATCH),
        (artifact_digest, context.artifact_digest, ReasonCode.ARTIFACT_MISMATCH),
        (environment, context.environment, ReasonCode.ENVIRONMENT_MISMATCH),
        (
            approval_digest,
            context.approved_request_digest,
            ReasonCode.APPROVAL_MISMATCH,
        ),
    )
    for actual, expected, reason in comparisons:
        if actual != expected:
            reasons.add(reason)
    return reasons


def _criteria_reasons(
    context: ApprovedContext,
    envelope: CompletionEnvelope,
) -> set[ReasonCode]:
    if envelope.criteria is None:
        return {ReasonCode.MISSING_CRITERIA}
    if not envelope.criteria:
        return {ReasonCode.EMPTY_CRITERIA}
    ids = [item.criterion_id for item in envelope.criteria]
    reasons: set[ReasonCode] = set()
    if len(ids) != len(set(ids)):
        reasons.add(ReasonCode.DUPLICATE_CRITERIA)
    if set(envelope.criteria) != set(context.criteria):
        reasons.add(ReasonCode.CRITERIA_MISMATCH)
    return reasons


def _receipt_reasons(
    context: ApprovedContext,
    receipts: tuple[TrustedReceipt, ...],
) -> tuple[set[ReasonCode], tuple[str, ...]]:
    reasons: set[ReasonCode] = set()
    criteria = {item.criterion_id: item for item in context.criteria}
    by_criterion: dict[str, list[TrustedReceipt]] = {
        criterion_id: [] for criterion_id in criteria
    }
    receipt_ids: set[str] = set()
    receipt_hashes: set[str] = set()

    for receipt in receipts:
        if receipt.receipt_id in receipt_ids or receipt.content_hash in receipt_hashes:
            reasons.add(ReasonCode.DUPLICATE_EVIDENCE)
        receipt_ids.add(receipt.receipt_id)
        receipt_hashes.add(receipt.content_hash)

        criterion = criteria.get(receipt.criterion_id)
        if criterion is None:
            reasons.add(ReasonCode.UNEXPECTED_EVIDENCE)
        else:
            by_criterion[criterion.criterion_id].append(receipt)
            if receipt.criterion_digest != criterion.criterion_digest:
                reasons.add(ReasonCode.RECEIPT_CRITERION_MISMATCH)

        reasons.update(
            _binding_reasons(
                context,
                receipt.task_id,
                receipt.attempt_id,
                receipt.artifact_digest,
                receipt.environment,
                receipt.approved_request_digest,
            )
        )
        if (
            receipt.origin is not EvidenceOrigin.INDEPENDENT_COLLECTOR
            or receipt.issuer == context.agent_identity
        ):
            reasons.add(ReasonCode.SELF_REPORTED_EVIDENCE)
        if receipt.issuer not in context.trusted_issuers:
            reasons.add(ReasonCode.RECEIPT_ISSUER_UNTRUSTED)

        try:
            expected_hash = _canonical_digest(_receipt_payload(receipt))
        except (AttributeError, ValueError):
            reasons.add(ReasonCode.RECEIPT_TIME_INVALID)
        else:
            if receipt.content_hash != expected_hash:
                reasons.add(ReasonCode.RECEIPT_HASH_INVALID)

        if (
            receipt.observed_at.tzinfo is None
            or receipt.observed_at.utcoffset() is None
        ):
            reasons.add(ReasonCode.RECEIPT_TIME_INVALID)
        else:
            age = context.evaluated_at - receipt.observed_at
            if age < timedelta(0):
                reasons.add(ReasonCode.RECEIPT_TIME_INVALID)
            elif age > context.receipt_max_age:
                reasons.add(ReasonCode.RECEIPT_STALE)

    for criterion_id, criterion in criteria.items():
        evidence = by_criterion[criterion_id]
        if criterion.kind is CriterionKind.STATE:
            observations = [
                item
                for item in evidence
                if item.evidence_kind is EvidenceKind.OBSERVATION
            ]
            if not observations:
                reasons.add(ReasonCode.MISSING_EVIDENCE)
            if len(observations) > 1:
                reasons.add(ReasonCode.DUPLICATE_EVIDENCE)
            if len(observations) != len(evidence):
                reasons.add(ReasonCode.UNEXPECTED_EVIDENCE)
            if any(
                item.outcome is not EvidenceOutcome.VERIFIED for item in observations
            ):
                reasons.add(ReasonCode.EVIDENCE_NOT_VERIFIED)
            continue

        effects = [
            item for item in evidence if item.evidence_kind is EvidenceKind.EFFECT
        ]
        read_backs = [
            item for item in evidence if item.evidence_kind is EvidenceKind.READ_BACK
        ]
        if not effects:
            reasons.add(ReasonCode.MISSING_EVIDENCE)
        if not read_backs:
            reasons.add(ReasonCode.READ_BACK_MISSING)
        if len(effects) > 1 or len(read_backs) > 1:
            reasons.add(ReasonCode.DUPLICATE_EVIDENCE)
        if len(effects) + len(read_backs) != len(evidence):
            reasons.add(ReasonCode.UNEXPECTED_EVIDENCE)
        if any(item.outcome is EvidenceOutcome.UNKNOWN for item in effects):
            reasons.add(ReasonCode.EXTERNAL_EFFECT_UNKNOWN)
        if any(item.outcome is EvidenceOutcome.FAILED for item in effects):
            reasons.add(ReasonCode.EVIDENCE_NOT_VERIFIED)
        if any(item.outcome is not EvidenceOutcome.VERIFIED for item in read_backs):
            reasons.add(ReasonCode.EVIDENCE_NOT_VERIFIED)
        if len(effects) == 1 and len(read_backs) == 1:
            if read_backs[0].related_receipt_hash != effects[0].content_hash:
                reasons.add(ReasonCode.READ_BACK_MISMATCH)

    accepted = tuple(sorted(receipt_hashes)) if not reasons else ()
    return reasons, accepted


def evaluate_completion(
    context: ApprovedContext,
    envelope: CompletionEnvelope,
    receipts: Iterable[TrustedReceipt],
) -> CompletionDecision:
    """Evaluate exact bindings and return a fail-closed, deterministic decision."""

    if not _valid_context(context):
        return CompletionDecision(
            status=CompletionStatus.INCOMPLETE,
            reasons=(ReasonCode.TRUST_CONTEXT_INVALID,),
        )

    receipt_tuple = tuple(receipts)
    reasons = _binding_reasons(
        context,
        envelope.task_id,
        envelope.attempt_id,
        envelope.artifact_digest,
        envelope.environment,
        envelope.approved_request_digest,
    )
    reasons.update(_criteria_reasons(context, envelope))
    receipt_reasons, accepted_hashes = _receipt_reasons(context, receipt_tuple)
    reasons.update(receipt_reasons)

    ordered_reasons = tuple(reason for reason in ReasonCode if reason in reasons)
    if ordered_reasons:
        external_state_unknown = bool(
            reasons
            & {
                ReasonCode.EXTERNAL_EFFECT_UNKNOWN,
                ReasonCode.READ_BACK_MISSING,
            }
        )
        status = (
            CompletionStatus.UNKNOWN
            if external_state_unknown
            else CompletionStatus.INCOMPLETE
        )
        return CompletionDecision(status=status, reasons=ordered_reasons)
    return CompletionDecision(
        status=CompletionStatus.COMPLETE,
        reasons=(),
        accepted_receipt_hashes=accepted_hashes,
    )


def render_status(decision: CompletionDecision) -> str:
    """Render controlled text without incorporating envelope or receipt prose."""

    if decision.status is CompletionStatus.COMPLETE and not decision.reasons:
        return (
            "COMPLETE: Approved criteria have current evidence from trusted collectors."
        )
    if decision.status is CompletionStatus.UNKNOWN:
        return (
            "STATUS: Completion is not verified; an external effect has an unknown "
            "outcome or lacks a matching independent read-back."
        )
    return (
        "STATUS: Completion is not verified; required trusted evidence is incomplete."
    )

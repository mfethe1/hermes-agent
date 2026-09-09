"""Adversarial behavior contracts for the completion-eligibility core."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timedelta, timezone
from typing import Callable

import pytest

from response_verification import (
    ApprovedContext,
    CompletionEnvelope,
    CompletionStatus,
    CriterionKind,
    CriterionReference,
    EvidenceKind,
    EvidenceOrigin,
    EvidenceOutcome,
    ReasonCode,
    TrustedReceipt,
    approved_request_digest,
    criterion_digest,
    evaluate_completion,
    render_status,
    with_receipt_hash,
)

Bundle = tuple[ApprovedContext, CompletionEnvelope, tuple[TrustedReceipt, ...]]
Scenario = Callable[[Bundle], Bundle]

_NOW = datetime(2026, 9, 9, 12, tzinfo=timezone.utc)
_ARTIFACT = criterion_digest("artifact", CriterionKind.STATE, "release artifact bytes")


def _valid_bundle() -> Bundle:
    state = CriterionReference(
        criterion_id="tests-pass",
        criterion_digest=criterion_digest(
            "tests-pass", CriterionKind.STATE, "the isolated suite passes"
        ),
        kind=CriterionKind.STATE,
    )
    effect = CriterionReference(
        criterion_id="artifact-stored",
        criterion_digest=criterion_digest(
            "artifact-stored",
            CriterionKind.EXTERNAL_EFFECT,
            "the exact artifact is stored and readable",
        ),
        kind=CriterionKind.EXTERNAL_EFFECT,
    )
    criteria = (state, effect)
    approval_digest = approved_request_digest(criteria)
    context = ApprovedContext(
        task_id="task-123",
        attempt_id="attempt-1",
        artifact_digest=_ARTIFACT,
        environment="test-mack",
        approval_id="approval-7",
        approved_request_digest=approval_digest,
        criteria=criteria,
        trusted_issuers=frozenset({"collector-a"}),
        agent_identity="agent-mack",
        evaluated_at=_NOW,
        receipt_max_age=timedelta(minutes=10),
    )
    envelope = CompletionEnvelope(
        task_id=context.task_id,
        attempt_id=context.attempt_id,
        artifact_digest=context.artifact_digest,
        environment=context.environment,
        approved_request_digest=context.approved_request_digest,
        criteria=context.criteria,
    )
    observation = _receipt(
        context,
        state,
        receipt_id="receipt-observation",
        evidence_kind=EvidenceKind.OBSERVATION,
    )
    effect_receipt = _receipt(
        context,
        effect,
        receipt_id="receipt-effect",
        evidence_kind=EvidenceKind.EFFECT,
    )
    read_back = _receipt(
        context,
        effect,
        receipt_id="receipt-read-back",
        evidence_kind=EvidenceKind.READ_BACK,
        related_receipt_hash=effect_receipt.content_hash,
    )
    return context, envelope, (observation, effect_receipt, read_back)


def _receipt(
    context: ApprovedContext,
    criterion: CriterionReference,
    *,
    receipt_id: str,
    evidence_kind: EvidenceKind,
    outcome: EvidenceOutcome = EvidenceOutcome.VERIFIED,
    related_receipt_hash: str | None = None,
) -> TrustedReceipt:
    return with_receipt_hash(
        TrustedReceipt(
            receipt_id=receipt_id,
            issuer="collector-a",
            origin=EvidenceOrigin.INDEPENDENT_COLLECTOR,
            observed_at=_NOW - timedelta(seconds=30),
            task_id=context.task_id,
            attempt_id=context.attempt_id,
            artifact_digest=context.artifact_digest,
            environment=context.environment,
            approved_request_digest=context.approved_request_digest,
            criterion_id=criterion.criterion_id,
            criterion_digest=criterion.criterion_digest,
            evidence_kind=evidence_kind,
            outcome=outcome,
            related_receipt_hash=related_receipt_hash,
        )
    )


def _replace_receipt(bundle: Bundle, index: int, **changes: object) -> Bundle:
    context, envelope, receipts = bundle
    changed = with_receipt_hash(replace(receipts[index], **changes, content_hash=""))
    return context, envelope, receipts[:index] + (changed,) + receipts[index + 1 :]


def _empty_criteria(bundle: Bundle) -> Bundle:
    context, envelope, receipts = bundle
    return context, replace(envelope, criteria=()), receipts


def _empty_approved_criteria(bundle: Bundle) -> Bundle:
    context, envelope, receipts = bundle
    empty_context = replace(
        context,
        criteria=(),
        approved_request_digest=approved_request_digest(()),
    )
    return empty_context, envelope, receipts


def _missing_criteria(bundle: Bundle) -> Bundle:
    context, envelope, receipts = bundle
    return context, replace(envelope, criteria=None), receipts


def _duplicate_criteria(bundle: Bundle) -> Bundle:
    context, envelope, receipts = bundle
    return (
        context,
        replace(envelope, criteria=context.criteria + context.criteria[:1]),
        receipts,
    )


def _extra_criteria(bundle: Bundle) -> Bundle:
    context, envelope, receipts = bundle
    extra = CriterionReference(
        criterion_id="model-added",
        criterion_digest=criterion_digest(
            "model-added", CriterionKind.STATE, "unapproved model criterion"
        ),
        kind=CriterionKind.STATE,
    )
    return context, replace(envelope, criteria=context.criteria + (extra,)), receipts


def _changed_criteria(bundle: Bundle) -> Bundle:
    context, envelope, receipts = bundle
    changed = replace(
        context.criteria[0],
        criterion_digest=criterion_digest(
            "tests-pass", CriterionKind.STATE, "a changed requirement"
        ),
    )
    return context, replace(envelope, criteria=(changed, context.criteria[1])), receipts


def _duplicate_evidence(bundle: Bundle) -> Bundle:
    context, envelope, receipts = bundle
    return context, envelope, receipts + receipts[:1]


def _unknown_effect(bundle: Bundle) -> Bundle:
    context, envelope, receipts = _replace_receipt(
        bundle, 1, outcome=EvidenceOutcome.UNKNOWN
    )
    changed_read_back = with_receipt_hash(
        replace(
            receipts[2],
            related_receipt_hash=receipts[1].content_hash,
            content_hash="",
        )
    )
    return context, envelope, receipts[:2] + (changed_read_back,)


def _absent_read_back(bundle: Bundle) -> Bundle:
    context, envelope, receipts = bundle
    return context, envelope, receipts[:2]


def _invalid_hash(bundle: Bundle) -> Bundle:
    context, envelope, receipts = bundle
    changed = replace(receipts[0], content_hash=f"sha256:{'0' * 64}")
    return context, envelope, (changed,) + receipts[1:]


def _stale_receipt(bundle: Bundle) -> Bundle:
    return _replace_receipt(bundle, 0, observed_at=_NOW - timedelta(hours=1))


def _untrusted_issuer(bundle: Bundle) -> Bundle:
    return _replace_receipt(bundle, 0, issuer="collector-untrusted")


def _self_report(bundle: Bundle) -> Bundle:
    return _replace_receipt(
        bundle,
        0,
        issuer="agent-mack",
        origin=EvidenceOrigin.MODEL_SELF_REPORT,
    )


def _bad_read_back_link(bundle: Bundle) -> Bundle:
    return _replace_receipt(bundle, 2, related_receipt_hash=f"sha256:{'1' * 64}")


def _envelope_field(field: str, value: str) -> Scenario:
    def mutate(bundle: Bundle) -> Bundle:
        context, envelope, receipts = bundle
        return context, replace(envelope, **{field: value}), receipts

    return mutate


def _receipt_field(field: str, value: str) -> Scenario:
    def mutate(bundle: Bundle) -> Bundle:
        return _replace_receipt(bundle, 0, **{field: value})

    return mutate


_SCENARIOS: tuple[tuple[str, Scenario, ReasonCode], ...] = (
    ("empty criteria", _empty_criteria, ReasonCode.EMPTY_CRITERIA),
    (
        "empty approved criteria",
        _empty_approved_criteria,
        ReasonCode.TRUST_CONTEXT_INVALID,
    ),
    ("missing criteria", _missing_criteria, ReasonCode.MISSING_CRITERIA),
    ("duplicate criteria", _duplicate_criteria, ReasonCode.DUPLICATE_CRITERIA),
    ("extra criteria", _extra_criteria, ReasonCode.CRITERIA_MISMATCH),
    ("changed criteria", _changed_criteria, ReasonCode.CRITERIA_MISMATCH),
    (
        "envelope task mismatch",
        _envelope_field("task_id", "attacker-task"),
        ReasonCode.TASK_MISMATCH,
    ),
    (
        "envelope attempt mismatch",
        _envelope_field("attempt_id", "attempt-2"),
        ReasonCode.ATTEMPT_MISMATCH,
    ),
    (
        "envelope artifact mismatch",
        _envelope_field("artifact_digest", f"sha256:{'2' * 64}"),
        ReasonCode.ARTIFACT_MISMATCH,
    ),
    (
        "envelope environment mismatch",
        _envelope_field("environment", "prod-rosie"),
        ReasonCode.ENVIRONMENT_MISMATCH,
    ),
    (
        "receipt task mismatch",
        _receipt_field("task_id", "attacker-task"),
        ReasonCode.TASK_MISMATCH,
    ),
    (
        "receipt attempt mismatch",
        _receipt_field("attempt_id", "attempt-2"),
        ReasonCode.ATTEMPT_MISMATCH,
    ),
    (
        "receipt artifact mismatch",
        _receipt_field("artifact_digest", f"sha256:{'3' * 64}"),
        ReasonCode.ARTIFACT_MISMATCH,
    ),
    (
        "receipt environment mismatch",
        _receipt_field("environment", "prod-rosie"),
        ReasonCode.ENVIRONMENT_MISMATCH,
    ),
    ("duplicate evidence", _duplicate_evidence, ReasonCode.DUPLICATE_EVIDENCE),
    ("unknown external effect", _unknown_effect, ReasonCode.EXTERNAL_EFFECT_UNKNOWN),
    ("absent read-back", _absent_read_back, ReasonCode.READ_BACK_MISSING),
    ("changed receipt after hashing", _invalid_hash, ReasonCode.RECEIPT_HASH_INVALID),
    ("stale receipt", _stale_receipt, ReasonCode.RECEIPT_STALE),
    ("untrusted issuer", _untrusted_issuer, ReasonCode.RECEIPT_ISSUER_UNTRUSTED),
    ("model self-report", _self_report, ReasonCode.SELF_REPORTED_EVIDENCE),
    ("wrong read-back link", _bad_read_back_link, ReasonCode.READ_BACK_MISMATCH),
)


def test_exact_approved_bindings_and_current_receipts_allow_controlled_completion() -> (
    None
):
    context, envelope, receipts = _valid_bundle()

    decision = evaluate_completion(context, envelope, receipts)

    assert decision.status is CompletionStatus.COMPLETE
    assert decision.eligible
    assert decision.reasons == ()
    assert decision.accepted_receipt_hashes == tuple(
        sorted(receipt.content_hash for receipt in receipts)
    )
    assert render_status(decision) == (
        "COMPLETE: Approved criteria have current evidence from trusted collectors."
    )


@pytest.mark.parametrize(
    ("_name", "mutate", "expected_reason"),
    _SCENARIOS,
    ids=[item[0] for item in _SCENARIOS],
)
def test_adversarial_input_denies_completion_and_releases_only_fixed_status(
    _name: str,
    mutate: Scenario,
    expected_reason: ReasonCode,
) -> None:
    context, envelope, receipts = mutate(_valid_bundle())

    decision = evaluate_completion(context, envelope, receipts)
    rendered = render_status(decision)

    assert not decision.eligible
    assert expected_reason in decision.reasons
    assert decision.accepted_receipt_hashes == ()
    assert rendered in {
        "STATUS: Completion is not verified; required trusted evidence is incomplete.",
        (
            "STATUS: Completion is not verified; an external effect has an unknown "
            "outcome or lacks a matching independent read-back."
        ),
    }
    assert "attacker-task" not in rendered
    assert "prod-rosie" not in rendered

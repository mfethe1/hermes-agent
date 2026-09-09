"""Bounded local snapshots, not authenticated authority or current-path evidence."""

import base64
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timedelta, timezone
import hashlib
import math
import os
import stat
import time

from response_verification.core import (
    ApprovedContext,
    CriterionKind,
    CriterionReference,
    _valid_context,
)

MAX_SNAPSHOT_BYTES = 4096
MAX_ADMISSION_AGE = timedelta(seconds=30)
SNAPSHOT_CLAIM = "admitted-snapshot"


def freeze_context(context):
    if type(context) is not ApprovedContext:
        raise ValueError("approval type")
    if type(context.criteria) not in (tuple, list) or len(context.criteria) != 1:
        raise ValueError("one snapshot criterion")
    if type(context.criteria[0]) is not CriterionReference:
        raise ValueError("criterion type")
    if type(context.receipt_max_age) is not timedelta or not (
        timedelta(0) < context.receipt_max_age <= timedelta(days=1)
    ):
        raise ValueError("receipt lifetime")
    if type(context.trusted_issuers) not in (set, frozenset) or not (
        1 <= len(context.trusted_issuers) <= 24
    ):
        raise ValueError("issuer bounds")
    context = replace(
        context,
        criteria=tuple(context.criteria),
        trusted_issuers=frozenset(context.trusted_issuers),
    )
    if (
        not _valid_context(context)
        or context.criteria[0].kind is not CriterionKind.STATE
    ):
        raise ValueError("invalid approval")
    if any(type(x) is not str or len(x) > 128 for x in context.trusted_issuers):
        raise ValueError("issuer bounds")
    return context


def snapshot_digest(data):
    return "sha256:" + hashlib.sha256(data).hexdigest()


@dataclass(frozen=True)
class SnapshotAdmission:
    data: bytes
    context: ApprovedContext
    admitted_at: datetime
    expires_at: datetime
    monotonic_deadline: float
    claim: str = SNAPSHOT_CLAIM

    def __post_init__(self):
        if type(self.data) is not bytes or len(self.data) > MAX_SNAPSHOT_BYTES:
            raise ValueError("snapshot bytes/size")
        object.__setattr__(self, "context", freeze_context(self.context))

    @property
    def digest(self):
        return snapshot_digest(self.data)

    def validate(self, context):
        now = datetime.now(timezone.utc)
        if (
            type(self.data) is not bytes
            or len(self.data) > MAX_SNAPSHOT_BYTES
            or type(self.monotonic_deadline) not in (int, float)
            or not math.isfinite(self.monotonic_deadline)
            or self.claim != SNAPSHOT_CLAIM
            or freeze_context(context) != self.context
            or self.digest != self.context.artifact_digest
            or not self.admitted_at <= now < self.expires_at
            or not timedelta(0)
            < self.expires_at - self.admitted_at
            <= MAX_ADMISSION_AGE
            or time.monotonic() >= self.monotonic_deadline
        ):
            raise ValueError("invalid or stale snapshot admission")

    def wire(self):
        return {
            "claim": self.claim,
            "data_base64": base64.b64encode(self.data).decode("ascii"),
            "digest": self.digest,
            "admitted_at": self.admitted_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
        }


def admit_snapshot(context, path, *, claim, max_age=MAX_ADMISSION_AGE):
    """Caller explicitly approves snapshot semantics outside the model envelope.

    Binary contents are allowed; only the private transport uses canonical base64.
    No path identity or continuing file-state promise survives admission.
    """
    context = freeze_context(context)
    if (
        claim != SNAPSHOT_CLAIM
        or type(max_age) is not timedelta
        or not (timedelta(0) < max_age <= MAX_ADMISSION_AGE)
    ):
        raise ValueError("snapshot claim/lifetime")
    # Nonblocking open avoids hanging on a FIFO; regular files only, no final symlink.
    fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as source:
        info = os.fstat(source.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_size > MAX_SNAPSHOT_BYTES:
            raise ValueError("snapshot file/size")
        data = source.read(MAX_SNAPSHOT_BYTES + 1)
    if (
        len(data) > MAX_SNAPSHOT_BYTES
        or snapshot_digest(data) != context.artifact_digest
    ):
        raise ValueError("snapshot size/digest")
    now = datetime.now(timezone.utc)
    return SnapshotAdmission(
        data, context, now, now + max_age, time.monotonic() + max_age.total_seconds()
    )


def context_wire(context):
    value = asdict(context)
    value["evaluated_at"] = context.evaluated_at.isoformat()
    value["receipt_max_age"] = context.receipt_max_age.total_seconds()
    value["trusted_issuers"] = sorted(context.trusted_issuers)
    return value

"""Durable dispatch: cross-process, restart-safe replay protection.

Implements RESPONSE-VERIFICATION-001 replay-acceptance contract:
- journal intent BEFORE transport acceptance; fsync each append
- cross-process mutual exclusion via flock on a lockfile
- duplicates (same task/attempt/destination/content) blocked across
  processes and restarts
- any ambiguous outcome (exception, crash, no positive ack) is held
  UNKNOWN -- never auto-retried, never reported success
- storage failures fail closed: no send without a durable intent,
  no success claim without a durable outcome
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import time
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional


class DispatchStatus(str, Enum):
    DELIVERED = "delivered"
    DUPLICATE_BLOCKED = "duplicate_blocked"
    UNKNOWN_HOLD = "unknown_hold"
    STORAGE_REFUSED = "storage_refused"
    EXPIRED_EVIDENCE_HOLD = "expired_evidence_hold"


@dataclass(frozen=True)
class DispatchRecord:
    dispatch_id: str
    task_id: str
    attempt: str
    destination: str
    content_hash: str
    phase: str  # "intent" | "outcome"
    outcome: Optional[str]  # "delivered" | "unknown" | None
    owner: str
    ts: float


def content_key(task_id: str, attempt: str, destination: str, payload: bytes) -> str:
    h = hashlib.sha256()
    h.update(task_id.encode())
    h.update(b"\x00")
    h.update(attempt.encode())
    h.update(b"\x00")
    h.update(destination.encode())
    h.update(b"\x00")
    h.update(payload)
    return h.hexdigest()


def logical_key(task_id: str, destination: str, payload: bytes) -> str:
    """Stable logical delivery identity, independent of attempt identity.

    Two dispatches that differ only in `attempt` share this key, so a
    relabeled attempt cannot silently resend the same logical response.
    """
    h = hashlib.sha256()
    h.update(task_id.encode())
    h.update(b"\x00")
    h.update(destination.encode())
    h.update(b"\x00")
    h.update(payload)
    return h.hexdigest()


class _StorageFailure(Exception):
    pass


class DurableDispatchLedger:
    """Append-only JSONL journal with flock-based cross-process locking."""

    def __init__(self, journal_dir: str):
        self.journal_dir = os.path.abspath(journal_dir)
        self.journal_path = os.path.join(self.journal_dir, "dispatch-journal.jsonl")
        self.lock_path = os.path.join(self.journal_dir, "dispatch.lock")
        os.makedirs(self.journal_dir, exist_ok=True)

    def _fsync_dir(self) -> None:
        fd = os.open(self.journal_dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _append(self, record: dict) -> None:
        # durable append: write, flush, fsync; crash leaves whole lines only
        line = json.dumps(record, sort_keys=True) + "\n"
        fd = os.open(self.journal_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, line.encode())
            os.fsync(fd)
        finally:
            os.close(fd)
        self._fsync_dir()

    def _read_all(self) -> list:
        records = []
        if not os.path.exists(self.journal_path):
            return records
        with open(self.journal_path, "rb") as fh:
            for raw in fh:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    text = raw.decode("utf-8")
                except UnicodeDecodeError:
                    # binary garbage: same class as a torn write
                    records.append({"phase": "torn", "outcome": "unknown"})
                    continue
                try:
                    records.append(json.loads(text))
                except json.JSONDecodeError:
                    # torn tail line from a crash mid-write: treat as unknown intent
                    records.append({"phase": "torn", "outcome": "unknown"})
        return records

    def lookup(self, key: str) -> Optional[dict]:
        """Return the most authoritative record for a content key.

        Authority order: outcome-delivered > outcome-unknown > intent > torn.
        """
        best: Optional[dict] = None
        rank = {"torn": 0, "intent": 1, "outcome-unknown": 2, "outcome-delivered": 3}
        for rec in self._read_all():
            if rec.get("content_key") != key and rec.get("logical_key") != key:
                continue
            phase = rec.get("phase", "torn")
            if phase == "outcome":
                r = f"outcome-{rec.get('outcome')}"
            else:
                r = phase
            if best is None or rank.get(r, 0) >= rank.get(best[0], 0):
                best = (r, rec)
        return best[1] if best else None

    def has_torn(self) -> bool:
        """True if the journal contains any unparseable/torn/binary line.

        A torn line cannot be attributed to any content key, so its mere
        presence makes the whole store ambiguous and must block dispatch.
        """
        return any(rec.get("phase") == "torn" and "content_key" not in rec
                   for rec in self._read_all())

    def acquire(self):
        fh = open(self.lock_path, "a+", encoding="utf-8")
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        return fh
    def release(self, fh) -> None:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()


class DurableDispatcher:
    """Fail-closed dispatcher: no transport call without a durable intent."""

    def __init__(self, journal_dir: str, owner: str, evidence_ttl: float = 3600.0):
        self.ledger = DurableDispatchLedger(journal_dir)
        self.owner = owner
        self.evidence_ttl = evidence_ttl

    def dispatch(
        self,
        dispatch_id: str,
        task_id: str,
        attempt: str,
        destination: str,
        payload: bytes,
        transport: Callable[[bytes], object],
    ) -> DispatchStatus:
        key = content_key(task_id, attempt, destination, payload)
        lkey = logical_key(task_id, destination, payload)
        lock = self.ledger.acquire()
        try:
            return self._dispatch_locked(
                dispatch_id, key, lkey, task_id, attempt, destination,
                payload, transport,
            )
        finally:
            self.ledger.release(lock)

    def _dispatch_locked(
        self, dispatch_id, key, lkey, task_id, attempt, destination,
        payload, transport,
    ) -> DispatchStatus:
        # Corrupt/torn store anywhere fails closed for ALL dispatches:
        # a torn line cannot be attributed, so its presence is ambiguous.
        try:
            if self.ledger.has_torn():
                return DispatchStatus.STORAGE_REFUSED
        except OSError:
            return DispatchStatus.STORAGE_REFUSED  # unreadable journal: no send
        try:
            prior = self.ledger.lookup(key)
        except OSError:
            return DispatchStatus.STORAGE_REFUSED  # unreadable journal: no send
        if prior is not None:
            phase = prior.get("phase", "torn")
            if phase == "outcome" and prior.get("outcome") == "delivered":
                return DispatchStatus.DUPLICATE_BLOCKED
            # evidence expiry denies dispatch even when a reservation exists
            if time.time() - prior.get("ts", 0) > self.evidence_ttl:
                return DispatchStatus.EXPIRED_EVIDENCE_HOLD
            # unknown / intent / torn: ambiguous prior state, hold fail-closed
            if prior.get("owner") != self.owner:
                # stale/foreign owner: never overwrite someone else's ambiguity
                return DispatchStatus.UNKNOWN_HOLD
            if phase == "outcome" and prior.get("outcome") == "unknown":
                return DispatchStatus.UNKNOWN_HOLD
            # our own intent without outcome: crash mid-dispatch; still hold
            return DispatchStatus.UNKNOWN_HOLD

        # F2: logical dedupe independent of attempt identity — a different
        # attempt label must not silently resend the same logical response.
        try:
            logical_prior = self.ledger.lookup(lkey)
        except OSError:
            return DispatchStatus.STORAGE_REFUSED
        if logical_prior is not None:
            # F3: evidence expiry — an expired reservation denies dispatch
            # as a distinct condition, never silently resends.
            intent_ts = logical_prior.get("ts", 0)
            if time.time() - intent_ts > self.evidence_ttl:
                return DispatchStatus.EXPIRED_EVIDENCE_HOLD
            lphase = logical_prior.get("phase", "torn")
            if lphase == "outcome" and logical_prior.get("outcome") == "delivered":
                return DispatchStatus.DUPLICATE_BLOCKED
            return DispatchStatus.UNKNOWN_HOLD

        # 1) durable intent BEFORE any transport effect
        intent = {
            "dispatch_id": dispatch_id,
            "content_key": key,
            "logical_key": lkey,
            "task_id": task_id,
            "attempt": attempt,
            "destination": destination,
            "phase": "intent",
            "outcome": None,
            "owner": self.owner,
            "ts": time.time(),
        }
        try:
            self.ledger._append(intent)
        except OSError:
            return DispatchStatus.STORAGE_REFUSED  # no send without durable intent

        # 2) transport; anything but an explicit positive ack is unknown
        ack = None
        transport_error: Optional[BaseException] = None
        try:
            ack = transport(payload)
        except BaseException as exc:  # noqa: BLE001 - ambiguity must be recorded
            transport_error = exc
        if ack is not True or transport_error is not None:
            try:
                self.ledger._append(
                    {
                        "dispatch_id": dispatch_id,
                        "content_key": key,
                        "logical_key": lkey,
                        "phase": "outcome",
                        "outcome": "unknown",
                        "owner": self.owner,
                        "ts": time.time(),
                        "error": repr(transport_error)[:200],
                    }
                )
            except OSError:
                pass  # intent already durable; recovery will hold unknown
            return DispatchStatus.UNKNOWN_HOLD

        # 3) durable delivered outcome
        try:
            self.ledger._append(
                {
                    "dispatch_id": dispatch_id,
                    "content_key": key,
                    "logical_key": lkey,
                    "phase": "outcome",
                    "outcome": "delivered",
                    "owner": self.owner,
                    "ts": time.time(),
                }
            )
        except OSError:
            # send happened but outcome not durable: NEVER claim success;
            # treat as unknown for us and for every later reader
            try:
                self.ledger._append(
                    {
                        "dispatch_id": dispatch_id,
                        "content_key": key,
                        "logical_key": lkey,
                        "phase": "outcome",
                        "outcome": "unknown",
                        "owner": self.owner,
                        "ts": time.time(),
                        "error": "outcome-storage-failure",
                    }
                )
            except OSError:
                pass
            return DispatchStatus.UNKNOWN_HOLD
        return DispatchStatus.DELIVERED

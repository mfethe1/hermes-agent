"""Durable dispatch identity store: atomic reservation, fencing, binding.

Implements the RESPONSE-VERIFICATION-001 replay-acceptance next slice on top
of the existing fail-closed journal (pilot_dispatch):

- explicit reservation states: reserved, dispatching, observed-success,
  observed-failure, unknown, cancelled (any other state string is an
  incompatible schema and fails closed)
- authorization binding: task/approval/destination/digest/claim/rendering
  form the logical delivery identity; a changed binding is a conflict,
  not fresh permission
- atomic reservation across real OS processes: store append + fsync while
  holding the cross-process flock; at most one reservation for one logical
  delivery can ever be committed
- fencing: each reservation carries a monotonically increasing token
  (per store); after lease transfer a stale owner's writes are rejected
- ambiguity is durable unknown: restart after intent never auto-resends;
  an expired lease alone does not authorize retry after a possible send
- transport success and authenticated read-back are separate fields;
  native delivery ID is captured when the transport provides one
- corrupt/truncated/locked/unreadable store or unknown schema version
  fails closed: no send, no fabricated receipts

Same-UID is not an authenticated identity boundary; owner names are
coordination labels, not security principals (see admission findings).
"""

from __future__ import annotations

import fcntl
import hashlib
import json
import os
import time
from dataclasses import dataclass, fields as dc_fields
from typing import Callable, Optional

SCHEMA_VERSION = 1
STORE_FILENAME = "dispatch-store.jsonl"
LOCK_FILENAME = "dispatch.lock"

# Explicit reservation states (contract: "Require explicit states")
RESERVED = "reserved"
DISPATCHING = "dispatching"
OBSERVED_SUCCESS = "observed-success"
OBSERVED_FAILURE = "observed-failure"
UNKNOWN = "unknown"
CANCELLED = "cancelled"
VALID_STATES = frozenset(
    {RESERVED, DISPATCHING, OBSERVED_SUCCESS, OBSERVED_FAILURE, UNKNOWN, CANCELLED}
)

# State machine: allowed next states per current state.
_TRANSITIONS = {
    RESERVED: {DISPATCHING, OBSERVED_SUCCESS, OBSERVED_FAILURE, UNKNOWN, CANCELLED},
    DISPATCHING: {OBSERVED_SUCCESS, OBSERVED_FAILURE, UNKNOWN, CANCELLED},
    OBSERVED_SUCCESS: set(),  # terminal
    OBSERVED_FAILURE: set(),  # terminal here; retry requires NEW logical key
    UNKNOWN: set(),           # terminal: held forever, manual reconciliation
    CANCELLED: set(),         # terminal here; retry requires NEW logical key
}

# Result statuses (distinct so callers/tests require exact outcomes)
RESERVED_OK = "reserved"
BINDING_CONFLICT = "binding_conflict"
DUPLICATE_DELIVERY = "duplicate_delivery"
STALE_OWNER = "stale_owner"
LEASE_EXPIRED = "lease_expired"
STORAGE_REFUSED = "storage_refused"
SCHEMA_REFUSED = "schema_refused"
SEND_UNKNOWN = "send_unknown"
SEND_SUCCESS = "send_success"
SEND_FAILURE = "send_failure"
CANCEL_OK = "cancel_ok"
CANCEL_REFUSED = "cancel_refused"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def binding_key(
    task_id: str,
    approved_request_digest: str,
    destination: str,
    artifact_digest: str,
    claim_class: str,
    rendering: str,
) -> str:
    """Logical delivery identity from the full authorization binding.

    Contract: "Bind authorization to exact approved task/request, destination
    platform/account/chat/thread, snapshot digest, claim class and rendering
    version; changed bindings are conflicts, not fresh permission."
    `destination` is the caller-joined opaque destination identity string
    (platform/account/chat/thread).
    """
    h = hashlib.sha256()
    for part in (
        task_id,
        approved_request_digest,
        destination,
        artifact_digest,
        claim_class,
        rendering,
    ):
        if type(part) is not str:
            raise ValueError("binding part type")
        h.update(part.encode())
        h.update(b"\x00")
    return h.hexdigest()


def payload_hash(payload: bytes) -> str:
    if type(payload) is not bytes:
        raise ValueError("payload type")
    return _sha256(payload)


class StoreResult:
    """Outcome of a store operation; exact statuses, never exceptions up."""

    __slots__ = ("status", "token", "delivery_id", "record")

    def __init__(self, status, token=None, delivery_id=None, record=None):
        self.status = status
        self.token = token
        # native delivery ID from transport/read-back, when available
        self.delivery_id = delivery_id
        self.record = record

    def __repr__(self):  # pragma: no cover - debug aid
        return (
            f"StoreResult(status={self.status!r}, token={self.token!r}, "
            f"delivery_id={self.delivery_id!r})"
        )


@dataclass(frozen=True)
class Authorization:
    """Caller-asserted approval binding. Not an authenticated principal."""

    task_id: str
    approved_request_digest: str
    destination: str
    artifact_digest: str
    claim_class: str = "verified-completion"
    rendering: str = "v1"
    lease_seconds: float = 300.0

    def __post_init__(self):
        for name in (
            "task_id",
            "approved_request_digest",
            "destination",
            "artifact_digest",
            "claim_class",
            "rendering",
        ):
            value = getattr(self, name)
            if type(value) is not str or not value:
                raise ValueError(f"authorization field {name}")
        if (
            type(self.lease_seconds) not in (int, float)
            or not 0 < self.lease_seconds <= 86400
        ):
            raise ValueError("lease seconds")

    def key(self) -> str:
        return binding_key(
            self.task_id,
            self.approved_request_digest,
            self.destination,
            self.artifact_digest,
            self.claim_class,
            self.rendering,
        )


class _StoreFailure(Exception):
    """Internal: store unusable — every path catching it fails closed."""

    kind = "storage"


class _SchemaFailure(_StoreFailure):
    """Internal: incompatible store schema/state — fails closed distinctly."""

    kind = "schema"


class DispatchStore:
    """Append-only JSONL reservation store with cross-process flock.

    One line per state transition. Every append happens under the exclusive
    flock and is fsynced (file and directory) before the lock is released,
    so a committed line is durable before any process can observe or act on
    it. Torn/corrupt lines make the whole store refuse service (fail-closed)
    because an unattributable line is unattributable ambiguity.
    """

    def __init__(self, store_dir: str):
        self.store_dir = os.path.abspath(store_dir)
        self.store_path = os.path.join(self.store_dir, STORE_FILENAME)
        self.lock_path = os.path.join(self.store_dir, LOCK_FILENAME)
        os.makedirs(self.store_dir, exist_ok=True)

    # ---- low-level durable append ----

    def _fsync_dir(self) -> None:
        fd = os.open(self.store_dir, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _append(self, record: dict) -> None:
        line = json.dumps(record, sort_keys=True) + "\n"
        fd = os.open(self.store_path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, 0o600)
        try:
            os.write(fd, line.encode())
            os.fsync(fd)
        finally:
            os.close(fd)
        self._fsync_dir()

    # ---- reading with fail-closed parsing ----

    def _read_all(self) -> list:
        """Parse every line; ANY unparseable/torn/binary line => refusal.

        Unlike the earlier journal (which recorded torn lines as data), the
        reservation store refuses ALL service while a torn line exists: a
        reservation state that cannot be attributed might be ours.
        """
        if not os.path.exists(self.store_path):
            return []
        records = []
        with open(self.store_path, "rb") as fh:
            for raw in fh:
                raw = raw.strip()
                if not raw:
                    continue
                try:
                    text = raw.decode("utf-8")
                    rec = json.loads(text)
                except (UnicodeDecodeError, json.JSONDecodeError):
                    raise _StoreFailure("torn/corrupt line") from None
                if type(rec) is not dict:
                    raise _StoreFailure("non-object line")
                records.append(rec)
        return records

    def _require_schema(self, records: list) -> None:
        for rec in records:
            if rec.get("schema") != SCHEMA_VERSION:
                raise _SchemaFailure("incompatible schema")
            if rec.get("state") not in VALID_STATES:
                raise _SchemaFailure("unknown state")

    def _states_for(self, key: str) -> list:
        """All committed state records for a binding key, in order."""
        records = self._read_all()
        self._require_schema(records)
        return [r for r in records if r.get("binding_key") == key]

    def _next_token(self, records: list) -> int:
        token = 0
        for rec in records:
            t = rec.get("token")
            if type(t) is int and t > token:
                token = t
        return token + 1

    # ---- locking ----

    def acquire(self):
        fh = open(self.lock_path, "a+", encoding="utf-8")
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        return fh

    def release(self, fh) -> None:
        fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        fh.close()

    # ---- current state view ----

    def current(self, key: str) -> Optional[dict]:
        """Latest committed state record for a key, or None."""
        try:
            states = self._states_for(key)
        except _StoreFailure:
            return None
        return states[-1] if states else None


class ReservedDispatch:
    """One held reservation: authority to send exactly one logical delivery.

    Obtained via DispatchStore.reserve(). Carries the fencing token; every
    transition method re-validates ownership, lease and state machine under
    the store lock, so a stale instance can never commit a transition.
    """

    def __init__(self, store: DispatchStore, record: dict):
        self._store = store
        self._record = record
        self.token = record["token"]
        self.binding_key = record["binding_key"]
        self.owner = record["owner"]

    # ---- internal: commit one transition if still valid ----

    def _commit(
        self,
        new_state: str,
        *,
        owner: str,
        extra: Optional[dict] = None,
        allow_expired: bool = False,
    ) -> str:
        lock = self._store.acquire()
        try:
            try:
                states = self._store._states_for(self.binding_key)
            except _StoreFailure:
                return STORAGE_REFUSED
            if not states:
                return STORAGE_REFUSED
            latest = states[-1]
            if latest["token"] != self.token:
                return STALE_OWNER  # fenced: a newer reservation exists
            if latest["owner"] != owner:
                return STALE_OWNER  # lease transferred; old owner fenced
            if latest["state"] not in _TRANSITIONS:
                return SCHEMA_REFUSED
            if new_state not in _TRANSITIONS[latest["state"]]:
                return CANCEL_REFUSED if new_state == CANCELLED else SEND_FAILURE
            lease_expired = (
                time.time() - latest["ts"] > latest.get("lease_seconds", 0)
            )
            if lease_expired and not allow_expired:
                return LEASE_EXPIRED
            record = {
                "schema": SCHEMA_VERSION,
                "binding_key": self.binding_key,
                "token": self.token,
                "owner": owner,
                "state": new_state,
                "ts": time.time(),
                # carry the lease forward so it spans every transition
                "lease_seconds": latest.get("lease_seconds", 0),
            }
            if extra:
                record.update(extra)
            try:
                self._store._append(record)
            except OSError:
                return STORAGE_REFUSED
            self._record = record
            return (
                CANCEL_OK
                if new_state == CANCELLED
                else SEND_SUCCESS
                if new_state == OBSERVED_SUCCESS
                else SEND_FAILURE
                if new_state == OBSERVED_FAILURE
                else new_state
            )
        finally:
            self._store.release(lock)

    # ---- public transitions ----

    def mark_dispatching(self, owner: str) -> str:
        return self._commit(DISPATCHING, owner=owner)

    def cancel(self, owner: str) -> str:
        return self._commit(CANCELLED, owner=owner)

    def mark_observed_failure(self, owner: str, reason: str = "") -> str:
        return self._commit(
            OBSERVED_FAILURE, owner=owner, extra={"reason": reason[:200]}
        )

    def send(
        self,
        owner: str,
        payload: bytes,
        transport: Callable[[bytes], object],
    ) -> StoreResult:
        """Fail-closed send: dispatching -> transport -> durable outcome.

        Transport contract: return True (or a string native delivery id) on
        success, False/None on failure, raise on exception. Any outcome other
        than an explicit positive result is committed as UNKNOWN and held.
        """
        # 1) durable "dispatching" intent BEFORE transport, lease-checked
        status = self._commit(DISPATCHING, owner=owner)
        if status != DISPATCHING:
            return StoreResult(status)

        # 2) transport
        # Transport contract: True = positive ack; non-empty str = positive
        # ack WITH a native delivery id; False = transport asserts the
        # delivery did NOT happen (observed-failure); None or an exception
        # = ambiguous (unknown, held forever, manual reconciliation).
        delivery_id: Optional[str] = None
        error: Optional[BaseException] = None
        try:
            ack = transport(payload)
        except BaseException as exc:  # noqa: BLE001 - ambiguity must be recorded
            ack, error = None, exc
        if ack is True:
            pass
        elif type(ack) is str and ack:
            delivery_id = ack
        elif ack is False:
            ack = "explicit-failure"
        else:
            ack = None

        # 3) durable outcome BEFORE reporting anything
        extra = {"payload_hash": payload_hash(payload)}
        if delivery_id:
            extra["native_delivery_id"] = delivery_id
        if error is not None:
            extra["reason"] = repr(error)[:200]

        # lease may expire during transport; outcome commit must still land
        if ack is None:
            status = self._commit_unknown(owner, extra)
        elif ack == "explicit-failure":
            status = self._commit(
                OBSERVED_FAILURE, owner=owner, extra=extra, allow_expired=True
            )
        else:
            status = self._commit(
                OBSERVED_SUCCESS, owner=owner, extra=extra, allow_expired=True
            )
        return StoreResult(
            status, token=self.token, delivery_id=delivery_id, record=self._record
        )

    def _commit_unknown(self, owner: str, extra: dict) -> str:
        # UNKNOWN from DISPATCHING is a legal transition; _commit maps
        # non-terminal non-cancel states to themselves.
        lock = self._store.acquire()
        try:
            try:
                states = self._store._states_for(self.binding_key)
            except _StoreFailure:
                return STORAGE_REFUSED
            if not states:
                return STORAGE_REFUSED
            latest = states[-1]
            if latest["token"] != self.token or latest["owner"] != owner:
                return STALE_OWNER
            if UNKNOWN not in _TRANSITIONS.get(latest["state"], set()):
                return SEND_FAILURE
            record = {
                "schema": SCHEMA_VERSION,
                "binding_key": self.binding_key,
                "token": self.token,
                "owner": owner,
                "state": UNKNOWN,
                "ts": time.time(),
            }
            record.update(extra)
            try:
                self._store._append(record)
            except OSError:
                return STORAGE_REFUSED
            self._record = record
            return SEND_UNKNOWN
        finally:
            self._store.release(lock)


class DispatchIdentityStore:
    """Facade: reserve / inspect / drive one logical delivery."""

    def __init__(self, store_dir: str):
        self.store = DispatchStore(store_dir)

    def status(self, authorization: Authorization) -> Optional[dict]:
        """Latest committed state for a binding (None if unseen).

        Raises nothing; a corrupt store surfaces as STORAGE_REFUSED in the
        reservation path and as None here (callers must treat a needed-but-
        unreadable store as refusal at mutation time, which enforces it).
        """
        return self.store.current(authorization.key())

    def reserve(
        self, authorization: Authorization, owner: str
    ) -> StoreResult:
        """Atomically reserve one logical delivery under the flock.

        Outcomes:
        - RESERVED_OK: exclusive authority granted (fencing token issued)
        - DUPLICATE_DELIVERY: this logical delivery already observed-success
        - BINDING_CONFLICT: same key exists in a non-success state under a
          different authorization identity hash or unknown/crashed state —
          a changed binding never silently creates fresh permission
        - LEASE_EXPIRED: prior reservation's evidence lease expired — deny,
          never auto-retry after a possible send
        - STALE_OWNER: reservation exists owned by another live lease
        - STORAGE_REFUSED / SCHEMA_REFUSED: store unusable — fail closed
        """
        key = authorization.key()
        lock = self.store.acquire()
        try:
            try:
                records = self.store._read_all()
                self.store._require_schema(records)
            except _SchemaFailure:
                return StoreResult(SCHEMA_REFUSED)
            except (_StoreFailure, OSError):
                return StoreResult(STORAGE_REFUSED)
            states = [r for r in records if r.get("binding_key") == key]
            if states:
                latest = states[-1]
                state = latest["state"]
                if state == OBSERVED_SUCCESS:
                    return StoreResult(
                        DUPLICATE_DELIVERY, token=latest.get("token"), record=latest
                    )
                # Any other existing state is a conflict: unknown stays held,
                # reserved/dispatching belong to someone (maybe crashed),
                # failure/cancelled require a NEW authorization identity,
                # which by construction produces a different binding key or
                # an explicit operator reconciliation outside this API.
                if state == UNKNOWN:
                    return StoreResult(SEND_UNKNOWN, record=latest)
                if time.time() - latest["ts"] > latest.get("lease_seconds", 0):
                    return StoreResult(LEASE_EXPIRED, record=latest)
                if latest.get("owner") == owner and state in (RESERVED,):
                    # idempotent re-reserve by the same live owner
                    return StoreResult(
                        RESERVED_OK, token=latest["token"], record=latest
                    )
                return StoreResult(BINDING_CONFLICT, record=latest)
            token = self.store._next_token(records)
            record = {
                "schema": SCHEMA_VERSION,
                "binding_key": key,
                "token": token,
                "owner": owner,
                "state": RESERVED,
                "ts": time.time(),
                "lease_seconds": authorization.lease_seconds,
                "task_id": authorization.task_id,
                "destination": authorization.destination,
                "artifact_digest": authorization.artifact_digest,
                "claim_class": authorization.claim_class,
                "rendering": authorization.rendering,
                "approved_request_digest": authorization.approved_request_digest,
                "payload_hash": None,
            }
            try:
                self.store._append(record)
            except OSError:
                return StoreResult(STORAGE_REFUSED)
            return StoreResult(
                RESERVED_OK,
                token=token,
                record=record,
            )
        finally:
            self.store.release(lock)

    def adopt(
        self, authorization: Authorization, owner: str
    ) -> StoreResult:
        """Take over an EXPIRED lease (fencing: old owner cannot then act).

        Contract: "stale owners cannot send after lease transfer (fencing)".
        Allowed only from RESERVED (never after DISPATCHING — that may have
        sent; it must go to UNKNOWN via reconciliation, not re-send).
        Pre-send reservation recovery must prove no dispatch intent was
        committed: a DISPATCHING/unknown record can never be adopted.
        """
        key = authorization.key()
        lock = self.store.acquire()
        try:
            try:
                records = self.store._read_all()
                self.store._require_schema(records)
            except _SchemaFailure:
                return StoreResult(SCHEMA_REFUSED)
            except (_StoreFailure, OSError):
                return StoreResult(STORAGE_REFUSED)
            states = [r for r in records if r.get("binding_key") == key]
            if not states:
                return StoreResult(BINDING_CONFLICT)
            latest = states[-1]
            state = latest["state"]
            if state == OBSERVED_SUCCESS:
                return StoreResult(DUPLICATE_DELIVERY, record=latest)
            if state != RESERVED:
                # DISPATCHING/UNKNOWN: possible send in flight — never adopt
                return StoreResult(SEND_UNKNOWN, record=latest)
            if time.time() - latest["ts"] <= latest.get("lease_seconds", 0):
                return StoreResult(LEASE_EXPIRED, record=latest)  # not yet stale
            token = self.store._next_token(records)
            record = dict(latest)
            record.update(
                {
                    "token": token,
                    "owner": owner,
                    "state": RESERVED,
                    "ts": time.time(),
                    "adopted_from_token": latest["token"],
                }
            )
            try:
                self.store._append(record)
            except OSError:
                return StoreResult(STORAGE_REFUSED)
            return StoreResult(RESERVED_OK, token=token, record=record)
        finally:
            self.store.release(lock)

    def hold(self, authorization: Authorization, owner: str, reason: str = ""):
        """Drive a reserved delivery to UNKNOWN (manual reconciliation).

        Acts only on an existing reservation held by this owner; never
        auto-reserves and never auto-retries.
        """
        key = authorization.key()
        lock = self.store.acquire()
        try:
            try:
                states = self.store._states_for(key)
            except _StoreFailure:
                return StoreResult(STORAGE_REFUSED)
            if not states:
                return StoreResult(BINDING_CONFLICT)
            latest = states[-1]
            if latest.get("owner") != owner:
                return StoreResult(STALE_OWNER)
            if UNKNOWN not in _TRANSITIONS.get(latest["state"], set()):
                return StoreResult(SEND_FAILURE)
            record = {
                "schema": SCHEMA_VERSION,
                "binding_key": key,
                "token": latest["token"],
                "owner": owner,
                "state": UNKNOWN,
                "ts": time.time(),
                "lease_seconds": latest.get("lease_seconds", 0),
                "reason": reason[:200],
            }
            try:
                self.store._append(record)
            except OSError:
                return StoreResult(STORAGE_REFUSED)
            return StoreResult(SEND_UNKNOWN, token=latest["token"], record=record)
        finally:
            self.store.release(lock)

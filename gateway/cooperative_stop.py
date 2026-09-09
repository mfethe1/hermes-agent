"""Private cooperative worker; see docs/cooperative-stop-contract.md.

Run: python -m gateway.cooperative_stop PRIVATE_STORE
JSON lines on stdin/stdout carry op, owner, incarnation, and (for finish) ticket.
This consumer reserves local work only; it is not a native StopBoundary adapter.
"""

import argparse
from copy import deepcopy
import fcntl
import json
import os
from pathlib import Path
import stat
import sys
import uuid

from gateway.activation_controller import Blocked, _save


def provision(store: Path, owner: str):
    """Offline, exclusive creation only. Never repair or reset an existing store."""
    if not isinstance(owner, str) or not owner.strip():
        raise ValueError("owner required")
    store.mkdir(mode=0o700)
    with (store / "lock").open("xb"):
        pass
    _save(
        store / "barrier.json",
        {
            "version": 2,
            "owner": owner,
            "incarnation": None,
            "closed": True,
            "stop_latched": False,
            "reservations": {},
        },
    )


class CooperativeWorker:
    """Single-threaded consumer; lifetime flock excludes concurrent replacements.

    All control operations and reservation transitions run on the pipe loop.
    A stopped worker has no reopening path, including on a new incarnation.
    """

    def __init__(self, store: Path):
        self.store = store
        info = store.lstat()
        if (
            not stat.S_ISDIR(info.st_mode)
            or info.st_uid != os.getuid()
            or info.st_mode & 0o077
        ):
            raise Blocked("private owned directory required")
        self.fd = os.open(store / "lock", os.O_RDWR | os.O_NOFOLLOW)
        try:
            info = os.fstat(self.fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise Blocked("invalid lifetime lock")
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self.state = self._read()
            self.failed = False
            self._commit(incarnation=uuid.uuid4().hex, closed=True)
        except BaseException:
            os.close(self.fd)
            raise

    def _read(self):
        with (self.store / "barrier.json").open() as stream:
            body = json.load(stream)
        if (
            not isinstance(body, dict)
            or type(body.get("version")) is not int
            or body["version"] != 2
            or not isinstance(body.get("owner"), str)
            or not body["owner"].strip()
            or type(body.get("closed")) is not bool
            or type(body.get("stop_latched")) is not bool
            or "incarnation" not in body
            or (
                body.get("incarnation") is not None
                and (
                    not isinstance(body["incarnation"], str)
                    or not body["incarnation"].strip()
                )
            )
            or (body["stop_latched"] and not body["closed"])
            or not isinstance(body.get("reservations"), dict)
        ):
            raise Blocked("invalid or legacy accounting; no repair")
        for ticket, record in body["reservations"].items():
            if (
                not isinstance(ticket, str)
                or not ticket.strip()
                or not isinstance(record, dict)
                or record.get("owner") != body["owner"]
                or not isinstance(record.get("incarnation"), str)
                or not record["incarnation"].strip()
                or type(record.get("completed")) is not bool
                or body["incarnation"] is None
            ):
                raise Blocked("invalid reservation accounting; no repair")
        return body

    def _commit(self, **changes):
        # Never expose an in-memory transition before its durable write returns.
        # A failed write may have replaced the file: poison this incarnation so
        # a caller catching the exception cannot continue from stale memory.
        state = {**self.state, **changes}
        try:
            _save(self.store / "barrier.json", state)
        except BaseException:
            self.failed = True
            raise
        self.state = state

    @property
    def tickets(self):
        return {
            ticket
            for ticket, record in self.state["reservations"].items()
            if not record["completed"]
        }

    @property
    def unresolved_prior_work(self):
        return sum(
            not record["completed"]
            and record["incarnation"] != self.state["incarnation"]
            for record in self.state["reservations"].values()
        )

    def snapshot(self, *, accepted=True, reason="", **extra):
        if self.failed:
            raise Blocked("storage outcome unknown; restart required")
        return {
            **deepcopy(self.state),
            "accepted": accepted,
            "reason": reason,
            "active_work": len(self.tickets),
            "unresolved_prior_work": self.unresolved_prior_work,
            "respawn_fenced": False,
            "install_enabled": False,
            **extra,
        }

    def dispatch(self, request):
        if self.failed:
            raise Blocked("storage outcome unknown; restart required")
        if not isinstance(request, dict):
            return self.snapshot(accepted=False, reason="object required")
        if (
            request.get("owner") != self.state["owner"]
            or request.get("incarnation") != self.state["incarnation"]
        ):
            return self.snapshot(accepted=False, reason="owner/incarnation mismatch")
        # Missing/corrupt/changed storage is fatal to the consumer, never a reopen.
        if self._read() != self.state:
            raise Blocked("barrier changed; reconcile without replay")
        handlers = {
            "status": self.snapshot,
            "open": self._open,
            "begin": self._begin,
            "stop": self._stop,
        }
        if request.get("op") == "finish":
            return self._finish(request.get("ticket"))
        handler = (
            handlers.get(request.get("op"))
            if isinstance(request.get("op"), str)
            else None
        )
        return (
            handler()
            if handler
            else self.snapshot(accepted=False, reason="unknown operation")
        )

    def _open(self):
        if self.state["stop_latched"]:
            return self.snapshot(accepted=False, reason="permanent stop latch")
        if self.unresolved_prior_work:
            return self.snapshot(accepted=False, reason="unresolved prior incarnation")
        self._commit(closed=False)
        return self.snapshot()

    def _begin(self):
        if self.state["closed"] or self.unresolved_prior_work:
            return self.snapshot(accepted=False, reason="admission closed")
        ticket = uuid.uuid4().hex
        self._commit(
            reservations={
                **self.state["reservations"],
                ticket: {
                    "owner": self.state["owner"],
                    "incarnation": self.state["incarnation"],
                    "completed": False,
                },
            }
        )
        return self.snapshot(ticket=ticket)

    def _finish(self, ticket):
        if not isinstance(ticket, str) or ticket not in self.tickets:
            return self.snapshot(accepted=False, reason="unknown reservation")
        record = self.state["reservations"][ticket]
        if record["incarnation"] != self.state["incarnation"]:
            return self.snapshot(
                accepted=False, reason="prior incarnation requires recovery"
            )
        self._commit(
            reservations={
                **self.state["reservations"],
                ticket: {**record, "completed": True},
            }
        )
        return self.snapshot()

    def _stop(self):
        self._commit(closed=True, stop_latched=True)
        return self.snapshot()

    def close(self):
        os.close(self.fd)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("store", type=Path)
    args = parser.parse_args(argv)
    worker = None
    try:
        worker = CooperativeWorker(args.store)
        print(json.dumps(worker.snapshot()), flush=True)
        for line in sys.stdin:
            result = worker.dispatch(json.loads(line))
            print(json.dumps(result), flush=True)
            if (
                result["accepted"]
                and worker.state["stop_latched"]
                and not worker.tickets
            ):
                return 0
        # Lost controller/EOF leaves state closed on the next startup. It is not
        # a successful stop acknowledgement and does not claim completed work.
        return 2
    except (OSError, ValueError, Blocked) as exc:
        print(
            json.dumps({"error": type(exc).__name__, "install_enabled": False}),
            file=sys.stderr,
        )
        return 2
    finally:
        if worker is not None:
            worker.close()


if __name__ == "__main__":
    raise SystemExit(main())

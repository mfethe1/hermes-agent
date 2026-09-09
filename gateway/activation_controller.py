"""External activation stop adapter. Native apply deliberately fails preflight.

Callable with ``python -m gateway.activation_controller --preflight``.
Only the injected disposable boundary is executable today; no native signal or
install implementation exists. Marker ownership is not an admission fence.
"""
import argparse
from contextlib import contextmanager
from dataclasses import asdict
import fcntl
import json
import math
import os
from pathlib import Path
import plistlib
import stat
import tempfile
import time
import uuid

from gateway import drain_control
from gateway.safe_stop import _refusal, stop_non_escalating


class Blocked(RuntimeError):
    pass


def native_preflight(plist: Path) -> dict:
    """Read configuration only. Configuration never proves a runtime fence."""
    missing = [
        'native atomic identity/owner/quiescence TERM boundary unavailable',
        'maintained launchd respawn fence unavailable',
        'authoritative whole-service conversation/API/cron quiescence unavailable',
        'installed owner-token writers and synchronous startup admission unverified',
        'complete install/rollback/start/health adapter not accepted',
        'independent successor review and live activation authorization required',
    ]
    service = {}
    try:
        body = plistlib.loads(plist.read_bytes())
        if not isinstance(body, dict):
            raise ValueError('service metadata must be a dictionary')
        service = {k: body.get(k) for k in ('Label', 'KeepAlive', 'ExitTimeOut')}
        if body.get('ExitTimeOut') != 0:
            missing.append('launchd ExitTimeOut does not establish non-escalation')
    except (OSError, ValueError, TypeError) as exc:
        missing.append(f'service metadata unavailable: {type(exc).__name__}')
    return {'ready': False, 'install_enabled': False, 'missing_capabilities': missing,
            'service': service}


def _save(path, body):
    fd, temp = tempfile.mkstemp(dir=path.parent, prefix='.activation-')
    try:
        with os.fdopen(fd, 'w') as stream:
            json.dump(body, stream, indent=2)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temp, path)
        fd = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
    finally:
        if os.path.exists(temp):
            os.unlink(temp)


class ActivationAdapter:
    """Concrete marker + durable journal + safe_stop wiring for an external caller.

    The boundary must meet StopBoundary independently. Passing an object here
    does not establish native acceptance. This adapter grants no install right.
    Receipt directory must be private, precreated, and outside the live runtime.
    One receipt is one attempt forever; existing receipts reconcile, never replay.
    """
    def __init__(self, *, home: Path, receipt: Path, boundary):
        self.home, self.receipt, self.boundary = home, receipt, boundary

    @contextmanager
    def _locked(self):
        # Per-home lock serializes different receipt names as well as restarts.
        lock = self.home / '.activation-controller.lock'
        fd = os.open(lock, os.O_CREAT | os.O_RDWR | os.O_NOFOLLOW, 0o600)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise Blocked('unsafe controller lock')
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            current = lock.stat()
            if (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino):
                raise Blocked('controller lock replaced')
            yield
        finally:
            os.close(fd)

    def marker_owner(self):
        body = drain_control.read_drain_request(home=self.home)
        return None if body is None else body.get('request_id', '')

    def refresh(self, owner):
        if drain_control.refresh_drain_request(home=self.home, request_id=owner) is None:
            raise Blocked('drain ownership lost')

    def clear(self, owner):
        if not drain_control.clear_drain_request(home=self.home, request_id=owner):
            raise Blocked('drain ownership lost; foreign marker retained')

    def stop(self, *, identity, timeout=30):
        if not isinstance(identity, str) or not identity.strip():
            raise Blocked("explicit identity required")
        if type(timeout) not in (int, float) or not math.isfinite(timeout) or not 0 < timeout <= 300:
            raise Blocked("finite timeout between zero and 300 seconds required")
        with self._locked():
            if self.receipt.exists():
                return self._reconcile(timeout=timeout)
            record = {'schema_version': 1, 'home': str(self.home.resolve()),
                      'attempt': uuid.uuid4().hex, 'identity': identity,
                      'phase': 'creating_marker', 'owner': None}
            # If marker creation succeeds but its acknowledgement is lost, do not
            # adopt by principal. Recovery retains it for explicit reconciliation.
            _save(self.receipt, record)
            marker = drain_control.create_drain_request(
                home=self.home, principal='activation:' + record['attempt'],
                suppress_notification=True,
            )
            if marker is None:
                raise Blocked('existing marker; no stop attempted')
            record.update(owner=marker['request_id'], phase='stop_entered')
            _save(self.receipt, record)  # durable BEFORE entering TERM boundary
            result = stop_non_escalating(
                self.boundary, owner=record['owner'], identity=identity, timeout=timeout,
            )
            record.update(phase='observed', result=asdict(result))
            _save(self.receipt, record)
            return {**record, 'install_enabled': False}

    def reconcile(self, *, timeout=5):
        with self._locked():
            return self._reconcile(timeout=timeout)

    def _reconcile(self, *, timeout):
        record = json.loads(self.receipt.read_text())
        if record.get('schema_version') != 1 or record.get('home') != str(self.home.resolve()):
            raise Blocked('receipt schema/home mismatch')
        owner, identity = record.get('owner'), record.get('identity')
        state, reason = 'unknown', 'marker acknowledgement missing; manual reconciliation required'
        if isinstance(owner, str) and owner and isinstance(identity, str) and identity:
            if self.marker_owner() != owner:
                reason = 'marker ownership lost; no replay or release'
            else:
                deadline = time.monotonic() + timeout
                try:
                    s = self.boundary.observe(deadline=deadline)
                    refusal = _refusal(s, owner, identity, 1.0)
                    if not refusal and time.monotonic() < deadline and s.service_stopped is True:
                        state, reason = 'stopped', 'fresh whole-service absence under maintained fences'
                    else:
                        reason = refusal or 'service stop unconfirmed; no replay'
                except Exception as exc:
                    reason = f'observation failed: {type(exc).__name__}; no replay'
        record.update(reconciliation={'state': state, 'reason': reason})
        _save(self.receipt, record)
        return {**record, 'install_enabled': False}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--preflight', '--dry-run', action='store_true')
    mode.add_argument('--apply', action='store_true')
    parser.add_argument('--plist', type=Path, required=True)
    args = parser.parse_args(argv)
    result = native_preflight(args.plist)
    result['mode'] = 'apply-refused' if args.apply else 'preflight'
    print(json.dumps(result, indent=2))
    return 2


if __name__ == '__main__':
    raise SystemExit(main())

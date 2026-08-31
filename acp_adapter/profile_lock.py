"""Exclusive per-profile ACP runtime lease.

Hermes profiles are state homes, not stateless presets.  An ACP process and the
profile gateway must never write the same profile concurrently.  Reuse the
existing OS-backed gateway runtime lock as the single authority so gateway↔ACP
and ACP↔ACP races fail closed; keep a separate metadata sidecar for diagnostics
only.
"""

from __future__ import annotations

import json
import os
from pathlib import Path

from hermes_constants import get_hermes_home


class ProfileRuntimeBusy(RuntimeError):
    """Raised when another gateway or ACP process owns this profile runtime."""


_active_lease: "ProfileRuntimeLease | None" = None


class ProfileRuntimeLease:
    """Process-owned profile runtime lease released explicitly or on process exit."""

    def __init__(self, profile_name: str, metadata_path: Path) -> None:
        self.profile_name = profile_name
        self.metadata_path = metadata_path
        self._released = False

    def release(self) -> None:
        global _active_lease
        if self._released:
            return
        self._released = True
        from gateway.status import release_gateway_runtime_lock

        release_gateway_runtime_lock()
        try:
            self.metadata_path.unlink(missing_ok=True)
        except OSError:
            pass
        if _active_lease is self:
            _active_lease = None


def _write_metadata(path: Path, profile_name: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "holder": "acp",
        "pid": os.getpid(),
        "profile": profile_name,
    }
    temp_path = path.with_suffix(".tmp")
    temp_path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
    os.replace(temp_path, path)


def acquire_profile_runtime_lease(profile_name: str) -> ProfileRuntimeLease:
    """Acquire the selected profile's shared gateway/ACP runtime authority.

    The gateway lock is an OS lock, so process death releases it automatically.
    ``acp.runtime.lock`` is metadata only and never participates in exclusion.
    """

    global _active_lease
    from gateway.status import acquire_gateway_runtime_lock

    if _active_lease is not None and not _active_lease._released:
        raise ProfileRuntimeBusy(
            f"Hermes profile {_active_lease.profile_name!r} is already active in this ACP process"
        )

    if not acquire_gateway_runtime_lock():
        raise ProfileRuntimeBusy(
            f"Hermes profile {profile_name!r} is already active; its gateway is already "
            "running or another ACP process owns the profile"
        )

    metadata_path = get_hermes_home() / "acp.runtime.lock"
    try:
        _write_metadata(metadata_path, profile_name)
    except Exception:
        from gateway.status import release_gateway_runtime_lock

        release_gateway_runtime_lock()
        raise
    lease = ProfileRuntimeLease(profile_name, metadata_path)
    _active_lease = lease
    return lease

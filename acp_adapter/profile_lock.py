"""Exclusive per-profile ACP runtime lease.

Hermes profiles are state homes, not stateless presets. An ACP process and the
profile gateway must never write the same profile concurrently. Reuse the
existing OS-backed gateway runtime lock as the single authority so gateway↔ACP
and ACP↔ACP races fail closed. No second liveness file is written: process death
releases the kernel lock even when the ACP host terminates the child abruptly.
"""

from __future__ import annotations


class ProfileRuntimeBusy(RuntimeError):
    """Raised when another gateway or ACP process owns this profile runtime."""


_active_lease: "ProfileRuntimeLease | None" = None


class ProfileRuntimeLease:
    """Process-owned profile runtime lease released explicitly or on process exit."""

    def __init__(self, profile_name: str) -> None:
        self.profile_name = profile_name
        self._released = False

    def release(self) -> None:
        global _active_lease
        if self._released:
            return
        self._released = True
        from gateway.status import release_gateway_runtime_lock

        release_gateway_runtime_lock()
        if _active_lease is self:
            _active_lease = None


def acquire_profile_runtime_lease(profile_name: str) -> ProfileRuntimeLease:
    """Acquire the selected profile's shared gateway/ACP runtime authority."""

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

    lease = ProfileRuntimeLease(profile_name)
    _active_lease = lease
    return lease

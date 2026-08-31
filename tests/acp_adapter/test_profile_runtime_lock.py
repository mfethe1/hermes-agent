from __future__ import annotations

from pathlib import Path

import pytest

from acp_adapter.profile_lock import ProfileRuntimeBusy, acquire_profile_runtime_lease


def test_profile_runtime_lease_is_exclusive_and_released(monkeypatch, tmp_path):
    from gateway import status as gateway_status

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(gateway_status, "is_gateway_runtime_lock_active", lambda *_: False)

    first = acquire_profile_runtime_lease("jake")
    with pytest.raises(ProfileRuntimeBusy, match="already active"):
        acquire_profile_runtime_lease("jake")

    first.release()
    second = acquire_profile_runtime_lease("jake")
    second.release()


def test_profile_runtime_lease_refuses_running_gateway(monkeypatch, tmp_path):
    from gateway import status as gateway_status

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(gateway_status, "acquire_gateway_runtime_lock", lambda: False)

    with pytest.raises(ProfileRuntimeBusy, match="gateway is already running"):
        acquire_profile_runtime_lease("default")


def test_profile_runtime_lock_metadata_contains_no_home_path(monkeypatch, tmp_path):
    from gateway import status as gateway_status

    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.setattr(gateway_status, "is_gateway_runtime_lock_active", lambda *_: False)

    lease = acquire_profile_runtime_lease("archie")
    try:
        contents = Path(tmp_path, "acp.runtime.lock").read_text(encoding="utf-8")
        assert '"profile": "archie"' in contents
        assert str(tmp_path) not in contents
    finally:
        lease.release()

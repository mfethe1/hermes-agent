"""Gateways the POSIX pause stopped restart once the update's tail finished, never inside it.

2026-10-24: the pause restarted the gateway as soon as the dependencies were synced, before
``→ Building the TUI…``. While the product builds ran, launchd relaunched it repeatedly; one
relaunch logged ``install out of sync (venv: out of sync with uv.lock)``, and the post-update
maintenance (config migration, skill sync) had not run yet.
"""
from __future__ import annotations

import pytest


@pytest.fixture
def completion(tmp_path, monkeypatch):
    """``(request, events)``: a completion request owing a POSIX resume; ``events`` logs the tail's
    steps (the real ``complete_source_checkout`` order, its long steps stubbed) and the resume."""
    from hermes_cli import (
        main, source_build, source_stamp, update_cmd, update_cmd_config, update_cmd_maint, update_cmd_posix_pause,
        venv_sync,
    )

    events: list[str] = []
    monkeypatch.setattr(venv_sync, "publish_launchers", lambda root: events.append("launchers"))
    monkeypatch.setattr(source_build, "build_update_products", lambda root, desktop: events.append("build"))
    monkeypatch.setattr(update_cmd_maint, "_run_post_update_maintenance",
                        lambda **kwargs: events.append("maintenance") or True)
    monkeypatch.setattr(source_stamp, "write_source_stamp", lambda root: None)
    monkeypatch.setattr(update_cmd_posix_pause, "resume_paused_set", lambda token: events.append("resume"))
    monkeypatch.setattr(update_cmd, "_sweep_bytecode_after_update", lambda branch: None)
    monkeypatch.setattr(update_cmd, "_fleet_restart_skip_reason", lambda plan: "not under test")
    monkeypatch.setattr(update_cmd, "_pending_fleet_restart_needed", lambda: False)
    monkeypatch.setattr(main, "PROJECT_ROOT", main.PROJECT_ROOT)
    monkeypatch.setattr(update_cmd_config, "_LAST_SIBLING_SNAPSHOTS", None, raising=False)
    request = {"source": str(tmp_path), "sibling_snapshots": {}, "plan": None, "branch": "main", "desktop": False,
               "assume_yes": True, "gateway_mode": False, "snapshot_id": None, "pre_update_version": "1.0",
               "windows_resume": {"platform": "posix", "resume_needed": True, "posix_units": [], "unmapped": []}}
    return request, events


def test_paused_gateways_restart_after_the_builds_and_maintenance(completion):
    from hermes_cli.update_completion import _complete_selected

    request, events = completion
    _complete_selected(request)
    assert events == ["launchers", "build", "maintenance", "resume"]


def test_a_tail_that_never_ran_still_restarts_the_paused_gateways(completion, monkeypatch):
    from hermes_cli import source_completion
    from hermes_cli.update_completion import _complete_selected

    def refused(*args, **kwargs):
        raise RuntimeError("an update is still running")

    monkeypatch.setattr(source_completion, "complete_source_checkout", refused)
    request, events = completion
    _complete_selected(request)
    assert events == ["resume"]

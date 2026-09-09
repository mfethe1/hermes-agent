"""Pre-existing maintenance drain must apply before startup admits any work."""
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

import gateway.drain_control as dc
import gateway.run as run
from gateway.config import Platform
from gateway.session import SessionEntry
from tests.gateway.restart_test_helpers import make_restart_runner, make_restart_source


@pytest.mark.asyncio
@pytest.mark.parametrize("marker", [False, True])
async def test_startup_latches_drain_before_first_await_and_cron(monkeypatch, tmp_path, marker):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    if marker:
        dc.write_drain_request(principal="startup-test")
    runner, adapter = make_restart_runner()
    runner._running = False
    runner._external_drain_active = False
    phases = []

    async def abort_check():
        # First awaited startup phase: the watcher has never been started.
        phases.append("abort")
        assert runner._external_drain_active is marker
        await asyncio.sleep(0)
        return False

    async def connect(pending):
        phases.append("connect")
        assert runner._external_drain_active is marker
        await adapter.connect()
        return []

    for name in ("_start_install_faulthandler", "_start_log_startup_environment",
                 "_start_startup_warmup", "_wire_teams_pipeline_runtime",
                 "_install_plugin_message_injector", "_start_spawn_background_watchers"):
        monkeypatch.setattr(runner, name, MagicMock())
    monkeypatch.setattr(runner, "_abort_startup_if_shutdown_requested", abort_check)
    monkeypatch.setattr(runner, "_start_check_access_policy", lambda: False)
    monkeypatch.setattr(runner, "_start_recover_previous_run", AsyncMock())
    monkeypatch.setattr(runner, "_start_prefilter_platforms", AsyncMock(return_value=(False, 1, [], [])))
    monkeypatch.setattr(runner, "_start_connect_pending", connect)
    monkeypatch.setattr(runner, "_start_aggregate_connect_results", AsyncMock(return_value=1))
    monkeypatch.setattr(runner, "_start_secondary_profiles", AsyncMock(return_value=(False, 1)))
    monkeypatch.setattr(runner, "_start_handle_no_connections", lambda *args: False)
    runner.delivery_router = MagicMock()

    # Exercise the real post-connect resume scheduling while other lifecycle I/O is isolated.
    async def finish(connected_count):
        assert runner._external_drain_active is marker
        phases.append("finish")
        runner._schedule_resume_pending_sessions()
    monkeypatch.setattr(runner, "_start_finish_wiring", finish)
    assert await runner.start() is True
    assert "connect" in phases and "finish" in phases
    assert runner._running is True
    runner._update_runtime_status.assert_called_with("draining" if marker else "running")

    # Capture the actual cron provider wiring without creating runtime threads.
    from cron.scheduler_provider import InProcessCronScheduler
    import cron.scheduler_provider as providers
    provider = InProcessCronScheduler()
    monkeypatch.setattr(providers, "resolve_cron_scheduler", lambda: provider)
    monkeypatch.setattr(providers, "scheduler_for_profile_mode", lambda p, **kw: p)
    threads = []
    def thread(**kwargs):
        threads.append(kwargs)
        return MagicMock()
    monkeypatch.setattr(run.threading, "Thread", thread)
    run._start_gateway_start_cron_and_housekeeping(runner)
    gate = threads[0]["kwargs"]["can_dispatch"]
    assert gate() is (not marker)
    # Execute a real scheduler iteration: a closed gate must never call tick.
    import cron.scheduler as scheduler
    tick = MagicMock()
    monkeypatch.setattr(scheduler, "tick", tick)
    stop = MagicMock()
    stop.is_set.side_effect = [False, True]
    provider.start(stop, can_dispatch=gate)
    assert tick.call_count == (0 if marker else 1)
    assert dc.drain_requested() is marker


@pytest.mark.asyncio
@pytest.mark.parametrize("marker", [False, True])
async def test_pending_resume_does_not_claim_or_spawn_while_drained(monkeypatch, tmp_path, marker):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    runner, adapter = make_restart_runner()
    runner._external_drain_active = marker
    runner._is_user_authorized = lambda source: True
    runner._persist_active_agents = MagicMock()
    source = make_restart_source()
    entry = SessionEntry(
        session_key="agent:main:telegram:dm:123456", session_id="pending",
        created_at=datetime.now(), updated_at=datetime.now(), origin=source,
        platform=Platform.TELEGRAM, chat_type="dm", resume_pending=True,
        resume_reason="restart_timeout", last_resume_marked_at=datetime.now(),
    )
    runner.session_store._entries = {entry.session_key: entry}
    runner._run_startup_resume_event = AsyncMock()
    assert runner._schedule_resume_pending_sessions() == (0 if marker else 1)
    await asyncio.sleep(0)
    assert runner._run_startup_resume_event.await_count == (0 if marker else 1)
    assert (entry.session_key in runner._running_agents) is (not marker)
    assert runner._persist_active_agents.call_count == (0 if marker else 1)
    assert entry.resume_pending is True

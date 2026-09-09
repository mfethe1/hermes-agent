"""Pre-existing maintenance drain must apply before startup admits any work."""
import asyncio
from datetime import datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

import gateway.drain_control as dc
import gateway.run as run
from gateway.config import Platform
from gateway.session import SessionEntry
from tests.gateway.restart_test_helpers import make_restart_runner, make_restart_source


async def _start_runner(monkeypatch, runner, adapter, marker):
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


@pytest.mark.asyncio
@pytest.mark.parametrize("body", [None, b"{broken-json", b"\xff", b""])
async def test_startup_latches_drain_before_first_await_and_cron(monkeypatch, tmp_path, body):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    marker = body is not None
    if body == b"":
        dc.write_drain_request(principal="startup-test")
    elif marker:
        dc.drain_request_path().write_bytes(body)
    runner, adapter = make_restart_runner()
    await _start_runner(monkeypatch, runner, adapter, marker)

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
    assert dc.drain_request_path().exists() is marker


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


@pytest.mark.asyncio
@pytest.mark.parametrize("shutdown", [None, "_draining", "_restart_requested", "_shutdown_event"])
async def test_watcher_cancellation_resumes_only_eligible_sessions(monkeypatch, tmp_path, shutdown):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    runner, adapter = make_restart_runner()
    runner._persist_active_agents = MagicMock()
    runner._run_startup_resume_event = AsyncMock()
    now = datetime.now()
    entries = {}
    for name in ("eligible", "revoked", "stale", "suspended", "claimed", "offline"):
        source = make_restart_source(chat_id=name)
        source.user_id = name
        if name == "offline":
            source.platform = Platform.DISCORD
        entry = SessionEntry(
            session_key=f"agent:main:{source.platform.value}:dm:{name}", session_id=name,
            created_at=now, updated_at=now, origin=source, platform=source.platform,
            chat_type="dm", resume_pending=True, resume_reason="restart_timeout",
            last_resume_marked_at=now, suspended=name == "suspended",
        )
        if name == "stale":
            entry.last_resume_marked_at = now - timedelta(
                seconds=run._auto_continue_freshness_window() + 60,
            )
        entries[name] = entry
    runner.session_store._entries = {e.session_key: e for e in entries.values()}
    runner._is_user_authorized = lambda source: source.user_id != "revoked"
    runner._session_state(entries["claimed"].session_key).turn.agent = object()
    dc.write_drain_request(principal="disposable-startup-recovery")
    await _start_runner(monkeypatch, runner, adapter, True)
    runner._run_startup_resume_event.assert_not_awaited()
    assert runner._schedule_resume_pending_sessions() == 0
    assert all(e.resume_pending for e in entries.values())

    if shutdown == "_shutdown_event":
        runner._shutdown_event.set()
    elif shutdown:
        setattr(runner, shutdown, True)
    runner._update_runtime_status.reset_mock()
    engaged, released = asyncio.Event(), asyncio.Event()
    runner._persist_active_agents.side_effect = engaged.set
    real_exit = runner._exit_external_drain

    def observe_exit():
        real_exit()
        released.set()

    monkeypatch.setattr(runner, "_exit_external_drain", observe_exit)
    watcher = asyncio.create_task(runner._drain_control_watcher(interval=0.01))
    try:
        await asyncio.wait_for(engaged.wait(), 2)
        assert runner._external_drain_active
        dc.clear_drain_request()
        await asyncio.wait_for(released.wait(), 2)
        await asyncio.sleep(0)
        automatic_count = runner._run_startup_resume_event.await_count
        assert automatic_count == (0 if shutdown else 1)
        # Correct post-fix contract: cancellation already claimed the eligible session.
        manual_count = runner._schedule_resume_pending_sessions()
        assert manual_count == 0
        assert runner._schedule_resume_pending_sessions() == 0
        await asyncio.sleep(0)
        assert runner._run_startup_resume_event.await_count == automatic_count
        if shutdown:
            runner._update_runtime_status.assert_not_called()
        else:
            runner._update_runtime_status.assert_called_once_with("running")
            assert runner._run_startup_resume_event.await_args.args[2] == entries["eligible"].session_key
        assert all(e.resume_pending for e in entries.values())
        for name in ("revoked", "stale", "suspended", "offline"):
            assert not runner._is_session_running(entries[name].session_key)
    finally:
        watcher.cancel()
        with pytest.raises(asyncio.CancelledError):
            await watcher

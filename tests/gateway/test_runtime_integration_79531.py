"""Owned drain replacement must preserve startup admission until its owner clears."""
import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import pytest

import gateway.drain_control as dc
from gateway.config import Platform
from gateway.session import SessionEntry
from tests.gateway.restart_test_helpers import make_restart_runner, make_restart_source
from tests.gateway.test_startup_drain_admission import _start_runner


@pytest.mark.asyncio
async def test_owned_replacement_holds_startup_until_matching_cancellation(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    first = dc.create_drain_request(home=tmp_path, principal="integration")
    runner, adapter = make_restart_runner()
    runner._persist_active_agents = MagicMock()
    runner._run_startup_resume_event = AsyncMock()
    now = datetime.now()
    entry = SessionEntry(
        session_key="agent:main:telegram:dm:123456", session_id="integration",
        created_at=now, updated_at=now, origin=make_restart_source(),
        platform=Platform.TELEGRAM, chat_type="dm", resume_pending=True,
        resume_reason="restart_timeout", last_resume_marked_at=now,
    )
    runner.session_store._entries = {entry.session_key: entry}
    await _start_runner(monkeypatch, runner, adapter, True)
    runner._run_startup_resume_event.assert_not_awaited()

    replacement = dc.write_drain_request(home=tmp_path, principal="integration")
    marker = dc.drain_request_path(tmp_path)
    replacement_bytes = marker.read_bytes()
    assert replacement["request_id"] != first["request_id"]
    assert not dc.clear_drain_request(home=tmp_path, request_id=first["request_id"])
    assert dc.refresh_drain_request(home=tmp_path, request_id=first["request_id"]) is None
    assert marker.read_bytes() == replacement_bytes

    observed, released = asyncio.Event(), asyncio.Event()
    runner._persist_active_agents.side_effect = observed.set
    real_exit = runner._exit_external_drain

    def observe_exit():
        real_exit()
        released.set()

    monkeypatch.setattr(runner, "_exit_external_drain", observe_exit)
    watcher = asyncio.create_task(runner._drain_control_watcher(interval=0.01))
    try:
        await asyncio.wait_for(observed.wait(), 2)
        assert runner._external_drain_active
        assert runner._schedule_resume_pending_sessions() == 0
        runner._run_startup_resume_event.assert_not_awaited()
        assert dc.clear_drain_request(home=tmp_path, request_id=replacement["request_id"])
        await asyncio.wait_for(released.wait(), 2)
        await asyncio.sleep(0)
        assert not runner._external_drain_active
        assert not marker.exists()
        runner._run_startup_resume_event.assert_awaited_once()
        assert runner._schedule_resume_pending_sessions() == 0
        assert entry.resume_pending
    finally:
        watcher.cancel()
        with pytest.raises(asyncio.CancelledError):
            await watcher

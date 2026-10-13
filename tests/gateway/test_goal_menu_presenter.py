"""A native menu tap is a goal control operation, not an untrusted conversational prompt."""
import contextlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from gateway.config import Platform
from gateway.session import SessionSource
from gateway.platforms.event import MessageEvent, MessageType
from hermes_cli import goals
from hermes_cli.goal_command import dispatch_goal_command


@pytest.fixture
def setup(monkeypatch):
    mgr = goals.GoalManager("presenter-menu")
    mgr.set("Verify browser", contract=goals.GoalContract(verification="browser acceptance"))
    mgr.state.last_reason = "disk prerequisite"; mgr.pause("disk")
    card = dispatch_goal_command(mgr, "unblock", authorize_gate=lambda: None)
    source = SessionSource(platform=Platform.TELEGRAM, chat_id="123", chat_type="dm", user_id="11", thread_id="7")
    class Native:
        async def send_goal_recovery_menu(self, *args, **kwargs):
            return SimpleNamespace(success=True)
    adapter = Native()
    adapter.send_goal_recovery_menu = AsyncMock(return_value=SimpleNamespace(success=True))
    enqueued = []
    async def current(event): return goals.GoalManager(mgr.session_id), None
    async def executor(fn): return fn()
    from hermes_constants import get_hermes_home
    adapter._message_handler = object()
    adapter._active_sessions = {}
    adapter._drop_unresolved = lambda event: False
    adapter._event_session_key = lambda event: "origin-key"
    def start(event, key):
        enqueued.append((event.source, event.text))
        return True
    adapter._start_session_processing = start
    runner = SimpleNamespace(_delivery_adapter_for=lambda s: adapter, _resume_caller_is_admin=lambda s: s.user_id=="11",
        _resolve_profile_home_for_source=lambda s: get_hermes_home(), _thread_metadata_for_source=lambda s: {"thread_id": s.thread_id},
        _get_goal_manager_for_event=current,
        _run_in_executor_with_context=executor, _clear_goal_continuations=lambda *a: None,
        _enqueue_goal_turn=lambda event, prompt, **kw: False)
    return mgr, card, source, runner, adapter, enqueued


@pytest.mark.asyncio
async def test_click_goes_through_shared_authorized_command_and_enqueues_original_goal(setup, monkeypatch):
    from gateway import goal_menu_presenter as presenter
    monkeypatch.setattr(presenter, "_scope", lambda home: contextlib.nullcontext())
    mgr, card, source, runner, adapter, enqueued = setup
    assert await presenter.send_menu(runner, source, mgr.session_id, card.menu)
    callback = adapter.send_goal_recovery_menu.await_args.args[2]
    result = await callback(source, card.menu["id"], "resume")
    assert not result.error
    assert len(enqueued)==1 and "Verify browser" in enqueued[0][1]
    assert goals.GoalManager(mgr.session_id).state.status == "active"


@pytest.mark.asyncio
async def test_other_chat_click_and_new_session_cannot_resume(setup, monkeypatch):
    from gateway import goal_menu_presenter as presenter
    monkeypatch.setattr(presenter, "_scope", lambda home: contextlib.nullcontext())
    mgr, card, source, runner, adapter, enqueued = setup
    await presenter.send_menu(runner, source, mgr.session_id, card.menu)
    callback = adapter.send_goal_recovery_menu.await_args.args[2]
    elsewhere = SessionSource(platform=Platform.TELEGRAM, chat_id="456", chat_type="dm", user_id="11", thread_id="7")
    assert (await callback(elsewhere, card.menu["id"], "resume")).error
    other_topic = SessionSource(platform=Platform.TELEGRAM, chat_id="123", chat_type="dm", user_id="11", thread_id="8")
    assert (await callback(other_topic, card.menu["id"], "resume")).error
    other = goals.GoalManager("newer-session"); other.set("new goal"); other.pause()
    async def current(event): return other, None
    runner._get_goal_manager_for_event = current
    assert (await callback(source, card.menu["id"], "resume")).error
    assert not enqueued
    assert goals.GoalManager(mgr.session_id).state.status == "paused"


@pytest.mark.asyncio
async def test_idle_menu_click_starts_processing_instead_of_leaving_a_fifo_orphan(setup, monkeypatch):
    from gateway import goal_menu_presenter as presenter
    monkeypatch.setattr(presenter, "_scope", lambda home: contextlib.nullcontext())
    mgr, card, source, runner, adapter, enqueued = setup
    adapter._start_session_processing = lambda event, key: False
    await presenter.send_menu(runner, source, mgr.session_id, card.menu)
    callback = adapter.send_goal_recovery_menu.await_args.args[2]
    result = await callback(source, card.menu["id"], "resume")
    assert result.error
    assert not enqueued
    assert goals.GoalManager(mgr.session_id).state.status == "paused"
    assert result.menu


@pytest.mark.asyncio
async def test_busy_menu_click_queues_without_interrupting_current_work(setup, monkeypatch):
    from gateway import goal_menu_presenter as presenter
    monkeypatch.setattr(presenter, "_scope", lambda home: contextlib.nullcontext())
    mgr, card, source, runner, adapter, enqueued = setup
    adapter._active_sessions["origin-key"] = object()
    def queue(event, prompt, **kw):
        enqueued.append((event.source, prompt))
        return True
    runner._enqueue_goal_turn = queue
    adapter._start_session_processing = lambda *a: (_ for _ in ()).throw(AssertionError("busy work interrupted"))
    await presenter.send_menu(runner, source, mgr.session_id, card.menu)
    callback = adapter.send_goal_recovery_menu.await_args.args[2]
    result = await callback(source, card.menu["id"], "resume")
    assert not result.error and len(enqueued)==1


@pytest.mark.asyncio
async def test_non_native_adapter_falls_back_without_sending_fake_buttons(setup):
    from gateway import goal_menu_presenter as presenter
    mgr, card, source, runner, _, _ = setup
    runner._delivery_adapter_for = lambda s: SimpleNamespace(send=AsyncMock())
    assert not await presenter.send_menu(runner, source, mgr.session_id, card.menu)


@pytest.mark.asyncio
async def test_button_tap_does_not_require_admin_and_audit_records_who(setup, monkeypatch, caplog):
    """Buttons do not require admin (Michael, 2026-10-13). A tap from the menu's own chat/topic by a user the
    adapter allowlisted is applied even when admin gating would deny it, and the audit line names the actor."""
    import logging
    from gateway import goal_menu_presenter as presenter
    monkeypatch.setattr(presenter, "_scope", lambda home: contextlib.nullcontext())
    mgr, card, source, runner, adapter, enqueued = setup
    runner._resume_caller_is_admin = lambda s: False  # nobody is an explicit admin (matches live config)
    await presenter.send_menu(runner, source, mgr.session_id, card.menu)
    callback = adapter.send_goal_recovery_menu.await_args.args[2]
    member = SessionSource(platform=Platform.TELEGRAM, chat_id="123", chat_type="dm", user_id="99", thread_id="7")
    with caplog.at_level(logging.INFO, logger="gateway.goal_menu_presenter"):
        applied = await callback(member, card.menu["id"], "resume")
    assert not applied.error and len(enqueued) == 1
    assert goals.GoalManager(mgr.session_id).state.status == "active"
    lines = [r.getMessage() for r in caplog.records if "goal_menu_tap" in r.getMessage()]
    assert lines == [f"goal_menu_tap outcome=applied reason=ok user=99 chat=123 thread=7 "
                     f"session={mgr.session_id} menu={card.menu['id']} action=resume"]
    caplog.clear()
    with caplog.at_level(logging.INFO, logger="gateway.goal_menu_presenter"):
        again = await callback(source, card.menu["id"], "resume")
    assert again.error and len(enqueued) == 1  # menu is single-use: a second tap cannot execute twice

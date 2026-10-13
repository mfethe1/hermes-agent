"""Deliver native goal controls through the receiving adapter and the shared command contract."""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

from gateway.platforms.event import MessageEvent, MessageType
from gateway.session_identity import replace_source
from hermes_cli.goal_command import GoalCommandResult, dispatch_goal_command


def _scope(home):
    from gateway.run import _profile_runtime_scope
    return _profile_runtime_scope(home)


async def status_menu(runner, source, manager, decision):
    adapter = runner._delivery_adapter_for(source)
    if (adapter is None or not callable(getattr(type(adapter), "send_goal_recovery_menu", None))
            or (decision.get("status") != "paused" and decision.get("verdict") != "wait")):
        return None
    from hermes_cli.goal_unblock import open_menu
    try:
        return await runner._run_in_executor_with_context(lambda: open_menu(manager))
    except (RuntimeError, ValueError) as exc:
        logger.debug("Goal menu not opened: %s", type(exc).__name__)
        return None


def schedule_continuation(runner, event, prompt):
    """Synchronous check + admission: busy lanes queue; idle lanes start, never interrupt."""
    adapter = runner._delivery_adapter_for(event.source)
    if adapter is None or not getattr(adapter, "_message_handler", None):
        return False
    turn = MessageEvent(text=prompt, message_type=MessageType.TEXT, source=event.source,
                        metadata={"goal_continuation": True})
    if adapter._drop_unresolved(turn):
        return False
    key = adapter._event_session_key(turn)
    if key in adapter._active_sessions:
        return runner._enqueue_goal_turn(event, prompt, label="goal-menu selection", kickoff=False) is True
    return bool(adapter._start_session_processing(turn, key))


async def send_menu(runner, source, session_id, menu) -> bool:
    adapter = runner._delivery_adapter_for(source)
    if adapter is None or not callable(getattr(type(adapter), "send_goal_recovery_menu", None)):
        return False
    home = runner._resolve_profile_home_for_source(source)
    if home is None:
        return False
    metadata = runner._thread_metadata_for_source(source)

    def audit(actor, token, action, outcome, reason):
        # One structured line per tap: consent (a tap) is not permission, so every decision records who and why.
        logger.info("goal_menu_tap outcome=%s reason=%s user=%s chat=%s thread=%s session=%s menu=%s action=%s",
                    outcome, reason, getattr(actor, "user_id", None), getattr(actor, "chat_id", None),
                    getattr(actor, "thread_id", None), session_id, token, action)

    async def selected(actor, token, action):
        if str(actor.chat_id) != str(source.chat_id) or str(actor.thread_id or "") != str(source.thread_id or ""):
            audit(actor, token, action, "denied", "wrong_origin")
            return GoalCommandResult("This menu belongs to another chat/topic. Use /goal unblock.", error=True)
        tapped_source = replace_source(source, user_id=str(actor.user_id), user_name=actor.user_name, is_bot=actor.is_bot)
        event = MessageEvent(text=f"/goal unblock {token} {action}", message_type=MessageType.TEXT, source=tapped_source)
        with _scope(home):
            if not runner._resume_caller_is_admin(tapped_source):
                audit(actor, token, action, "denied", "not_admin")
                return GoalCommandResult("Not authorized to unblock this goal.", error=True)
            mgr, _ = await runner._get_goal_manager_for_event(event)
            if mgr is None or mgr.session_id != session_id:
                audit(actor, token, action, "denied", "stale_session")
                return GoalCommandResult("This is a previous session's menu. Use /goal unblock in the current session.", error=True)
            try:
                result = await runner._run_in_executor_with_context(lambda: dispatch_goal_command(
                    mgr, f"unblock {token} {action}", authorize_gate=lambda: None))
            except (RuntimeError, ValueError) as exc:
                audit(actor, token, action, "rejected", type(exc).__name__)
                raise
            if result.clear_pending:
                runner._clear_goal_continuations(event, result.clear_pending)
            if result.prompt and not schedule_continuation(runner, event, result.prompt):
                from hermes_cli.goal_unblock import hold_after_admission_failure
                audit(actor, token, action, "held", "admission_failed")
                return await runner._run_in_executor_with_context(lambda: hold_after_admission_failure(mgr))
            audit(actor, token, action, "rejected" if result.error else "applied", "command_error" if result.error else "ok")
            return result

    result = await adapter.send_goal_recovery_menu(source.chat_id, menu, selected, metadata=metadata)
    return bool(result and result.success)

"""Real Telegram rendering with a fake transport; callback authorization and identity are mandatory."""
from types import SimpleNamespace
from unittest.mock import AsyncMock
import time

import pytest


@pytest.fixture
def adapter(monkeypatch):
    import importlib
    module = importlib.import_module("plugins.platforms.telegram.adapter")
    from plugins.platforms.telegram.adapter import TelegramAdapter
    monkeypatch.setattr(module, "InlineKeyboardButton", lambda text, callback_data: SimpleNamespace(text=text, callback_data=callback_data))
    monkeypatch.setattr(module, "InlineKeyboardMarkup", lambda rows: SimpleNamespace(inline_keyboard=rows))
    instance = object.__new__(TelegramAdapter)
    instance._reply_to_mode = "auto"
    instance._callback_authorized = AsyncMock(return_value=True)
    instance.send = AsyncMock(return_value=SimpleNamespace(success=True))
    instance._effective_message_thread_id = lambda message: str(message.message_thread_id) if message.message_thread_id is not None else None
    instance.build_source = lambda **kw: SimpleNamespace(**kw)
    return instance


def menu():
    return dict(id="a"*16, text="Goal paused: browser verification. Risk: low.", expires_at=time.time()+3600,
                choices=[dict(id="resume", label="Retry approved work"), dict(id="hold", label="Keep paused")])


def query(card, *, chat="123", thread="7", message=40, actor="11", action="resume"):
    return SimpleNamespace(data=f"gu:{card['id']}:{action}", from_user=SimpleNamespace(id=actor, first_name="User", is_bot=False),
        message=SimpleNamespace(chat_id=chat, message_id=message, message_thread_id=thread,
                                chat=SimpleNamespace(id=chat, title="room", type="supergroup", is_forum=True)),
        answer=AsyncMock(), edit_message_text=AsyncMock(), edit_message_reply_markup=AsyncMock())


async def send(adapter, card, callback=None):
    captured = {}
    async def prompt(name, chat_id, metadata, build, **kwargs):
        text, keyboard, remember = build()
        captured.update(text=text, keyboard=keyboard, kwargs=kwargs)
        remember(SimpleNamespace(message_id=40))
        return SimpleNamespace(success=True, message_id="40")
    adapter._send_prompt = prompt
    callback = callback or AsyncMock(return_value=SimpleNamespace(error=False, output="Goal resumed", prompt="continue"))
    result = await adapter.send_goal_recovery_menu("123", card, callback, metadata={"thread_id": "7"})
    assert result.success
    return captured, callback


@pytest.mark.asyncio
async def test_native_keyboard_labels_and_data_are_bounded(adapter):
    assert hasattr(adapter, "send_goal_recovery_menu"), "native goal menu not implemented"
    card = menu(); capture, _ = await send(adapter, card)
    buttons = [b for row in capture["keyboard"].inline_keyboard for b in row]
    assert buttons[0].text == "Retry approved work"
    assert buttons[0].callback_data == f"gu:{card['id']}:resume"
    assert all(len(b.callback_data.encode()) <= 64 for b in buttons)
    assert capture["kwargs"]["thread_id"] == "7"


@pytest.mark.asyncio
async def test_click_uses_actual_user_and_origin_topic_and_reports_success(adapter):
    card = menu(); _, selected = await send(adapter, card)
    q = query(card)
    await adapter._handle_goal_recovery_callback(q, q.data, {})
    args = selected.await_args.args
    assert args[0].user_id == "11" and args[0].thread_id == "7"
    assert args[1:] == (card["id"], "resume")
    assert q.edit_message_reply_markup.await_count == 1


@pytest.mark.asyncio
async def test_unauthorized_click_leaves_owner_menu_intact(adapter):
    card = menu(); _, selected = await send(adapter, card)
    adapter._callback_authorized.return_value = False
    q = query(card)
    await adapter._handle_goal_recovery_callback(q, q.data, {})
    selected.assert_not_awaited()
    q.edit_message_reply_markup.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changes", [{"chat": "other"}, {"thread": "other"}, {"message": 99}, {"action": "not-offered"}])
async def test_wrong_identity_or_action_is_not_applied(adapter, changes):
    card = menu(); _, selected = await send(adapter, card)
    q = query(card, **changes)
    await adapter._handle_goal_recovery_callback(q, q.data, {})
    selected.assert_not_awaited()
    assert q.answer.await_count


@pytest.mark.asyncio
async def test_expired_menu_and_restart_are_safe(adapter):
    card = menu(); _, selected = await send(adapter, card)
    card["expires_at"] = time.time()-1
    q = query(card)
    await adapter._handle_goal_recovery_callback(q, q.data, {})
    selected.assert_not_awaited()
    adapter._goal_recovery_menus.clear()
    await adapter._handle_goal_recovery_callback(q, q.data, {})
    selected.assert_not_awaited()


@pytest.mark.asyncio
async def test_rejected_action_does_not_claim_success_or_remove_buttons(adapter):
    card = menu()
    callback = AsyncMock(return_value=SimpleNamespace(error=True, output="Not authorized", prompt=None))
    _, _ = await send(adapter, card, callback)
    q = query(card)
    await adapter._handle_goal_recovery_callback(q, q.data, {})
    q.edit_message_reply_markup.assert_not_awaited()
    assert "Not authorized" in q.answer.await_args.kwargs["text"]


@pytest.mark.asyncio
async def test_second_click_cannot_execute_twice(adapter):
    card = menu(); _, selected = await send(adapter, card)
    q = query(card)
    await adapter._handle_goal_recovery_callback(q, q.data, {})
    await adapter._handle_goal_recovery_callback(q, q.data, {})
    assert selected.await_count == 1

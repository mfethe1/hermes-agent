"""A Telegram flood deadline applies to sends and edits for the full server wait."""

import asyncio
from unittest.mock import AsyncMock, MagicMock

import pytest
from telegram.error import RetryAfter

from gateway.config import PlatformConfig
from plugins.platforms.telegram.adapter import TelegramAdapter


def _adapter():
    adapter = TelegramAdapter(PlatformConfig(enabled=True, token="test-token"))
    adapter._rich_send_disabled = True
    adapter._bot = MagicMock()
    adapter._bot.send_message = AsyncMock(return_value=MagicMock(message_id=43))
    adapter._bot.edit_message_text = AsyncMock(return_value=MagicMock())
    return adapter


@pytest.mark.asyncio
async def test_long_server_wait_is_not_capped_to_five_minutes(monkeypatch):
    adapter = _adapter()
    loop = asyncio.get_running_loop()
    clock = [loop.time()]
    monkeypatch.setattr(loop, "time", lambda: clock[0])
    adapter._record_send_flood_cooldown(123, 9000)
    clock[0] += 301

    result = await adapter.send("123", "still waiting")

    assert result.success is False
    assert result.retry_after == pytest.approx(8699)
    adapter._bot.send_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_shorter_refusal_cannot_shorten_an_existing_flood_deadline():
    adapter = _adapter()
    adapter._record_send_flood_cooldown("123", 9000)
    adapter._record_send_flood_cooldown(123, 30)

    assert adapter._send_flood_cooldown_remaining("123") > 8990


@pytest.mark.asyncio
async def test_rate_limited_final_edit_does_not_retry_plain_or_hammer_the_chat():
    adapter = _adapter()
    adapter._bot.edit_message_text.side_effect = RetryAfter(9000)

    failed = await adapter.edit_message("123", "42", "final reply", finalize=True)

    assert failed.success is False and failed.retry_after == 9000
    assert adapter._bot.edit_message_text.await_count == 1
    for finalize in (False, True):
        blocked = await adapter.edit_message("123", "42", "later reply", finalize=finalize)
        assert blocked.success is False and blocked.retry_after > 8990
    blocked_send = await adapter.send("123", "another reply")
    assert blocked_send.success is False and blocked_send.retry_after > 8990
    adapter._bot.send_message.assert_not_awaited()
    assert adapter._bot.edit_message_text.await_count == 1

    other_chat = await adapter.send("456", "unaffected chat")
    assert other_chat.success is True
    adapter._bot.send_message.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("finalize", [False, True])
async def test_existing_send_flood_blocks_edits_until_expiry(monkeypatch, finalize):
    adapter = _adapter()
    loop = asyncio.get_running_loop()
    clock = [loop.time()]
    monkeypatch.setattr(loop, "time", lambda: clock[0])
    adapter._record_send_flood_cooldown("123", 9000)

    blocked = await adapter.edit_message("123", "42", "reply", finalize=finalize)

    assert blocked.success is False and blocked.retry_after == pytest.approx(9000)
    adapter._bot.edit_message_text.assert_not_awaited()
    clock[0] += 9001
    recovered = await adapter.edit_message("123", "42", "reply", finalize=finalize)
    assert recovered.success is True
    adapter._bot.edit_message_text.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["send", "edit"])
async def test_rich_rate_limit_arms_the_same_typed_chat_cooldown(operation):
    adapter = _adapter()
    adapter._rich_send_disabled = False
    adapter._rich_messages_enabled = True
    adapter._bot.do_api_request = AsyncMock(side_effect=RetryAfter(9000))
    content = "| Name | Value |\n| --- | --- |\n| Answer | 42 |"

    if operation == "send":
        result = await adapter.send("123", content)
    else:
        result = await adapter.edit_message("123", "42", content, finalize=True)

    assert result.success is False and result.retry_after == 9000
    assert result.error == "flood_control:9000.0"
    assert adapter._send_flood_cooldown_remaining("123") > 8990
    adapter._bot.do_api_request.assert_awaited_once()
    blocked = await adapter.edit_message("123", "42", "plain final", finalize=True)
    assert blocked.success is False and blocked.retry_after > 8990
    adapter._bot.edit_message_text.assert_not_awaited()


@pytest.mark.asyncio
async def test_typing_does_not_call_telegram_during_a_send_flood():
    adapter = _adapter()
    adapter._bot.send_chat_action = AsyncMock()
    adapter._record_send_flood_cooldown("123", 9000)

    await adapter.send_typing("123")

    adapter._bot.send_chat_action.assert_not_awaited()


@pytest.mark.asyncio
async def test_topic_creation_preserves_flood_refusal_and_stops_retrying():
    adapter = _adapter()
    adapter._bot.create_forum_topic = AsyncMock(side_effect=RetryAfter(9000))

    with pytest.raises(RuntimeError, match="flood_control:9000"):
        await adapter._create_dm_topic(123, "Reports")
    assert adapter._send_flood_cooldown_remaining("123") > 8990
    with pytest.raises(RuntimeError, match="flood_control:"):
        await adapter._create_dm_topic(123, "Another report")
    adapter._bot.create_forum_topic.assert_awaited_once()

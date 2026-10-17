"""Standalone Telegram sends must not hold a cron worker through an hours-long flood."""

from unittest.mock import AsyncMock, MagicMock

import pytest
from tools.send_message_senders import _send_telegram_message_with_retry


class RetryAfter(Exception):
    def __init__(self, seconds):
        super().__init__(f"Flood control exceeded. Retry in {seconds} seconds")
        self.retry_after = seconds


@pytest.mark.asyncio
async def test_long_flood_returns_the_server_refusal_without_sleep(monkeypatch):
    bot = MagicMock()
    bot.send_message = AsyncMock(side_effect=RetryAfter(9000))
    sleep = AsyncMock()
    monkeypatch.setattr("tools.send_message_senders.asyncio.sleep", sleep)

    with pytest.raises(RetryAfter) as caught:
        await _send_telegram_message_with_retry(bot, chat_id="123", text="reply")

    assert caught.value.retry_after == 9000
    bot.send_message.assert_awaited_once()
    sleep.assert_not_awaited()


@pytest.mark.asyncio
async def test_short_flood_keeps_bounded_inline_retry(monkeypatch):
    bot = MagicMock()
    message = MagicMock(message_id=42)
    bot.send_message = AsyncMock(side_effect=[RetryAfter(2), message])
    sleep = AsyncMock()
    monkeypatch.setattr("tools.send_message_senders.asyncio.sleep", sleep)

    assert await _send_telegram_message_with_retry(bot, chat_id="123", text="reply") is message
    assert bot.send_message.await_count == 2
    sleep.assert_awaited_once_with(2)

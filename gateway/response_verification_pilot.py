"""Unregistered, local sink adapter exercising Hermes' actual final-text entrypoint.

Explicit construction only; never loaded by platform discovery or configuration.
One object owns one attempt and one dispatch identity for its entire lifetime.
"""

import asyncio

from gateway.config import Platform, PlatformConfig
from gateway.platforms.base import BasePlatformAdapter, SendResult
from response_verification.pilot import DENIED


class IsolatedPilotAdapter(BasePlatformAdapter):
    def __init__(self, attempt, sink, chat_id="isolated-pilot"):
        super().__init__(
            PlatformConfig(enabled=False, typing_indicator=False), Platform.LOCAL
        )
        self.attempt = attempt
        self.sink = sink
        self.chat_id = chat_id
        self._dispatch_lock = asyncio.Lock()
        self._dispatched = False
        self._sending = False

    async def connect(self, *, is_reconnect=False):
        return False

    async def disconnect(self):
        self.attempt.cancel()

    async def get_chat_info(self, chat_id):
        return {"name": "isolated pilot", "type": "dm"}

    async def send_draft(self, chat_id, draft_id, content, metadata=None):
        if chat_id != self.chat_id:
            raise ValueError("outside pilot scope")
        self.attempt.buffer(content)

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        if chat_id != self.chat_id or not self._sending:
            raise ValueError("outside controlled dispatch")
        # No await between the boundary clock check and the synchronous test sink.
        payload = self.attempt.disposition()
        self.sink.append(payload.encode())
        return SendResult(success=True, message_id="isolated-dispatch")

    async def deliver(self, event, raw, draft=""):
        if event.source.chat_id != self.chat_id:
            raise ValueError("outside pilot scope")
        async with self._dispatch_lock:
            if self._dispatched:
                return
            self._dispatched = (
                True  # Consume before work, including cancellation/failure.
            )
            self.attempt.buffer(draft)
            await self.attempt.prepare(raw)
            self._sending = True
            try:
                # Production entrypoint, real retry/transport chain; only fixed text enters it.
                await self._send_final_text(
                    event, "isolated-pilot", DENIED, {}, True, 0, lambda result: None
                )
            finally:
                self._sending = False

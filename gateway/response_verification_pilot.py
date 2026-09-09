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
        self._dispatched = True
        await self.attempt.cancel_and_join()

    def _final_delivery_adapter(self, source):
        # This unregistered pilot has no authority to adopt a runner or replacement.
        if self.gateway_runner is not None:
            self.attempt.cancel()
            raise ValueError("unsupported pilot owner/replacement relationship")
        return self

    async def get_chat_info(self, chat_id):
        return {"name": "isolated pilot", "type": "dm"}

    async def send_draft(self, chat_id, draft_id, content, metadata=None):
        if chat_id != self.chat_id:
            raise ValueError("outside pilot scope")
        self.attempt.buffer(content)

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        if chat_id != self.chat_id or not self._sending:
            raise ValueError("outside controlled dispatch")
        self._final_delivery_adapter(None)
        # No await between the boundary clock check and the synchronous test sink.
        payload = self.attempt.disposition()
        self.sink.append(payload.encode())
        return SendResult(success=True, message_id="isolated-dispatch")

    async def deliver(self, event, raw, draft="", *, record_delivery=None):
        if event.source.chat_id != self.chat_id:
            raise ValueError("outside pilot scope")
        async with self._dispatch_lock:
            if self._dispatched:
                return
            self._dispatched = (
                True  # Consume before work, including cancellation/failure.
            )
            try:
                self._final_delivery_adapter(event.source)
                self.attempt.buffer(draft)
                await self.attempt.prepare(raw)
                self._sending = True
                # Production entrypoint, real retry/transport chain; only fixed text enters it.
                await self._send_final_text(
                    event,
                    "isolated-pilot",
                    DENIED,
                    {},
                    True,
                    0,
                    record_delivery
                    if record_delivery is not None
                    else lambda result: None,
                )
            finally:
                self._sending = False
                self.attempt.close()


class OuterPilotLifecycle:
    """Explicit local owner; replacing an attempt joins its complete outer task.

    No runner registration or reconnect authority. Dispatch identities are consumed
    for this owner only; durable/cross-instance replay is deliberately unsupported.
    """

    def __init__(self):
        self.active = None
        self.consumed = set()
        self._lock = asyncio.Lock()

    async def start(self, attempt, sink, event, raw, handler):
        async with self._lock:
            identity = (attempt.context.task_id, attempt.context.attempt_id)
            if identity in self.consumed:
                raise ValueError("dispatch identity already consumed")
            if self.active is not None:
                await self.active.stop()
            self.consumed.add(identity)
            adapter = _OuterPilotAdapter(self, attempt, sink, event, raw, handler)
            self.active = adapter
            adapter._start_session_processing(event, "isolated-outer")
            adapter.outer_task = adapter._session_tasks["isolated-outer"]
            adapter.outer_guard = adapter._active_sessions["isolated-outer"]
            return adapter

    async def stop(self):
        async with self._lock:
            if self.active is not None:
                await self.active.stop()


class _OuterPilotAdapter(IsolatedPilotAdapter):
    def __init__(self, owner, attempt, sink, event, raw, handler):
        super().__init__(attempt, sink, event.source.chat_id)
        self.owner = owner
        self.selected_destination = (self, event.source.chat_id, sink)
        self.raw = raw
        self.handler = handler
        self.outer_task = self.outer_guard = self.consumer_task = self.consumer = None
        self.closed = False
        self.draft_seen = asyncio.Event()
        self._message_handler = self._handle_local

    def current(self):
        return self.owner.active is self and not self.closed

    def _final_delivery_adapter(self, source):
        adapter = super()._final_delivery_adapter(source)
        selected_adapter, selected_chat, selected_sink = self.selected_destination
        if (
            not self.current()
            or selected_adapter is not self
            or selected_chat != self.chat_id
            or selected_sink is not self.sink
        ):
            raise ValueError("obsolete pilot destination")
        return adapter

    def supports_draft_streaming(self, chat_type=None, metadata=None, chat_id=None):
        return True

    async def send_draft(self, chat_id, draft_id, content, metadata=None):
        if self.current():
            await super().send_draft(chat_id, draft_id, content, metadata)
            self.draft_seen.set()
        return SendResult(success=True)

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        if self._sending:
            return await super().send(chat_id, content, reply_to, metadata)
        # Consumer finals, cancellation edits and error notices remain local drafts.
        await self.send_draft(chat_id, 1, content, metadata)
        return SendResult(success=True)

    async def _send_final_text(
        self,
        event,
        session_key,
        text_content,
        metadata,
        is_ephemeral_response,
        ephemeral_ttl,
        record_delivery,
    ):
        if self._sending:
            return await super()._send_final_text(
                event,
                session_key,
                text_content,
                metadata,
                is_ephemeral_response,
                ephemeral_ttl,
                record_delivery,
            )
        if self.current():
            await self.deliver(event, self.raw, record_delivery=record_delivery)

    async def _handle_local(self, event):
        from gateway.platforms.base import EphemeralReply
        from gateway.stream_consumer import GatewayStreamConsumer, StreamConsumerConfig
        from response_verification.pilot_cleanup import settle_owned

        self.consumer = GatewayStreamConsumer(
            self,
            self.chat_id,
            StreamConsumerConfig(
                transport="draft", buffer_threshold=1, edit_interval=0
            ),
            run_still_current=self.current,
        )
        consumer = self.consumer
        self.consumer_task = asyncio.create_task(consumer.run())
        accepting = True

        def delta(text):
            if accepting and self.current():
                consumer.on_delta(text)

        try:
            final = await self.handler(event, delta)
            consumer.finish(final)
            await self.consumer_task
            return EphemeralReply(DENIED, ttl_seconds=0)
        finally:
            accepting = False
            self.consumer_task.cancel()
            await settle_owned(self.consumer_task)
            consumer._wake_flush_waiters()
            consumer._reset_message_state()
            consumer._think_buffer = ""
            consumer._delivered_commentary_texts.clear()
            consumer._delivered_segment_texts.clear()

    async def _process_message_background(self, event, session_key):
        try:
            await super()._process_message_background(event, session_key)
        finally:
            self.closed = True
            if not self.attempt.terminal:
                await self.attempt.cancel_and_join()
            self.attempt.close()

    async def stop(self):
        from response_verification.pilot_cleanup import settle_owned

        self.closed = True
        self._dispatched = True
        if self.outer_task is not None and not self.outer_task.done():
            # Mark before attempt.cancel(), which can also cancel the outer task.
            self._expected_cancelled_tasks.add(self.outer_task)
        self.attempt.cancel()
        if self.outer_task is not None:
            self.outer_task.cancel()
            await settle_owned(self.outer_task)
            # A task cancelled before entry cannot execute the base finally.
            # Reconcile only this task and its captured guard after joining.
            if self._session_tasks.get("isolated-outer") is self.outer_task:
                self._session_tasks.pop("isolated-outer")
                self._release_session_guard("isolated-outer", guard=self.outer_guard)
        await self.attempt.cancel_and_join()

    async def disconnect(self):
        await self.stop()

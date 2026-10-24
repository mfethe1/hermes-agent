"""Tests for the gateway delivery-obligation ledger (gateway/delivery_ledger.py).

State machine, dead-owner claiming, attempts cap, stale cutoff, retention,
id stability, and the startup redelivery sweep's contract:
- pending rows redeliver plainly (send never started, no dup risk)
- attempting/failed rows carry the recovered-reply marker (honest
  at-least-once; ambiguity is labeled, never silently resent)
- rows owned by a LIVE process are never claimed
- poison rows abandon at the attempts cap / stale cutoff
"""

import os
import sqlite3
import threading
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from gateway import delivery_ledger as dl


@pytest.fixture(autouse=True)
def _fresh_db(tmp_path, monkeypatch):
    """Isolated state.db per test (autouse HERMES_HOME isolation already
    redirects get_hermes_home; make the redirect explicit and per-test)."""
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setattr(dl, "_db_path", lambda: home / "state.db")
    yield


def _record(oid="ob-1", session_key="agent:main:slack:channel:C1", **kw):
    dl.record_obligation(
        obligation_id=oid,
        session_key=session_key,
        platform=kw.get("platform", "slack"),
        chat_id=kw.get("chat_id", "C1"),
        thread_id=kw.get("thread_id", "171.001"),
        content=kw.get("content", "the final answer"),
        adapter_profile=kw.get("adapter_profile"),
    )


def _row(oid):
    with dl._connect() as conn:
        r = conn.execute(
            """SELECT state, attempts, owner_pid, content, last_error
               FROM delivery_obligations WHERE obligation_id=?""",
            (oid,),
        ).fetchone()
    return None if r is None else {
        "state": r[0],
        "attempts": r[1],
        "owner_pid": r[2],
        "content": r[3],
        "last_error": r[4],
    }


def _blocking_probe():
    """Return a blocking ledger call and an event-loop progress witness."""
    ledger_started = threading.Event()
    event_loop_progressed = threading.Event()
    blocked_event_loop = []

    def _slow_ledger_call(*args, **kwargs):
        ledger_started.set()
        # Generous timeout: a genuinely blocked loop can never set the event
        # (the witness coroutine cannot run), so a longer wait only guards
        # against loaded-CI scheduling flake, not against missing the bug.
        if not event_loop_progressed.wait(timeout=5.0):
            blocked_event_loop.append(True)

    async def _event_loop_witness():
        import asyncio

        deadline = asyncio.get_running_loop().time() + 10
        while not ledger_started.is_set():
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError("ledger call never started")
            await asyncio.sleep(0)
        event_loop_progressed.set()

    return _slow_ledger_call, _event_loop_witness, blocked_event_loop


def _orphan(oid):
    """Make the row look like it belongs to a dead process."""
    with dl._connect() as conn:
        conn.execute(
            "UPDATE delivery_obligations SET owner_pid=999999999, "
            "owner_started_at=1 WHERE obligation_id=?",
            (oid,),
        )


def _age(oid, seconds):
    """Backdate the row's last state change by *seconds*."""
    with dl._connect() as conn:
        conn.execute(
            "UPDATE delivery_obligations SET updated_at=updated_at-? WHERE obligation_id=?",
            (seconds, oid),
        )


class TestSchemaMigration:
    def test_adds_adapter_profile_to_existing_ledger(self):
        conn = sqlite3.connect(dl._db_path())
        try:
            conn.execute(
                """CREATE TABLE delivery_obligations (
                    obligation_id TEXT PRIMARY KEY,
                    session_key TEXT NOT NULL,
                    platform TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    thread_id TEXT,
                    content TEXT NOT NULL,
                    state TEXT NOT NULL,
                    attempts INTEGER NOT NULL DEFAULT 0,
                    created_at REAL NOT NULL,
                    updated_at REAL NOT NULL,
                    owner_pid INTEGER,
                    owner_started_at INTEGER,
                    last_error TEXT
                )"""
            )
            dl._initialize_schema(conn)
            columns = {
                row[1] for row in conn.execute("PRAGMA table_info(delivery_obligations)")
            }
        finally:
            conn.close()

        assert "adapter_profile" in columns


class TestStateMachine:
    def test_record_starts_pending(self):
        _record()
        assert _row("ob-1")["state"] == "pending"


class TestObligationId:
    def test_stable_and_distinct(self):
        a = dl.compute_obligation_id("sk1", "msg1", "hello")
        assert a == dl.compute_obligation_id("sk1", "msg1", "hello")
        # Different thread (baked into session_key) → different id. This is
        # the cron-topic collision class from the earlier outbox attempt.
        assert a != dl.compute_obligation_id("sk1:threadB", "msg1", "hello")
        assert a != dl.compute_obligation_id("sk1", "msg2", "hello")
        assert a != dl.compute_obligation_id("sk1", "msg1", "other")
        assert len(a) == 24


class TestSweep:
    def test_live_owner_rows_never_claimed(self):
        _record()  # owner = this (live) process
        assert dl.sweep_recoverable() == []

    def test_dead_owner_pending_claimed_without_marker(self):
        _record()
        _orphan("ob-1")
        claimed = dl.sweep_recoverable()
        assert len(claimed) == 1
        assert claimed[0]["needs_marker"] is False
        assert claimed[0]["attempts"] == 1
        # Claim re-stamps ownership: a second sweep in the same (live)
        # process must not double-claim.
        assert dl.sweep_recoverable() == []


class TestRuntimeFailedSweep:
    """A live gateway may reclaim only its own transient reconnect failures."""

    def test_claims_current_process_send_path_degraded_row(self):
        _record(platform="telegram")
        dl.mark_failed("ob-1", "send_path_degraded")

        claimed = dl.sweep_failed_for_runtime("telegram")

        assert len(claimed) == 1
        assert claimed[0]["needs_marker"] is True
        assert claimed[0]["attempts"] == 1
        assert _row("ob-1")["state"] == "attempting"

    def test_permanent_failure_is_not_claimed(self):
        _record(platform="telegram")
        dl.mark_failed("ob-1", "Forbidden: bot was blocked by the user")

        assert dl.sweep_failed_for_runtime("telegram", now=time.time() + 10_000) == []
        assert _row("ob-1")["state"] == "failed"
        assert _row("ob-1")["attempts"] == 0

    def test_other_rejection_is_claimed_only_after_its_backoff(self):
        _record(platform="telegram")
        dl.mark_failed("ob-1", "503 Service Unavailable")
        with dl._connect() as conn:
            conn.execute("UPDATE delivery_obligations SET attempts=1, updated_at=1000.0 WHERE obligation_id='ob-1'")

        assert dl.sweep_failed_for_runtime("telegram", now=1000.0 + 60) == []
        assert dl.pending_retries(now=1000.0 + 60) == [
            {"platform": "telegram", "profile": "default", "not_before": 1000.0 + 120}]
        claimed = dl.sweep_failed_for_runtime("telegram", now=1000.0 + 121)
        assert [row["obligation_id"] for row in claimed] == ["ob-1"]
        assert claimed[0]["needs_marker"] is True
        assert _row("ob-1")["state"] == "attempting"

    def test_other_rejection_keeps_the_last_attempt_for_a_restart(self):
        """The in-process timer never spends the final budgeted attempt: an outage can outlast any
        backoff, and a row the timer abandoned would be lost for good (review on #91655)."""
        _record(platform="telegram")
        dl.mark_failed("ob-1", "503 Service Unavailable")
        with dl._connect() as conn:
            conn.execute("UPDATE delivery_obligations SET attempts=?, updated_at=1000.0 WHERE obligation_id='ob-1'",
                         (dl.MAX_ATTEMPTS - 1,))

        assert dl.sweep_failed_for_runtime("telegram", now=1000.0 + 10_000) == []
        assert dl.pending_retries(now=1000.0 + 10_000) == []
        assert _row("ob-1")["state"] == "failed"
        assert _row("ob-1")["attempts"] == dl.MAX_ATTEMPTS - 1

    def test_reconnect_only_row_is_never_timer_driven(self):
        """A claim released because the adapter was gone comes back as `send_path_degraded` with a
        fresh stamp; only the reconnect sweep may re-claim it, or a timer would loop every tick."""
        _record(platform="telegram")
        dl.mark_failed("ob-1", "send_path_degraded")

        assert dl.pending_retries(now=time.time() + 10_000) == []
        assert [r["obligation_id"] for r in dl.sweep_failed_for_runtime("telegram")] == ["ob-1"]

    def test_claim_is_platform_scoped_and_not_reclaimed_while_attempting(self):
        _record(platform="telegram")
        dl.mark_failed("ob-1", "send_path_degraded")
        _record(
            oid="ob-2",
            session_key="agent:main:slack:channel:C2",
            platform="slack",
            chat_id="C2",
        )
        dl.mark_failed("ob-2", "send_path_degraded")

        claimed = dl.sweep_failed_for_runtime("telegram")

        assert [row["obligation_id"] for row in claimed] == ["ob-1"]
        assert dl.sweep_failed_for_runtime("telegram") == []
        assert _row("ob-2")["state"] == "failed"
        assert _row("ob-2")["attempts"] == 0

    def test_other_live_owner_is_not_claimed_or_abandoned(self, monkeypatch):
        _record(platform="telegram")
        dl.mark_failed("ob-1", "send_path_degraded")
        with dl._connect() as conn:
            conn.execute(
                "UPDATE delivery_obligations SET owner_pid=?, "
                "owner_started_at=?, attempts=? WHERE obligation_id=?",
                (12345, 101, dl.MAX_ATTEMPTS, "ob-1"),
            )
        monkeypatch.setattr(dl, "_owner_stamp", lambda: (54321, 202))

        assert dl.sweep_failed_for_runtime("telegram") == []
        assert _row("ob-1")["state"] == "failed"
        assert _row("ob-1")["attempts"] == dl.MAX_ATTEMPTS

    def test_unowned_row_is_not_claimed(self):
        _record(platform="telegram")
        dl.mark_failed("ob-1", "send_path_degraded")
        with dl._connect() as conn:
            conn.execute(
                "UPDATE delivery_obligations SET owner_pid=NULL, "
                "owner_started_at=NULL WHERE obligation_id=?",
                ("ob-1",),
            )

        assert dl.sweep_failed_for_runtime("telegram") == []
        assert _row("ob-1")["state"] == "failed"

    def test_missing_current_process_start_stamp_fails_closed(self, monkeypatch):
        _record(platform="telegram")
        dl.mark_failed("ob-1", "send_path_degraded")
        with dl._connect() as conn:
            conn.execute(
                "UPDATE delivery_obligations SET owner_started_at=NULL "
                "WHERE obligation_id=?",
                ("ob-1",),
            )
        monkeypatch.setattr(dl, "_owner_stamp", lambda: (os.getpid(), None))

        assert dl.sweep_failed_for_runtime("telegram") == []
        assert _row("ob-1")["state"] == "failed"

    def test_same_pid_with_different_start_stamp_is_not_claimed(self, monkeypatch):
        _record(platform="telegram")
        dl.mark_failed("ob-1", "send_path_degraded")
        with dl._connect() as conn:
            conn.execute(
                "UPDATE delivery_obligations SET owner_pid=?, owner_started_at=? "
                "WHERE obligation_id=?",
                (os.getpid(), 101, "ob-1"),
            )
        monkeypatch.setattr(dl, "_owner_stamp", lambda: (os.getpid(), 202))

        assert dl.sweep_failed_for_runtime("telegram") == []
        assert _row("ob-1")["state"] == "failed"

    def test_profile_scope_never_claims_another_bot_identity(self):
        _record(platform="telegram")
        dl.mark_failed("ob-1", "send_path_degraded")
        _record(
            oid="ob-2",
            session_key="agent:reviewer:telegram:dm:C2",
            platform="telegram",
            chat_id="C2",
            adapter_profile="reviewer",
        )
        dl.mark_failed("ob-2", "send_path_degraded")

        claimed = dl.sweep_failed_for_runtime("telegram", profile="reviewer")

        assert [row["obligation_id"] for row in claimed] == ["ob-2"]
        assert claimed[0]["profile"] == "reviewer"
        assert _row("ob-1")["state"] == "failed"

    def test_current_owner_row_at_attempt_cap_is_abandoned(self):
        _record(platform="telegram")
        dl.mark_failed("ob-1", "send_path_degraded")
        with dl._connect() as conn:
            conn.execute(
                "UPDATE delivery_obligations SET attempts=? WHERE obligation_id=?",
                (dl.MAX_ATTEMPTS, "ob-1"),
            )

        assert dl.sweep_failed_for_runtime("telegram") == []
        assert _row("ob-1")["state"] == "abandoned"

    def test_delivered_row_is_never_reclaimed_by_reconnect_sweep(self):
        """Idempotency: once delivered, a reconnect sweep must not re-send.

        Strongest form: the row previously failed with the allowlisted
        transient error and is force-restamped with that ``last_error`` even
        after delivery, so the ONLY guard standing between the sweep and a
        duplicate send is the ``state='delivered'`` filter itself.
        """
        _record(platform="telegram")
        dl.mark_failed("ob-1", "send_path_degraded")

        # First reconnect legitimately claims and (successfully) redelivers.
        assert len(dl.sweep_failed_for_runtime("telegram")) == 1
        dl.mark_delivered("ob-1")
        # Simulate a mark_delivered that leaves the retryable error string
        # behind: even then, a delivered row must never be reclaimed.
        with dl._connect() as conn:
            conn.execute(
                "UPDATE delivery_obligations SET last_error=? "
                "WHERE obligation_id=?",
                ("send_path_degraded", "ob-1"),
            )

        assert dl.sweep_failed_for_runtime("telegram") == []
        row = _row("ob-1")
        assert row is not None
        assert row["state"] == "delivered"
        assert row["attempts"] == 1

    def test_current_owner_stale_row_is_abandoned(self):
        _record(platform="telegram")
        dl.mark_failed("ob-1", "send_path_degraded")
        now = time.time()
        with dl._connect() as conn:
            conn.execute(
                "UPDATE delivery_obligations SET created_at=? WHERE obligation_id=?",
                (now - dl.STALE_AFTER_SECONDS - 1, "ob-1"),
            )

        assert dl.sweep_failed_for_runtime("telegram", now=now) == []
        assert _row("ob-1")["state"] == "abandoned"

    def test_stale_attempting_row_is_reclaimed_with_the_reconnect_marker(self):
        """MSG-1 (2026-10-24, obligation eeebc7bb): a row this live process left in 'attempting' (its send
        was cancelled, or the delivered/failed write was lost) waited 78 minutes for the next restart,
        because the runtime sweep only took 'failed' rows. It is now reclaimed, marked."""
        _record(platform="telegram")
        dl.mark_attempting("ob-1")
        later = time.time() + dl.STALE_ATTEMPTING_SECONDS + 1

        claimed = dl.sweep_failed_for_runtime("telegram", now=later)

        assert [r["obligation_id"] for r in claimed] == ["ob-1"]
        assert claimed[0]["needs_marker"] is True
        assert claimed[0]["marker"] == dl.RECONNECTED_MARKER
        assert claimed[0]["attempts"] == 1
        assert _row("ob-1")["state"] == "attempting"
        # The claim re-stamps the row: a second sweep at the same moment cannot take it again.
        assert dl.sweep_failed_for_runtime("telegram", now=later) == []

    def test_recent_attempting_row_is_left_to_its_send(self):
        _record(platform="telegram")
        dl.mark_attempting("ob-1")

        assert dl.sweep_failed_for_runtime("telegram", now=time.time() + dl.STALE_ATTEMPTING_SECONDS - 5) == []
        assert _row("ob-1")["attempts"] == 0

    def test_attempting_row_whose_send_is_in_flight_is_never_reclaimed(self):
        """A slow send is not a stuck one: while this process is still sending the row, no sweep may
        start a second copy of it, however old the row is."""
        _record(platform="telegram")
        dl.mark_attempting("ob-1")
        later = time.time() + 10 * dl.STALE_ATTEMPTING_SECONDS

        with dl.sending(["ob-1"]):
            assert dl.sweep_failed_for_runtime("telegram", now=later) == []
        assert _row("ob-1")["attempts"] == 0
        assert [r["obligation_id"] for r in dl.sweep_failed_for_runtime("telegram", now=later)] == ["ob-1"]

    def test_nested_in_flight_registration_holds_until_the_outer_block_exits(self):
        """Registrations nest (a send's finalize triggers a reconnect redelivery of the same row; the boot
        task wraps its own redelivery): an inner exit must not unregister a row the outer still holds."""
        _record(platform="telegram")
        dl.mark_attempting("ob-1")
        later = time.time() + 10 * dl.STALE_ATTEMPTING_SECONDS

        with dl.sending(["ob-1"]):
            with dl.sending(["ob-1"]):
                pass
            assert dl.sweep_failed_for_runtime("telegram", now=later) == []
        assert [r["obligation_id"] for r in dl.sweep_failed_for_runtime("telegram", now=later)] == ["ob-1"]

    def test_stale_attempting_row_of_another_process_is_not_reclaimed(self, monkeypatch):
        _record(platform="telegram")
        dl.mark_attempting("ob-1")
        monkeypatch.setattr(dl, "_owner_stamp", lambda: (54321, 202))

        assert dl.sweep_failed_for_runtime("telegram", now=time.time() + 10 * dl.STALE_ATTEMPTING_SECONDS) == []
        assert _row("ob-1")["state"] == "attempting"
        assert _row("ob-1")["attempts"] == 0

    def test_stale_attempting_row_at_the_attempt_cap_is_abandoned(self):
        _record(platform="telegram")
        dl.mark_attempting("ob-1")
        with dl._connect() as conn:
            conn.execute("UPDATE delivery_obligations SET attempts=? WHERE obligation_id='ob-1'", (dl.MAX_ATTEMPTS,))

        assert dl.sweep_failed_for_runtime("telegram", now=time.time() + 10 * dl.STALE_ATTEMPTING_SECONDS) == []
        assert _row("ob-1")["state"] == "abandoned"


class TestPrune:
    def test_old_delivered_rows_pruned(self):
        _record()
        dl.mark_delivered("ob-1")
        with dl._connect() as conn:
            conn.execute(
                "UPDATE delivery_obligations SET updated_at=? WHERE obligation_id=?",
                (time.time() - dl._RETENTION_SECONDS - 60, "ob-1"),
            )
        # Prune has no wrapper of its own: it runs inside a writer's transaction, lock held.
        with dl._DB_LOCK, dl._transaction() as conn:
            dl._prune_unlocked(conn, time.time())
        assert _row("ob-1") is None




class TestGatewayRedeliverySweep:
    """Drive the real GatewayRunner._redeliver_pending_obligations."""

    @staticmethod
    def _runner(adapter=None):
        from gateway.config import Platform
        from gateway.run import GatewayRunner

        runner = object.__new__(GatewayRunner)
        runner.adapters = {Platform.SLACK: adapter} if adapter else {}
        runner._profile_adapters = {}
        runner._active_profile_name = lambda: "default"
        _store = MagicMock()
        _store.clear_resume_pending = AsyncMock()
        _store._store = None
        runner.session_store = None
        runner._async_session_store = _store
        return runner

    @staticmethod
    def _adapter(success=True):
        adapter = MagicMock()
        adapter.send = AsyncMock(
            return_value=MagicMock(success=success, error="" if success else "nope")
        )
        return adapter

    @pytest.mark.asyncio
    async def test_pending_redelivers_plain_and_clears_resume(self):
        _record()  # pending
        _orphan("ob-1")
        adapter = self._adapter()
        runner = self._runner(adapter)

        n = await runner._redeliver_pending_obligations()

        assert n == 1
        sent = adapter.send.call_args.kwargs
        assert sent["content"] == "the final answer"  # no marker
        assert sent["metadata"] == {"thread_id": "171.001"}
        assert _row("ob-1")["state"] == "delivered"
        runner._async_session_store.clear_resume_pending.assert_awaited_once_with(
            "agent:main:slack:channel:C1"
        )

    @pytest.mark.asyncio
    async def test_startup_redelivery_uses_persisted_transport_owner(self):
        from gateway.config import Platform

        _record(
            session_key="agent:routed-profile:slack:channel:C1",
            adapter_profile="credential-owner",
        )
        _orphan("ob-1")
        default_adapter = self._adapter()
        owner_adapter = self._adapter()
        runner = self._runner(default_adapter)
        runner._profile_adapters = {
            "credential-owner": {Platform.SLACK: owner_adapter}
        }

        n = await runner._redeliver_pending_obligations()

        assert n == 1
        default_adapter.send.assert_not_awaited()
        owner_adapter.send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_startup_does_not_claim_disconnected_transport_owner(self):
        _record(
            session_key="agent:routed-profile:slack:channel:C1",
            adapter_profile="credential-owner",
        )
        _orphan("ob-1")
        default_adapter = self._adapter()
        runner = self._runner(default_adapter)

        n = await runner._redeliver_pending_obligations()

        assert n == 0
        default_adapter.send.assert_not_awaited()
        assert _row("ob-1")["state"] == "pending"
        assert _row("ob-1")["attempts"] == 0

    @pytest.mark.asyncio
    async def test_attempting_redelivers_with_marker(self):
        _record()
        dl.mark_attempting("ob-1")
        _orphan("ob-1")
        adapter = self._adapter()
        runner = self._runner(adapter)

        await runner._redeliver_pending_obligations()

        sent = adapter.send.call_args.kwargs
        assert sent["content"].startswith(dl.RECOVERED_MARKER)
        assert sent["content"].endswith("the final answer")

    @pytest.mark.parametrize("earlier_boot", ["killed_inside_send", "older_build_left_pending"])
    @pytest.mark.asyncio
    async def test_redelivery_after_an_earlier_boot_claim_is_marked(self, earlier_boot):
        """Once a boot has claimed a row it may have sent it: every later copy carries the marker."""
        import asyncio

        _record()
        _orphan("ob-1")
        if earlier_boot == "killed_inside_send":
            # The platform accepts the plain resend, then the boot dies before mark_delivered.
            first = MagicMock()
            first.send = AsyncMock(side_effect=asyncio.CancelledError)
            with pytest.raises(asyncio.CancelledError):
                await self._runner(first)._redeliver_pending_obligations()
            assert first.send.call_args.kwargs["content"] == "the final answer"
        else:
            with dl._connect() as conn:
                conn.execute("UPDATE delivery_obligations SET attempts=1 WHERE obligation_id='ob-1'")
        _orphan("ob-1")
        second = self._adapter()

        await self._runner(second)._redeliver_pending_obligations()

        sent = second.send.call_args.kwargs["content"]
        assert sent == dl.RECOVERED_MARKER + "the final answer"
        assert _row("ob-1")["state"] == "delivered"

    def test_boot_claimed_row_is_not_reclaimed_by_runtime_sweep_mid_send(self):
        """A boot claim is exclusive while its send is in flight: the reconnect sweep must not resend it."""
        _record(platform="telegram")
        dl.mark_failed("ob-1", "send_path_degraded")
        _orphan("ob-1")

        assert [r["obligation_id"] for r in dl.sweep_recoverable()] == ["ob-1"]

        assert dl.sweep_failed_for_runtime("telegram") == []

    @pytest.mark.parametrize("claimed_state", ["pending", "failed"])
    @pytest.mark.asyncio
    async def test_boot_claim_whose_adapter_vanishes_is_left_for_the_reconnect_sweep(self, claimed_state):
        """A boot claim never sent (platform went away before dispatch) must not strand in 'attempting'."""
        _record()
        if claimed_state == "failed":
            dl.mark_failed("ob-1", "send_path_degraded")
        _orphan("ob-1")
        runner = self._runner(self._adapter())
        claimed = await runner._claim_pending_obligations()
        assert [r["obligation_id"] for r in claimed] == ["ob-1"]
        runner.adapters = {}  # platform went fatal during the restart notification / flood sleep

        await runner._redeliver_claimed_obligations(claimed)

        assert _row("ob-1")["state"] == "failed" and _row("ob-1")["attempts"] == 0
        assert [r["obligation_id"] for r in dl.sweep_failed_for_runtime("slack")] == ["ob-1"]

    @pytest.mark.asyncio
    async def test_runtime_failed_redelivery_clears_resume_before_send(self):
        from gateway.config import Platform

        _record(platform="slack")
        dl.mark_failed("ob-1", "send_path_degraded")
        adapter = self._adapter()
        runner = self._runner(adapter)

        n = await runner._redeliver_failed_obligations_for_platform(Platform.SLACK)

        assert n == 1
        runner._async_session_store.clear_resume_pending.assert_awaited_once_with(
            "agent:main:slack:channel:C1"
        )
        assert adapter.send.await_count == 1
        assert adapter.send.call_args.kwargs["content"].startswith(
            dl.RECONNECTED_MARKER
        )
        assert _row("ob-1")["state"] == "delivered"

    @pytest.mark.asyncio
    async def test_runtime_profile_redelivery_uses_matching_bot_adapter(self):
        from gateway.config import Platform

        _record(
            session_key="agent:reviewer:slack:channel:C1",
            platform="slack",
            adapter_profile="reviewer",
        )
        dl.mark_failed("ob-1", "send_path_degraded")
        default_adapter = self._adapter()
        reviewer_adapter = self._adapter()
        runner = self._runner(default_adapter)
        runner._profile_adapters = {
            "reviewer": {Platform.SLACK: reviewer_adapter}
        }

        n = await runner._redeliver_failed_obligations_for_platform(
            Platform.SLACK, profile="reviewer"
        )

        assert n == 1
        default_adapter.send.assert_not_awaited()
        reviewer_adapter.send.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_runtime_missing_adapter_releases_unsent_claim(self):
        from gateway.config import Platform

        _record(platform="slack")
        dl.mark_failed("ob-1", "send_path_degraded")
        runner = self._runner()

        n = await runner._redeliver_failed_obligations_for_platform(Platform.SLACK)

        assert n == 0
        assert _row("ob-1")["state"] == "failed"
        assert _row("ob-1")["attempts"] == 0
        assert _row("ob-1")["last_error"] == "send_path_degraded"

    @pytest.mark.asyncio
    async def test_runtime_clear_failure_does_not_send_or_lose_retry(self):
        from gateway.config import Platform

        _record(platform="slack")
        dl.mark_failed("ob-1", "send_path_degraded")
        adapter = self._adapter()
        runner = self._runner(adapter)
        runner._async_session_store.clear_resume_pending.side_effect = RuntimeError(
            "session store unavailable"
        )

        n = await runner._redeliver_failed_obligations_for_platform(Platform.SLACK)

        assert n == 0
        adapter.send.assert_not_awaited()
        assert _row("ob-1")["state"] == "failed"
        assert _row("ob-1")["attempts"] == 0
        assert _row("ob-1")["last_error"] == "send_path_degraded"

    @pytest.mark.parametrize(
        ("send_success", "ledger_method"),
        [(True, "mark_delivered"), (False, "mark_failed")],
    )
    @pytest.mark.asyncio
    async def test_slow_state_update_does_not_block_event_loop(
        self, send_success, ledger_method
    ):
        import asyncio

        _record()
        _orphan("ob-1")
        runner = self._runner(self._adapter(success=send_success))
        slow_update, event_loop_witness, blocked_event_loop = _blocking_probe()

        with patch.object(dl, ledger_method, side_effect=slow_update):
            await asyncio.gather(
                runner._redeliver_pending_obligations(), event_loop_witness()
            )

        assert blocked_event_loop == []

    @pytest.mark.asyncio
    async def test_clear_resume_pending_before_send_so_a_hang_cannot_also_resume(
        self,
    ):
        """A hung redelivery send must still clear resume_pending.

        Otherwise a timed-out startup-restore gate would schedule resume and
        replay a turn whose answer is already in the ledger (#91969).
        """
        import asyncio

        _record()
        _orphan("ob-1")
        hang = asyncio.Event()

        async def hanging_send(**_kwargs):
            await hang.wait()
            return MagicMock(success=True, error="")

        adapter = MagicMock()
        adapter.send = hanging_send
        runner = self._runner(adapter)
        task = asyncio.create_task(runner._redeliver_pending_obligations())

        deadline = asyncio.get_running_loop().time() + 2
        while runner._async_session_store.clear_resume_pending.await_count == 0:
            if asyncio.get_running_loop().time() >= deadline:
                raise AssertionError("resume_pending was not cleared before send")
            await asyncio.sleep(0)

        runner._async_session_store.clear_resume_pending.assert_awaited_once_with(
            "agent:main:slack:channel:C1"
        )
        assert not task.done()

        hang.set()
        assert await task == 1

    @staticmethod
    def _refusing_adapter(error="send_path_degraded"):
        adapter = MagicMock()
        adapter.send = AsyncMock(return_value=MagicMock(success=False, error=error))
        return adapter

    @pytest.mark.asyncio
    async def test_runtime_degraded_adapter_releases_claim_without_spending_an_attempt(self):
        """A reconnect in degraded mode (Telegram's receive path not yet proven) installs an adapter whose
        send() refuses every message with ``send_path_degraded`` without touching the network. Sending
        the claim there spent an attempt on a no-op and left the row reconnect-only with no later sweep."""
        from gateway.config import Platform

        _record(platform="slack")
        dl.mark_failed("ob-1", "send_path_degraded")
        adapter = self._refusing_adapter()
        adapter.send_path_degraded = True
        runner = self._runner(adapter)

        n = await runner._redeliver_failed_obligations_for_platform(Platform.SLACK)

        assert n == 0
        adapter.send.assert_not_awaited()
        assert _row("ob-1")["state"] == "failed"
        assert _row("ob-1")["attempts"] == 0
        assert _row("ob-1")["last_error"] == "send_path_degraded"

    @pytest.mark.asyncio
    async def test_reply_survives_an_outage_and_a_degraded_reconnect(self):
        """MSG-1 end to end (2026-10-23, obligation 2b168c6d): the final reply fails 'Not connected' while
        Telegram is down, the backoff timer finds no adapter, Telegram comes back degraded at 09:48, and
        only later proves its send path. The reply goes out then, marked, on its first attempt, instead
        of ten hours later at the next restart."""
        from gateway.config import Platform

        _record(platform="telegram", chat_id="T1")
        dl.mark_failed("ob-1", "Not connected")
        runner = self._runner()

        # The 30 s backoff timer fires while the fatal-error handler has removed the adapter.
        _age("ob-1", 31)
        assert await runner._redeliver_failed_obligations_for_platform(Platform.TELEGRAM) == 0
        assert _row("ob-1")["state"] == "failed"
        assert _row("ob-1")["attempts"] == 0
        assert _row("ob-1")["last_error"] == "send_path_degraded"

        # Degraded reconnect: the adapter is back but cannot deliver yet.
        live = self._refusing_adapter()
        live.send_path_degraded = True
        runner.adapters = {Platform.TELEGRAM: live}
        assert await runner._redeliver_failed_obligations_for_platform(Platform.TELEGRAM) == 0
        live.send.assert_not_awaited()
        assert _row("ob-1")["attempts"] == 0

        # The send path is confirmed and the adapter's recovery hook sweeps again.
        live.send_path_degraded = False
        live.send = AsyncMock(return_value=MagicMock(success=True, error=""))
        assert await runner._redeliver_failed_obligations_for_platform(Platform.TELEGRAM) == 1
        assert live.send.call_args.kwargs["content"] == dl.RECONNECTED_MARKER + "the final answer"
        assert _row("ob-1")["state"] == "delivered"
        assert _row("ob-1")["attempts"] == 1

    @pytest.mark.asyncio
    async def test_reconnect_redelivers_a_stale_attempting_row(self):
        from gateway.config import Platform

        _record(platform="slack")
        dl.mark_attempting("ob-1")
        _age("ob-1", dl.STALE_ATTEMPTING_SECONDS + 1)
        adapter = self._adapter()
        runner = self._runner(adapter)

        n = await runner._redeliver_failed_obligations_for_platform(Platform.SLACK)

        assert n == 1
        assert adapter.send.call_args.kwargs["content"] == dl.RECONNECTED_MARKER + "the final answer"
        assert _row("ob-1")["state"] == "delivered"

    @pytest.mark.asyncio
    async def test_reconnect_arms_a_timer_for_rows_not_yet_due(self):
        """A reconnect that finds only rows still inside their backoff must leave them on a timer, not
        assume one is armed (a timer task that died would strand them until the next restart)."""
        from gateway.config import Platform

        _record(platform="slack")
        dl.mark_failed("ob-1", "503 Service Unavailable")
        adapter = self._adapter()
        runner = self._runner(adapter)
        runner._schedule_flood_redelivery = MagicMock()

        n = await runner._redeliver_failed_obligations_for_platform(Platform.SLACK)

        assert n == 0
        adapter.send.assert_not_awaited()
        runner._schedule_flood_redelivery.assert_called_once_with("slack", profile="default")

    @pytest.mark.asyncio
    async def test_redelivery_in_flight_is_not_reclaimed_by_an_overlapping_sweep(self):
        """Idempotency: a redelivery still sending owns its row, so a reconnect sweep that lands
        meanwhile cannot send a second copy, however long the first send takes."""
        import asyncio

        _record()
        _orphan("ob-1")
        started, hang = asyncio.Event(), asyncio.Event()

        async def hanging_send(**_kwargs):
            started.set()
            await hang.wait()
            return MagicMock(success=True, error="")

        adapter = MagicMock()
        adapter.send = hanging_send
        task = asyncio.create_task(self._runner(adapter)._redeliver_pending_obligations())
        await asyncio.wait_for(started.wait(), timeout=5)

        assert dl.sweep_failed_for_runtime("slack", now=time.time() + 10 * dl.STALE_ATTEMPTING_SECONDS) == []

        hang.set()
        assert await task == 1
        assert _row("ob-1")["state"] == "delivered"
        assert _row("ob-1")["attempts"] == 1

    @pytest.mark.asyncio
    async def test_boot_claims_waiting_behind_the_restart_notice_are_not_reclaimed(self, monkeypatch):
        """Boot claims every recoverable row first, then sends the restart notification (a flood wait can
        stretch it for minutes), then the rows. A reconnect sweep in that gap must not send them too, or
        the boot loop would deliver each one a second time."""
        import asyncio

        monkeypatch.setenv("HERMES_STARTUP_RESTORE_DRAIN_TIMEOUT", "0")
        _record()
        _orphan("ob-1")
        adapter = self._adapter()
        runner = self._runner(adapter)
        notice_started, hang = asyncio.Event(), asyncio.Event()

        async def slow_notice():
            notice_started.set()
            await hang.wait()

        runner._send_restart_notification = slow_notice
        task = asyncio.create_task(runner._await_startup_boot_sends(planned_restart_notification_pending=False))
        await asyncio.wait_for(notice_started.wait(), timeout=5)

        assert dl.sweep_failed_for_runtime("slack", now=time.time() + 10 * dl.STALE_ATTEMPTING_SECONDS) == []

        hang.set()
        await asyncio.wait_for(task, timeout=5)
        assert adapter.send.await_count == 1
        assert _row("ob-1")["state"] == "delivered"


class TestAttemptsOnlySpentOnRealSends:
    """``attempts`` is the redelivery budget — it must buy a send.

    ``self.adapters`` only holds a platform after its ``connect()`` succeeded,
    and the sweep claimed every dead-owner row regardless. A platform that
    failed to connect this boot therefore burned one attempt per boot while
    the caller's ``adapter is None`` branch skipped it without sending — so
    after MAX_ATTEMPTS boots the row abandoned having never been sent once,
    losing exactly the response the ledger exists to guarantee. That failure
    correlates with the crash that created the obligation: the network
    trouble that killed the send tends to still be there on the next boot.
    """

    def test_absent_platform_does_not_burn_attempts(self):
        _record(platform="telegram")
        dl.mark_attempting("ob-1")

        for _ in range(dl.MAX_ATTEMPTS + 2):
            _orphan("ob-1")
            assert dl.sweep_recoverable(deliverable_platforms={"discord"}) == []

        with dl._connect() as conn:
            state, attempts = conn.execute(
                "SELECT state, attempts FROM delivery_obligations "
                "WHERE obligation_id=?", ("ob-1",),
            ).fetchone()
        assert attempts == 0, "an unsendable boot must not spend the budget"
        assert state == "attempting"

    def test_row_still_delivers_once_its_platform_returns(self):
        _record(platform="telegram")
        for _ in range(dl.MAX_ATTEMPTS + 2):
            _orphan("ob-1")
            dl.sweep_recoverable(deliverable_platforms={"discord"})

        _orphan("ob-1")
        claimed = dl.sweep_recoverable(deliverable_platforms={"telegram"})
        assert len(claimed) == 1
        assert claimed[0]["attempts"] == 1


class TestUnconnectedPlatformKeepsItsBudget:
    """End-to-end through the real runner: boots where the platform failed to
    connect must not consume the row's redelivery budget."""

    @staticmethod
    def _runner_without_slack():
        from gateway.run import GatewayRunner

        runner = object.__new__(GatewayRunner)
        runner.adapters = {}  # slack failed to connect this boot
        _store = MagicMock()
        _store.clear_resume_pending = AsyncMock()
        _store._store = None
        runner.session_store = None
        runner._async_session_store = _store
        return runner

    @pytest.mark.asyncio
    async def test_row_survives_boots_where_its_platform_is_down(self):
        _record(platform="slack")
        dl.mark_attempting("ob-1")

        for _ in range(dl.MAX_ATTEMPTS + 1):
            _orphan("ob-1")
            runner = self._runner_without_slack()
            assert await runner._redeliver_pending_obligations() == 0

        assert _row("ob-1")["state"] != "abandoned", (
            "the obligation was abandoned without a single send being attempted"
        )
        assert _row("ob-1")["attempts"] == 0



class TestOwnerAlivePidProbe:
    """_owner_alive's no-start-time fallback must route through
    gateway.status._pid_exists, never a raw ``os.kill(pid, 0)`` probe.

    On Windows ``os.kill(pid, 0)`` is NOT a no-op: CPython maps sig=0 to
    ``GenerateConsoleCtrlEvent(0, pid)`` (bpo-14484), so probing a LIVE pid
    whose start time psutil could not read would Ctrl+C its console group.
    Pattern per the windows-native-support reference: patch
    ``gateway.status._pid_exists``, not ``os.kill``.
    """

    def _no_start_time(self, monkeypatch):
        from gateway import status

        monkeypatch.setattr(status, "get_process_start_time", lambda pid: None)

    def test_alive_when_pid_exists(self, monkeypatch):
        from gateway import status

        self._no_start_time(monkeypatch)
        monkeypatch.setattr(status, "_pid_exists", lambda pid: True)
        assert dl._owner_alive(12345, 999) is True

    def test_dead_when_pid_gone(self, monkeypatch):
        from gateway import status

        self._no_start_time(monkeypatch)
        monkeypatch.setattr(status, "_pid_exists", lambda pid: False)
        assert dl._owner_alive(12345, 999) is False

    def test_raw_os_kill_probe_never_used(self, monkeypatch):
        """Regression guard: the probe must not touch os.kill when
        gateway.status._pid_exists is importable (i.e. always in-tree)."""
        from gateway import status

        self._no_start_time(monkeypatch)
        calls = []
        monkeypatch.setattr(status, "_pid_exists", lambda pid: calls.append(pid) or True)
        monkeypatch.setattr(
            dl.os, "kill", lambda *a, **k: (_ for _ in ()).throw(AssertionError("raw os.kill probe used"))
        )
        assert dl._owner_alive(4242, 999) is True
        assert calls == [4242]

    def test_probe_exception_means_dead(self, monkeypatch):
        from gateway import status

        self._no_start_time(monkeypatch)

        def boom(pid):
            raise RuntimeError("probe blew up")

        monkeypatch.setattr(status, "_pid_exists", boom)
        assert dl._owner_alive(12345, 999) is False


class TestTelegramInPlaceRecovery:
    """MSG-1 through the real pieces: TelegramAdapter, the runner's runtime sweep and this ledger."""

    @pytest.mark.asyncio
    async def test_reply_parked_by_a_degraded_reconnect_goes_out_when_polling_confirms(self):
        import asyncio

        from gateway.config import Platform, PlatformConfig
        from plugins.platforms.telegram.adapter import TelegramAdapter

        telegram = TelegramAdapter(PlatformConfig(enabled=True, token="***"))
        telegram._bot = MagicMock()
        telegram._bot.send_message = AsyncMock(return_value=MagicMock(message_id=7))
        telegram._rich_send_disabled = True
        telegram._write_runtime_status_safe = MagicMock()
        runner = TestGatewayRedeliverySweep._runner()
        runner.adapters = {Platform.TELEGRAM: telegram}
        telegram.gateway_runner = runner
        _record(platform="telegram", session_key="agent:main:telegram:dm:42", chat_id="42", thread_id=None)
        dl.mark_failed("ob-1", "send_path_degraded")

        # Degraded reconnect: the runner publishes the adapter and sweeps, as _install_reconnected_adapter does.
        generation, _event = telegram._begin_polling_generation()
        telegram._running = True
        assert await runner._redeliver_failed_obligations_for_platform(Platform.TELEGRAM) == 0
        telegram._bot.send_message.assert_not_awaited()
        assert _row("ob-1")["attempts"] == 0

        # The first confirmed getUpdates round-trip: recovered in place, no runner reconnect.
        assert telegram._record_polling_progress(generation) is True
        for _ in range(100):
            if _row("ob-1")["state"] == "delivered":
                break
            await asyncio.sleep(0.01)

        assert _row("ob-1")["state"] == "delivered"
        assert _row("ob-1")["attempts"] == 1
        telegram._bot.send_message.assert_awaited_once()
        assert telegram._bot.send_message.await_args.kwargs["text"].endswith("the final answer")

"""Regression tests: the idle-compaction gap anchors on the last persisted
history message timestamp, not ``agent._last_activity_ts``.

On the messaging gateway ``_last_activity_ts`` is reset to ``time.time()`` at
every cached-agent turn start (gateway/run.py) and re-initialized to now when
an evicted agent is rebuilt (agent/agent_init.py), so a gap measured from it is
always ~0 and ``idle_compact_after_seconds`` never fires. Restored transcript
messages carry a durable epoch ``timestamp`` (hermes_state_messages), which
survives those resets. CLI sessions with timestamp-less history keep the old
``_last_activity_ts`` behaviour via the fallback.
"""

from __future__ import annotations

import time
from pathlib import Path
from unittest.mock import MagicMock

from hermes_state import SessionDB

from agent.turn_context_compaction import _idle_gap_anchor

from tests.agent.test_idle_compaction_lock_and_guards import (
    _prep_idle_agent,
    _run_prologue,
)


def _gateway_agent(db: SessionDB, sid: str, *, idle_after: int = 1800):
    """Idle-eligible agent whose ``_last_activity_ts`` the gateway just reset
    to now (the cached-agent turn-start stamp that made the gap always ~0)."""
    agent = _prep_idle_agent(db, sid, idle_after=idle_after)
    agent._last_activity_ts = time.time()
    return agent


def _history_with_ts(*timestamps):
    """Interleaved history rows; ``None`` leaves the message unstamped."""
    msgs = []
    for i, ts in enumerate(timestamps):
        msg = {"role": "user" if i % 2 == 0 else "assistant",
               "content": f"turn {i} " + "x" * 30}
        if ts is not None:
            msg["timestamp"] = ts
        msgs.append(msg)
    return msgs


def _recording_seam(agent, captured: dict):
    """``_compress_context`` stub that records its input and reports a skip."""
    def _seam(messages, *a, **k):
        captured["messages"] = list(messages)
        return messages, "SYSTEM"
    seam = MagicMock(side_effect=_seam)
    agent._compress_context = seam
    return seam


def test_fires_from_old_history_despite_gateway_reset_activity_ts(
    tmp_path: Path,
) -> None:
    """Gateway case: _last_activity_ts was just reset, but the last persisted
    message landed 2h ago and idle_compact_after_seconds=1800."""
    db = SessionDB(db_path=tmp_path / "state.db")
    sid = "IDLE_ANCHOR_FIRES"
    db.create_session(sid, source="cli")
    now = time.time()
    agent = _gateway_agent(db, sid)
    seam = _recording_seam(agent, {})

    _run_prologue(agent, _history_with_ts(now - 8000, now - 7200))

    seam.assert_called_once()


def test_recent_history_message_does_not_fire(tmp_path: Path) -> None:
    db = SessionDB(db_path=tmp_path / "state.db")
    sid = "IDLE_ANCHOR_RECENT"
    db.create_session(sid, source="cli")
    now = time.time()
    agent = _gateway_agent(db, sid)
    seam = _recording_seam(agent, {})

    _run_prologue(agent, _history_with_ts(now - 8000, now - 600))

    seam.assert_not_called()
    agent.context_compressor.compress.assert_not_called()


def test_timestampless_history_falls_back_to_last_activity_ts(
    tmp_path: Path,
) -> None:
    """CLI behaviour unchanged: no usable history timestamps -> the old
    _last_activity_ts gap still drives the decision."""
    db = SessionDB(db_path=tmp_path / "state.db")
    sid = "IDLE_ANCHOR_FALLBACK"
    db.create_session(sid, source="cli")
    agent = _gateway_agent(db, sid)
    agent._last_activity_ts = time.time() - 7200
    seam = _recording_seam(agent, {})

    _run_prologue(agent, _history_with_ts(None, None))

    seam.assert_called_once()


def test_current_turn_user_message_timestamp_is_not_the_anchor(
    tmp_path: Path,
) -> None:
    """The current turn's user message is already appended (and stamped ~now)
    when idle compaction runs; if it anchored the gap, nothing would fire."""
    db = SessionDB(db_path=tmp_path / "state.db")
    sid = "IDLE_ANCHOR_USER_MSG"
    db.create_session(sid, source="cli")
    now = time.time()
    agent = _gateway_agent(db, sid)
    captured: dict = {}
    seam = _recording_seam(agent, captured)

    _run_prologue(agent, _history_with_ts(now - 7200))

    seam.assert_called_once()
    # The captured pre-compaction list proves the appended user message really
    # did carry a ~now timestamp and was still excluded from the anchor.
    user_msg = captured["messages"][-1]
    assert user_msg["role"] == "user"
    assert now - 60 < user_msg["timestamp"] <= time.time()


def test_garbage_and_future_timestamps_ignored(tmp_path: Path) -> None:
    """No valid history timestamp -> fallback gap (~0 here) -> no fire."""
    db = SessionDB(db_path=tmp_path / "state.db")
    sid = "IDLE_ANCHOR_GARBAGE"
    db.create_session(sid, source="cli")
    now = time.time()
    agent = _gateway_agent(db, sid)
    seam = _recording_seam(agent, {})
    history = _history_with_ts(None, None)
    history[0]["timestamp"] = "not-a-number"
    history[1]["timestamp"] = now + 10_000  # future
    history.append({"role": "user", "content": "ms epoch", "timestamp": 1.7e12})

    _run_prologue(agent, history)

    seam.assert_not_called()
    agent.context_compressor.compress.assert_not_called()


def test_one_valid_old_timestamp_wins_over_garbage(tmp_path: Path) -> None:
    db = SessionDB(db_path=tmp_path / "state.db")
    sid = "IDLE_ANCHOR_MIXED"
    db.create_session(sid, source="cli")
    now = time.time()
    agent = _gateway_agent(db, sid)
    seam = _recording_seam(agent, {})

    _run_prologue(agent, _history_with_ts(now + 10_000, now - 7200))

    seam.assert_called_once()


class TestIdleGapAnchorHelper:
    """Pure-function edges the prologue tests don't reach."""

    def test_user_message_not_yet_in_messages(self):
        # current_turn_user_idx == len(messages): history is the whole list.
        now = time.time()
        msgs = [{"role": "user", "content": "a", "timestamp": now - 7200}]
        assert _idle_gap_anchor(msgs, len(msgs), now) == now - 7200

    def test_bool_and_nonpositive_timestamps_rejected(self):
        now = time.time()
        msgs = [{"role": "user", "content": "a", "timestamp": True},
                {"role": "user", "content": "b", "timestamp": 0},
                {"role": "user", "content": "c", "timestamp": -5}]
        assert _idle_gap_anchor(msgs, len(msgs), now) is None

    def test_max_not_min_of_valid_timestamps(self):
        now = time.time()
        msgs = [{"role": "user", "content": "a", "timestamp": now - 9000},
                {"role": "assistant", "content": "b", "timestamp": now - 3000}]
        assert _idle_gap_anchor(msgs, len(msgs), now) == now - 3000

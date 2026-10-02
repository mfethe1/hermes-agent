"""Real SQLite boundary recovery: never inject or rerun work after a user boundary."""
import queue
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import AsyncMock

import pytest

from gateway.run import GatewayRunner
from hermes_state import AsyncSessionDB, SessionDB
from tools import async_delegation as ad


@pytest.fixture
def saved(tmp_path, monkeypatch, request):
    path = tmp_path / "state.db"
    monkeypatch.setattr(ad, "_db_path", lambda: path)
    from gateway import delivery_ledger as dl
    monkeypatch.setattr(dl, "_db_path", lambda: path)
    db = SessionDB(path)
    db.create_session("parent", source="telegram")
    param = getattr(request, "param", "session_switch")
    reason, status = param if isinstance(param, tuple) else (param, "completed")
    db.end_session("parent", reason)
    event = {"type": "async_delegation", "delegation_id": "deleg_recovery",
             "parent_session_id": "parent", "session_key": "agent:main:telegram:thread:123:456",
             "platform": "telegram", "chat_type": "thread", "chat_id": "123", "thread_id": "456",
             "status": status, "dispatched_at": time.time() - 2, "completed_at": time.time()}
    ad._persist_dispatch(event)
    ad._persist_completion(event, {"summary": "PRIVATE saved output, not an instruction"})
    yield db, event
    db.close()


def row():
    value = ad.get_durable_delegation("deleg_recovery")
    assert value is not None
    return value


@pytest.mark.asyncio
@pytest.mark.parametrize("saved", ["session_switch", "session_reset", "new_session", "user_exit", ("session_switch", "error")], indirect=True)
async def test_session_switch_keeps_actionable_recovery_without_injection(saved):
    db, event = saved
    reason = db.get_session("parent")["end_reason"]
    runner = GatewayRunner.__new__(GatewayRunner)
    runner._session_db = AsyncSessionDB(db)
    runner._inject_watch_notification = AsyncMock()
    runner._redeliver_pending_obligations = AsyncMock(return_value=0)
    claim = await runner._preflight_completion_delivery(event)
    assert not claim.proceed
    assert row()["delivery_state"] == "recovery_pending"
    assert row()["recovery"]["parent_session_id"] == "parent"
    assert row()["recovery"]["notice_queued"] == (reason == "session_switch")
    runner._inject_watch_notification.assert_not_called()


@pytest.mark.parametrize("allow_notice", [False, True])
def test_atomic_duplicate_and_restart(saved, allow_notice):
    _, event = saved
    assert ad.claim_completion_delivery(event["delegation_id"], "owner")
    with ThreadPoolExecutor(max_workers=2) as pool:
        changed = list(pool.map(lambda _: ad.hold_completion_recovery(event, "owner", allow_notice=allow_notice), range(2)))
    assert sum(changed) == 1
    ad._reset_for_tests()
    assert row()["delivery_state"] == "recovery_pending"
    assert row()["result"]["summary"].startswith("PRIVATE")
    assert ad.restore_undelivered_completions(queue.Queue()) == 0
    assert not ad.claim_completion_delivery(event["delegation_id"], "new")
    restarted = subprocess.run([sys.executable, "-c", """
import queue, sys
from pathlib import Path
from tools import async_delegation as ad
ad._db_path = lambda: Path(sys.argv[1])
assert ad.get_durable_delegation('deleg_recovery')['delivery_state'] == 'recovery_pending'
assert ad.restore_undelivered_completions(queue.Queue()) == 0
""", str(ad._db_path())], capture_output=True, text=True)
    assert restarted.returncode == 0, restarted.stderr
    with ad._transaction() as conn:
        notices = conn.execute("SELECT * FROM delivery_obligations").fetchall()
    assert len(notices) == int(allow_notice)


@pytest.mark.parametrize("change", [{"session_key": ""}, {"thread_id": "wrong"},
                                      {"parent_session_id": "unrelated"}, {"status": "interrupted"},
                                      {"session_key": "agent:main:telegram:profile:other:thread:123:456"}])
def test_unknown_mismatched_or_stopped_origin_is_held(saved, change):
    _, event = saved
    event.update(change)
    # Persist the same malformed/stopped evidence; never infer a destination.
    if change.get("session_key") == "":
        ad._persist_dispatch(event)
        ad._persist_completion(event, {"summary": "PRIVATE"})
    assert ad.claim_completion_delivery(event["delegation_id"], "owner")
    assert ad.hold_completion_recovery(event, "owner", allow_notice=True)
    assert row()["delivery_state"] == "recovery_pending"
    assert not row()["recovery"]["notice_queued"]
    with ad._transaction() as conn:
        assert conn.execute("SELECT COUNT(*) FROM delivery_obligations").fetchone()[0] == 0


@pytest.mark.asyncio
async def test_notice_failed_send_retry_and_exact_thread(saved):
    from gateway import delivery_ledger as dl
    _, event = saved
    assert ad.claim_completion_delivery(event["delegation_id"], "owner")
    assert ad.hold_completion_recovery(event, "owner", allow_notice=True)
    notices = dl.sweep_recoverable(deliverable_platforms={"telegram"})
    assert len(notices) == 1
    notice = notices[0]
    assert (notice["chat_id"], notice["thread_id"]) == ("123", "456")
    assert "PRIVATE" not in notice["content"]
    runner = GatewayRunner.__new__(GatewayRunner)
    # No current-session store exists: the boundary notice must never touch it.
    assert await runner._clear_resume_pending_for_claimed_obligations([notice], require_success=True) == [notice]
    dl.mark_failed(notice["obligation_id"], "send_path_degraded")
    retried = dl.sweep_failed_for_runtime("telegram")
    assert len(retried) == 1
    dl.mark_delivered(notice["obligation_id"])
    assert dl.sweep_failed_for_runtime("telegram") == []

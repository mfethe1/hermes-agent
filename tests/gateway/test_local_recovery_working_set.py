"""Boot recovers durable work, not every historical local/cron conversation.

Eagerly restoring thousands of idle receipts pins their policies and blocks the
Telegram owner loop; historical sessions must still mount lazily on resume.
"""
import pytest

from tests.gateway.test_viewer_close_lifecycle import (
    _call, _open, _owner, _restarted_owner, _viewer,
)


@pytest.mark.asyncio
async def test_idle_local_receipt_is_lazy_but_history_still_resumes(tmp_path, monkeypatch):
    from gateway.session_local_recovery import recover_local_sessions

    db, authority, _ = await _owner(tmp_path, monkeypatch)
    await _open(db, authority, tmp_path)
    db2, restarted, calls = await _restarted_owner(tmp_path, monkeypatch, db)

    recover_local_sessions(restarted, schedule=True)

    assert 'chat' not in restarted.sessions
    assert calls == []
    assert db2.get_session('chat') is not None
    await _call(_viewer(restarted, tmp_path), 'session.resume', session_id='chat')
    assert 'chat' in restarted.sessions
    assert [row['content'] for row in db2.get_messages('chat')] == ['q', 'a']


@pytest.mark.asyncio
async def test_compression_catchup_does_not_read_idle_uncompressed_policies(tmp_path, monkeypatch):
    from gateway import session_local_adoption

    db, authority, _ = await _owner(tmp_path, monkeypatch)
    await _open(db, authority, tmp_path)
    inspected = []
    monkeypatch.setattr(session_local_adoption, '_due', lambda owner, sid: inspected.append(sid))

    await session_local_adoption.adopt_local_compressions(authority)

    assert inspected == []


@pytest.mark.asyncio
async def test_queued_local_input_is_restored_and_scheduled(tmp_path, monkeypatch):
    from gateway.session_local_recovery import recover_local_sessions
    from hermes_state_runtime import admit_session_input

    db, authority, _ = await _owner(tmp_path, monkeypatch)
    await _open(db, authority, tmp_path)
    admit_session_input(db, epoch=authority.epoch, principal_id='native', session_id='chat',
                        request_id='pending', payload={'text': 'continue'})
    _, restarted, _ = await _restarted_owner(tmp_path, monkeypatch, db)
    scheduled = []
    monkeypatch.setattr(restarted, '_schedule', scheduled.append)

    recover_local_sessions(restarted, schedule=True)

    assert 'chat' in restarted.sessions
    assert [ref.session_id for ref in scheduled] == ['chat']

"""A messaging user whose session FIFO is paused is told once, and can /reset out of it.

An owner crash mid-turn leaves the started admission ``unknown`` and the session paused
(fail-closed, never weakened here). Before this fix the next Telegram message was admitted,
``admit_message`` awaited it forever and the platform user saw nothing at all.

That notice is sent once per pause episode, which was its own silence: a user who keeps typing
instead of reading it got nothing back. So a message from the sender of the lost turn now settles
it and resumes the chat (FLE-72, ``gateway/session_person_unknown``); the pause that remains is the
one with nobody waiting behind it.
"""
import asyncio
from contextlib import closing
from http.server import ThreadingHTTPServer
import json
from pathlib import Path
import queue
import sqlite3
import threading
import time

import importlib.machinery

import pytest

from tests.gateway.fixtures.local_recovery_probe import Model, child_env, daemon
from tests.gateway.test_native_telegram_startup_recovery import BotAPI, wait_for

PAUSE_MARKER = 'This conversation is paused'
# gateway/session_person_unknown.lost_turn_notice: the line that names the turn a restart dropped.
LOST_TURN_MARKER = 'Lost message:'


@pytest.mark.asyncio
async def test_a_waiting_senders_messages_resume_the_fifo_and_name_the_lost_turn_once(tmp_path, monkeypatch):
    """No daemon: the authority alone. Every messaging waiter parked on this session returns and
    none ever hangs — but a sender with a message queued behind their *own* lost turn is no longer
    told to ``/discard``: the turn settles as ``/discard`` would, the FIFO runs, and exactly one
    reply names what was dropped (FLE-72, tests/gateway/test_person_unknown_resumes.py).

    The once-per-episode notice plumbing is asserted directly off ``pause_notice`` here, because
    this scenario no longer reaches a pause through the drain. The pause that *does* still happen
    (nobody waiting behind the lost turn) is covered by
    tests/gateway/test_unknown_automation_settles.py::test_a_persons_unknown_turn_still_pauses_the_fifo.
    """
    from gateway.platforms.event import MessageEvent
    from gateway.session_authority import SessionAuthority
    from gateway.session_contract import Principal, Submission
    from gateway.session_ingress import admit_message, pause_notice
    from hermes_state_runtime import get_session_admission
    from tests.gateway.test_authority_unknown_resolution import _restarted_owner_with_unknown_head

    executed = []
    # No pre-restart follower: the lost-turn notice rides the first person's turn claimed, so the
    # only person's rows behind the pause must be the ones this test admits and can observe.
    store, authority, ref, unknown, _ = await _restarted_owner_with_unknown_head(
        tmp_path, monkeypatch, executed, follower=False)
    try:
        actor = Principal('human', 'p', frozenset({'session:submit'}), 'fixture')

        async def admit_native(event):
            return await authority.submit(actor, Submission(event.text, ref, {'text': event.text}, 'queue'))
        monkeypatch.setattr(authority, 'admit_native', admit_native)
        # One notice per pause episode: the first message onto a pause is told, later ones are
        # admitted silently. The /discard copy still names the escape for the pause that remains.
        first_notice = pause_notice(authority, ref, 'unknown_execution')
        assert PAUSE_MARKER in first_notice and '/discard' in first_notice, first_notice
        assert pause_notice(authority, ref, 'unknown_execution') is None
        # A restart drain is transient: the saved row runs on the next owner, so the notice must
        # not tell the user to /reset (that would discard the very message that is about to run).
        authority.sessions['s'].pause_notified = False
        draining = pause_notice(authority, ref, 'runtime_draining')
        assert 'restarting' in draining and '/reset' not in draining, draining
        authority.sessions['s'].pause_notified = True

        authority._schedule = lambda ref: SessionAuthority._schedule(authority, ref)
        source = authority.sessions['s'].source
        first = asyncio.create_task(admit_message(authority, MessageEvent(text='one', source=source)))
        second = asyncio.create_task(admit_message(authority, MessageEvent(text='two', source=source)))
        replies = await asyncio.wait_for(asyncio.gather(first, second), 5)
        await asyncio.wait_for(authority.sessions['s'].task, 5)
        # Both waiters came back with an answer instead of a second silence, and the lost turn was
        # settled rather than replayed: 'HEAD' never reaches the model again.
        assert executed == ['one', 'two'], executed
        settled = get_session_admission(authority.db, admission_id=unknown['admission_id'])
        assert settled['status'] != 'unknown', settled['status']
        # Named once, quoting the lost message, on a reply the sender actually receives.
        named = [r for r in replies if r is not None and 'Lost message' in r]
        assert len(named) == 1, replies
        assert 'HEAD' in named[0], named[0]
        # The FIFO moved, so the pause episode is over and a later pause gets its own notice.
        assert not authority.sessions['s'].pause_notified
    finally:
        store.close_all_db_handles()


@pytest.mark.skipif(importlib.machinery.PathFinder.find_spec("telegram") is None, reason="python-telegram-bot not installed (on-demand extra)")
@pytest.mark.platforms("linux")
def test_crash_mid_turn_then_platform_message_resumes_the_chat_and_names_the_lost_turn(tmp_path):
    """End to end over a real Telegram transport: the 2026-11-02 silence, and that it is over.

    A crash mid-turn fences the started admission ``unknown``. Before FLE-72 the next message was
    admitted, the user was told once to ``/discard``, and every message after that got nothing at
    all. Now that first message settles the lost turn, the chat answers, and one reply names what
    was dropped. ``/reset`` still works and still moves the chat to a fresh session.
    """
    root = Path(__file__).resolve().parents[2]
    home, user = tmp_path / 'state', tmp_path / 'user'
    home.mkdir(mode=0o700)
    user.mkdir()
    model = ThreadingHTTPServer(('127.0.0.1', 0), Model)
    model.requests = []
    model.blocked, model.release = threading.Event(), threading.Event()
    bot = ThreadingHTTPServer(('127.0.0.1', 0), BotAPI)
    bot.calls, bot.sent, bot.updates = [], [], queue.Queue()
    for peer in (model, bot):
        threading.Thread(target=peer.serve_forever, daemon=True).start()
    base = f'http://127.0.0.1:{model.server_port}/v1'
    cfg = {'model': {'provider': 'custom', 'default': 'local-wire-stub', 'base_url': base},
           'gateway': {'multiplex_profiles': False},
           'platforms': {'telegram': {'enabled': True, 'extra': {
               'base_url': f'http://127.0.0.1:{bot.server_port}/bot', 'dm_policy': 'allowlist'}}},
           'auxiliary': {'title_generation': {'enabled': False}},
           'platform_toolsets': {'telegram': []}, 'terminal': {'cwd': str(home)}}
    (home / 'config.yaml').write_text(json.dumps(cfg), encoding='utf-8')
    env = child_env()
    env.update(HOME=str(user), USERPROFILE=str(user), HERMES_HOME=str(home), PYTHONPATH=str(root),
               OPENAI_API_KEY='loopback-only', OPENAI_BASE_URL=base, PYTHONUNBUFFERED='1',
               TELEGRAM_BOT_TOKEN='987654321:owned-loopback-fixture', TELEGRAM_ALLOWED_USERS='202')

    def rows():
        with closing(sqlite3.connect(f'file:{home / "state.db"}?mode=ro', uri=True)) as db:
            return {json.loads(payload)['text']: {'session': sid, 'status': status}
                    for sid, status, payload in db.execute(
                        'SELECT target_session_id,status,payload_json FROM session_admissions')}

    def sent_texts():
        # Telegram MarkdownV2 escapes underscores; compare on the unescaped text.
        return [m.get('text', '').replace('\\', '') for m in bot.sent]

    def notices():
        return [t for t in sent_texts() if PAUSE_MARKER in t]

    def lost_notices():
        return [t for t in sent_texts() if LOST_TURN_MARKER in t]

    def diagnostic():
        return repr(rows()) + '\n' + repr(sent_texts()) + '\n' + '\n'.join(
            p.read_text(encoding='utf-8') for p in (home / 'logs').glob('*.log'))

    update_id = 0

    def push(text):
        nonlocal update_id
        update_id += 1
        bot.updates.put({'update_id': update_id, 'message': {
            'message_id': update_id, 'date': int(time.time()),
            'chat': {'id': 202, 'type': 'private', 'first_name': 'Owned'},
            'from': {'id': 202, 'is_bot': False, 'first_name': 'Owned'}, 'text': text}})

    def send(text, status):
        push(text)
        wait_for(lambda: rows().get(text, {}).get('status') == status, diagnostic)

    try:
        with daemon(root, home, env, barrier=False) as (proc, _desc):
            send('WARM_HISTORY', 'terminal')
            send('BLOCK_STARTED', 'started')
            assert model.blocked.wait(20), diagnostic()
            proc.kill()
            assert proc.wait(timeout=10) == -9
        model.release.set()
        with daemon(root, home, env, barrier=False) as (proc, _desc):
            wait_for(lambda: rows()['BLOCK_STARTED']['status'] == 'unknown', diagnostic)
            paused_session = rows()['BLOCK_STARTED']['session']
            # First message onto the paused FIFO: it is the sender saying they are still waiting,
            # so the lost turn settles, this message runs, and one reply names what was dropped.
            send('AFTER_CRASH_ONE', 'terminal')
            wait_for(lambda: len(lost_notices()) == 1, diagnostic)
            assert 'BLOCK_STARTED' in lost_notices()[0], lost_notices()
            # The chat is no longer paused, so nothing asks for /discard and the next message is
            # answered like any other.
            assert notices() == [], notices()
            send('AFTER_CRASH_TWO', 'terminal')
            assert rows()['AFTER_CRASH_TWO']['session'] == paused_session
            # The named escape works from the platform: /reset (confirmed) moves the chat to
            # a fresh session and the next message runs and is answered.
            push('/reset')
            wait_for(lambda: any('Confirm /new' in t for t in sent_texts()), diagnostic)
            push('/approve')
            wait_for(lambda: any('Session reset' in t or 'New session' in t for t in sent_texts()), diagnostic)
            send('AFTER_RESET', 'terminal')
            wait_for(lambda: any('RECOVERY_ACK_AFTER_RESET' in t for t in sent_texts()), diagnostic)
            after = rows()
            assert after['AFTER_RESET']['session'] != paused_session
            # The lost turn is named exactly once for the whole episode, is settled rather than
            # left unknown, and the messages queued behind it were answered instead of stranded.
            assert len(lost_notices()) == 1, lost_notices()
            assert after['BLOCK_STARTED']['status'] != 'unknown', after['BLOCK_STARTED']
            assert after['AFTER_CRASH_ONE']['status'] == after['AFTER_CRASH_TWO']['status'] == 'terminal'
        texts = [next(m['content'] for m in reversed(r['messages']) if m['role'] == 'user') for r in model.requests]
        # The two messages ran; the lost turn itself was dropped, never replayed.
        assert any('AFTER_CRASH_ONE' in t for t in texts), texts
        assert sum('BLOCK_STARTED' in t for t in texts) == 1, texts
        print(json.dumps({'lost_turn_notice': lost_notices()[0], 'final_rows': rows()}))
    finally:
        model.release.set()
        for peer in (model, bot):
            peer.shutdown()
            peer.server_close()

"""FLE-72: a person's lost turn settles itself once that person is demonstrably still waiting.

A turn lost across a gateway restart is fenced ``unknown`` and pauses its session's FIFO for
``/discard`` (GW-10). ``pause_notice`` offers that way out once per pause episode, so on
2026-11-02 three live Telegram topics answered nothing at all after their one notice — one of them
took the same question five times.

A queued message from a person is the acknowledgement the pause was waiting for: the lost turn
settles through the same fenced ``resolve_unknown_session_input`` ``/discard`` uses (never
replayed), the FIFO runs, and the reply names what was dropped. With no person queued behind it,
today's pause stands.

Real ``SessionStore`` SQLite, two real owner epochs; no store monkeypatching.
"""
import queue
from types import SimpleNamespace

import pytest

from gateway.config import GatewayConfig, Platform
from gateway.session import SessionSource, SessionStore
from gateway.session_authority import LiveSession, initialize_session_authority
from gateway.session_contract import SessionRef, Submission
from gateway.session_controls import AuthorityConnection
from hermes_state_runtime import admit_session_input, claim_session_input, list_session_admissions
from tests.waiting import async_wait_until


class Peer:
    def __init__(self):
        self.frames = queue.Queue()

    def write(self, frame):
        self.frames.put(frame)
        return True


def _runner(tmp_path, monkeypatch, executed):
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    store = SessionStore(tmp_path / 'sessions', GatewayConfig())

    async def answer(event):
        from gateway.session_results import execution_result
        executed.append(event.text)
        execution_result.get()['result'] = {'final_response': 'ACK_' + event.text, 'completed': True, 'messages': []}
        return 'ACK_' + event.text

    runner = SimpleNamespace(_session_db=store._db, session_store=store, _draining=False,
                             _handle_message=answer, _adapter_for_source=lambda source: None)
    return store, runner


def _admit_automation(db, epoch, request_id, text, *, session_id='s'):
    """A ``local_automation_v1`` row that passes ``check_local_automation``: its principal, owner,
    route and identity all name the session it runs on, as the producer's own commit does."""
    return admit_session_input(
        db, epoch=epoch, principal_id='automation:' + session_id, session_id=session_id,
        request_id=request_id, payload={'text': text, 'local_automation_v1': {
            'identity': request_id, 'owner': session_id, 'route': session_id}})['admission_id']


async def _restart_losing_head(tmp_path, monkeypatch, executed, followers=(), automation=()):
    """Owner 1 admits a person's head, then *automation* rows, then each text in *followers*;
    claims the head and dies. Owner 2 starts on the same store, so the head is fenced ``unknown``.

    Returns ``(store, owner2, head_id, [follower ids], [automation ids])``.
    """
    store, runner = _runner(tmp_path, monkeypatch, executed)
    db = store._db
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='fle72')
    db.create_session('s', source='telegram')
    first = await initialize_session_authority(runner, profile_id='p', instance_id='owner-1')
    first.sessions['s'] = LiveSession(source, 's')
    ref = SessionRef('p', 's')
    actor = AuthorityConnection(first, Peer(), {'user_id': 'human'}).actor
    first._schedule = lambda ref: None
    head = await first.submit(actor, Submission('head', ref, {'text': 'WHAT IS THE STATUS?'}, 'queue'))
    automation_ids = [_admit_automation(db, first.epoch, f'auto-{index}', text)
                      for index, text in enumerate(automation)]
    follower_ids = [(await first.submit(actor, Submission(f'follower-{index}', ref, {'text': text}, 'queue'))).admission_id
                    for index, text in enumerate(followers)]
    claimed = claim_session_input(db, epoch=first.epoch, session_id='s')
    assert claimed['admission_id'] == head.admission_id
    db.append_message('s', 'user', 'WHAT IS THE STATUS?')  # the lost turn got as far as its user row
    second = await initialize_session_authority(runner, profile_id='p', instance_id='owner-2')
    second.sessions['s'] = LiveSession(source, 's')
    return store, second, head.admission_id, follower_ids, automation_ids


def _statuses(db):
    return {r['admission_id']: (r['status'], r['outcome'])
            for r in list_session_admissions(db, session_id='s', pending_only=False)}


async def _drain(authority):
    authority._schedule(SessionRef('p', 's'))
    live = authority.sessions['s']
    await async_wait_until(lambda: live.task is not None and live.task.done(), what='drain finished')
    return live


# --------------------------------------------------------------------- fix 1: the queued follow-up


@pytest.mark.asyncio
async def test_queued_follow_up_settles_the_lost_turn_and_gets_answered(tmp_path, monkeypatch):
    """The incident: a restart loses a turn, the person's follow-up sits behind it. Before this
    fix the FIFO paused and the follow-up was never answered."""
    executed = []
    store, authority, head_id, (follower_id,), _ = await _restart_losing_head(
        tmp_path, monkeypatch, executed, followers=['ANY UPDATE?'])
    try:
        assert _statuses(authority.db)[head_id] == ('unknown', None)
        await _drain(authority)
        # The follow-up ran; the lost turn settled terminal and was never replayed.
        assert executed == ['ANY UPDATE?']
        statuses = _statuses(authority.db)
        assert statuses[head_id][0] == 'terminal'
        assert statuses[follower_id] == ('terminal', 'completed')
    finally:
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_the_reply_names_the_lost_message_and_quotes_its_first_line(tmp_path, monkeypatch):
    """The person is told once, in the reply they actually receive, which message was dropped."""
    from gateway.session_ingress import execute_admission
    executed = []
    store, authority, _head_id, (follower_id,), _ = await _restart_losing_head(
        tmp_path, monkeypatch, executed, followers=['ANY UPDATE?'])
    try:
        ref = SessionRef('p', 's')
        from gateway.session_person_unknown import resume_for_waiting_person
        assert await resume_for_waiting_person(authority, ref)
        live = authority.sessions['s']
        assert live.lost_turn_notice is not None
        row = next(r for r in list_session_admissions(authority.db, session_id='s')
                   if r['admission_id'] == follower_id)
        reply = await execute_admission(authority, ref, row)
        # One reply: the notice, the quoted first line of the lost message, then the real answer.
        assert 'WHAT IS THE STATUS?' in reply, reply
        assert 'ACK_ANY UPDATE?' in reply, reply
        assert 'not' in reply.lower() and 're-run' in reply.lower(), reply
        # Told once: the notice is consumed, so the next turn's reply is only its answer.
        assert live.lost_turn_notice is None
    finally:
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_no_queued_person_message_keeps_todays_pause(tmp_path, monkeypatch):
    """The guard on fix 1: nothing queued behind the lost turn means nobody has said they are
    waiting, so the session stays paused for ``/discard`` exactly as before."""
    executed = []
    store, authority, head_id, _followers, _ = await _restart_losing_head(tmp_path, monkeypatch, executed)
    try:
        await _drain(authority)
        assert executed == []
        assert _statuses(authority.db)[head_id] == ('unknown', None)
        # ``getattr``: this guard asserts the pre-fix behaviour, so it must also pass on a tree
        # where ``LiveSession`` has no notice field at all.
        assert getattr(authority.sessions['s'], 'lost_turn_notice', None) is None
    finally:
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_only_automation_queued_behind_the_lost_turn_keeps_the_pause(tmp_path, monkeypatch):
    """A heartbeat queued behind a person's lost turn is not that person asking again: a standing
    goal continuing on its own must never settle somebody's lost question for them."""
    executed = []
    store, authority, head_id, _f, (auto_id,) = await _restart_losing_head(
        tmp_path, monkeypatch, executed, automation=['[Continuing toward your standing goal]'])
    try:
        await _drain(authority)
        assert executed == []
        assert _statuses(authority.db)[head_id] == ('unknown', None)
        assert _statuses(authority.db)[auto_id] == ('queued', None)
    finally:
        store.close_all_db_handles()


# ------------------------------------------------------------ fix 2: one answer per repeated burst


@pytest.mark.asyncio
async def test_identical_messages_queued_in_a_row_are_answered_once(tmp_path, monkeypatch):
    """Michael sent the same question five times into a paused topic. Before this fix the resumed
    FIFO ran all five and sent five identical answers."""
    executed = []
    store, authority, _head_id, follower_ids, _ = await _restart_losing_head(
        tmp_path, monkeypatch, executed, followers=['ANY UPDATE?'] * 5)
    try:
        await _drain(authority)
        assert executed == ['ANY UPDATE?']
        statuses = _statuses(authority.db)
        # The last copy is the one answered: its delivery waiter is the one still parked.
        assert statuses[follower_ids[-1]] == ('terminal', 'completed')
        assert [statuses[i] for i in follower_ids[:-1]] == [('terminal', 'cancelled')] * 4
    finally:
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_different_messages_are_all_answered(tmp_path, monkeypatch):
    """The guard on fix 2: only identical text collapses. Two real questions get two answers."""
    executed = []
    store, authority, _head_id, _ids, _ = await _restart_losing_head(
        tmp_path, monkeypatch, executed, followers=['ANY UPDATE?', 'AND THE OTHER ONE?'])
    try:
        await _drain(authority)
        assert executed == ['ANY UPDATE?', 'AND THE OTHER ONE?']
    finally:
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_identical_messages_split_by_another_row_are_both_answered(tmp_path, monkeypatch):
    """"In a row" is literal: a row committed between two identical messages breaks the run, so
    neither is silently dropped. Here the separator is a second person in the same group chat."""
    executed = []
    store, runner = _runner(tmp_path, monkeypatch, executed)
    db = store._db
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='fle72')
    db.create_session('s', source='telegram')
    first = await initialize_session_authority(runner, profile_id='p', instance_id='owner-1')
    first.sessions['s'] = LiveSession(source, 's')
    ref = SessionRef('p', 's')
    actor = AuthorityConnection(first, Peer(), {'user_id': 'human'}).actor
    other = AuthorityConnection(first, Peer(), {'user_id': 'colleague'}).actor
    first._schedule = lambda ref: None
    head = await first.submit(actor, Submission('head', ref, {'text': 'LOST'}, 'queue'))
    await first.submit(actor, Submission('f1', ref, {'text': 'SAME'}, 'queue'))
    await first.submit(other, Submission('mid', ref, {'text': 'SOMEONE ELSE'}, 'queue'))
    await first.submit(actor, Submission('f2', ref, {'text': 'SAME'}, 'queue'))
    assert claim_session_input(db, epoch=first.epoch, session_id='s')['admission_id'] == head.admission_id
    second = await initialize_session_authority(runner, profile_id='p', instance_id='owner-2')
    second.sessions['s'] = LiveSession(source, 's')
    try:
        await _drain(second)
        assert executed == ['SAME', 'SOMEONE ELSE', 'SAME']
    finally:
        store.close_all_db_handles()


def test_superseded_duplicates_only_collapses_an_adjacent_run_by_one_person():
    """The adjacency and ownership rules of fix 2, on rows alone: an automation row between two
    identical messages breaks the run, and two people repeating each other never collapse."""
    from gateway.session_person_unknown import superseded_duplicates

    def row(seq, principal, text, status='queued', payload_extra=None):
        return {'seq': seq, 'admission_id': f'a{seq}', 'status': status, 'principal_id': principal,
                'payload': {'text': text, **(payload_extra or {})}}

    auto = {'local_automation_v1': {'identity': 'proc-1', 'owner': 's'}}
    rows = [row(1, 'human', 'SAME'), row(2, 'human', 'SAME'), row(3, 'human', 'SAME')]
    assert [r['admission_id'] for r in superseded_duplicates(rows)] == ['a1', 'a2']
    split = [row(1, 'human', 'SAME'), row(2, 'automation:s', 'TICK', payload_extra=auto),
             row(3, 'human', 'SAME')]
    assert superseded_duplicates(split) == []
    two_people = [row(1, 'human', 'SAME'), row(2, 'colleague', 'SAME')]
    assert superseded_duplicates(two_people) == []
    # An already-claimed row is not a queued duplicate, and empty text never collapses.
    assert superseded_duplicates([row(1, 'human', 'SAME', status='started'), row(2, 'human', 'SAME')]) == []
    assert superseded_duplicates([row(1, 'human', ''), row(2, 'human', '')]) == []


# ------------------------------------------------------- automation already queued: say what happens


@pytest.mark.asyncio
async def test_queued_automation_keeps_its_place_and_is_disclosed(tmp_path, monkeypatch):
    """What happens to a stale ``[Continuing toward your standing goal]`` queued before the
    restart: nothing. It is not cancelled (only its sender knows whether it still matters) and it
    keeps its FIFO position, so it still runs — and the person's reply says how many are coming,
    rather than letting an answer they never asked for arrive with no explanation."""
    executed = []
    store, authority, _head_id, (follower_id,), (auto_id,) = await _restart_losing_head(
        tmp_path, monkeypatch, executed, followers=['ANY UPDATE?'],
        automation=['[Continuing toward your standing goal]'])
    try:
        ref = SessionRef('p', 's')
        from gateway.session_person_unknown import resume_for_waiting_person
        assert await resume_for_waiting_person(authority, ref)
        notice = authority.sessions['s'].lost_turn_notice
        assert 'automated' in notice.lower() and 'still run' in notice.lower(), notice
        assert '1 automated' in notice, notice
        # Untouched and still ahead of the person's message: the resume reorders nothing.
        pending = list_session_admissions(authority.db, session_id='s')
        assert [r['admission_id'] for r in pending] == [auto_id, follower_id]
        assert [r['status'] for r in pending] == ['queued', 'queued']
    finally:
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_the_notice_rides_the_persons_reply_not_an_automation_turn(tmp_path, monkeypatch):
    """The notice must reach a message the person reads. An automation row claimed first would
    bury it in a reply they never asked for, so it waits for the person's own turn."""
    from gateway.session_person_unknown import resume_for_waiting_person, take_lost_turn_notice
    executed = []
    store, authority, _head_id, (follower_id,), (auto_id,) = await _restart_losing_head(
        tmp_path, monkeypatch, executed, followers=['ANY UPDATE?'],
        automation=['[Continuing toward your standing goal]'])
    try:
        assert await resume_for_waiting_person(authority, SessionRef('p', 's'))
        live = authority.sessions['s']
        rows = {r['admission_id']: r for r in list_session_admissions(authority.db, session_id='s')}
        # The automation turn claimed first takes nothing; the notice is still owed.
        assert take_lost_turn_notice(live, rows[auto_id]) is None
        assert live.lost_turn_notice is not None
        notice = take_lost_turn_notice(live, rows[follower_id])
        assert notice is not None and 'WHAT IS THE STATUS?' in notice, notice
        # Consumed: a second person's turn in the same chat is not told again.
        assert take_lost_turn_notice(live, rows[follower_id]) is None
    finally:
        store.close_all_db_handles()


# ----------------------------------------------------------------- fix 3: the fleet-visible count


@pytest.mark.asyncio
async def test_paused_person_turn_count_reports_live_paused_sessions(tmp_path, monkeypatch):
    """What the daily fleet check reads: one per live session paused on a person's lost turn."""
    from gateway.session_person_unknown import paused_person_turn_count
    executed = []
    store, authority, _head_id, _f, _a = await _restart_losing_head(tmp_path, monkeypatch, executed)
    runner = SimpleNamespace(session_authority=authority)
    try:
        assert paused_person_turn_count(runner) == 1
        # Resolved the way /discard does: the count drops without anything else changing.
        rows = list_session_admissions(authority.db, session_id='s')
        lost = next(r for r in rows if r['status'] == 'unknown')
        await authority.resolve_unknown_as_owner(SessionRef('p', 's'), lost['admission_id'], lost['generation'])
        assert paused_person_turn_count(runner) == 0
    finally:
        store.close_all_db_handles()


@pytest.mark.asyncio
async def test_paused_count_ignores_an_unknown_turn_nobody_can_be_asked_about(tmp_path, monkeypatch):
    """An automation row fenced ``unknown`` settles itself (GW-10) and is nobody's silent chat:
    counting it would make the fleet check report a pause no person can clear."""
    from gateway.session_person_unknown import paused_person_turn_count
    store, runner = _runner(tmp_path, monkeypatch, [])
    db = store._db
    db.create_session('s', source='telegram')
    authority = await initialize_session_authority(runner, profile_id='p', instance_id='owner-1')
    authority.sessions['s'] = LiveSession(SessionSource(platform=Platform.TELEGRAM, chat_id='fle72'), 's')
    authority._schedule = lambda ref: None
    try:
        admit_session_input(db, epoch=authority.epoch, principal_id='automation:probe', session_id='s',
                            request_id='auto', payload={'text': 'HEARTBEAT'})
        row = claim_session_input(db, epoch=authority.epoch, session_id='s')
        db._execute_write(lambda c: c.execute(
            "UPDATE session_admissions SET status='unknown' WHERE admission_id=?", (row['admission_id'],)))
        assert paused_person_turn_count(SimpleNamespace(session_authority=authority)) == 0
    finally:
        store.close_all_db_handles()


def test_readiness_reports_the_paused_session_count(tmp_path, monkeypatch):
    """The readiness payload a fleet check scrapes carries the count, and a paused chat does not
    flip the gateway to ``degraded``: only the person in that chat can clear it."""
    from gateway.readiness import collect_runtime_readiness
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    monkeypatch.setattr('hermes_constants.get_hermes_home', lambda: tmp_path)
    result = collect_runtime_readiness(
        configured_model='m', runtime_status={'gateway_state': 'running', 'session_store': {'status': 'ok'}},
        paused_person_sessions=3)
    assert result['checks']['paused_sessions'] == {'status': 'ok', 'unknown_person_turns': 3}
    assert result['status'] == 'ok'

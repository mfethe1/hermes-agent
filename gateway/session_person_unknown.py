"""A person's lost turn settles itself once that same person is demonstrably still waiting.

A turn lost across an owner restart is fenced ``unknown`` and pauses its session's FIFO, because
only the person who sent it knows whether to resend it (GW-10, ``session_unattended_unknown``
settles the deliveries nobody can be asked about). The way out is ``/discard`` in that chat, and
``session_ingress.pause_notice`` offers it once per pause episode (``live.pause_notified``).

That "once" is what broke on 2026-11-02: three live Telegram topics each lost a turn to one
gateway restart, each sent its one notice, and every later message was then admitted in silence
behind the paused FIFO. One topic took the same question five times. A person who keeps typing
instead of reading a notice they already scrolled past gets nothing back at all, and only another
person noticing the silence ends it.

A queued message from a person IS the acknowledgement the pause was waiting for: they are present,
and they want an answer now. So the lost turn settles through exactly the resolution ``/discard``
uses (``resolve_unknown_session_input`` via ``SessionAuthority.resolve_unknown_as_owner``) —
terminal, never requeued, never replayed — the FIFO runs, and the reply to the message that
unblocked it carries a one-time notice naming what was dropped.

Three rules hold this to the narrow case:

* **No person queued behind it, nothing changes.** ``person_waiting_on_pause`` is false, the pause
  and its ``/discard`` contract stand exactly as before. This module never settles a lost turn on
  a timer or on its own judgement.
* **One answer per burst.** Identical text the same person queued in a row while paused is
  answered once (``superseded_duplicates``); the earlier copies are cancelled, which settles their
  observers with no reply rather than sending the same answer five times.
* **Queued automation is disclosed, not hidden and not dropped.** Heartbeats and standing-goal
  continuations already committed to this FIFO keep their order and still run; the notice says how
  many, so a stale ``[Continuing toward your standing goal]`` never surfaces behind a person's
  message as if it were fresh.
"""
from __future__ import annotations

import asyncio
import itertools
import logging

from gateway.session_unattended_unknown import is_unattended

logger = logging.getLogger(__name__)

# The quoted first line of the lost message, long enough to recognise a question and short enough
# to stay one line in a chat bubble.
_QUOTE_LIMIT = 160


def _is_person(row) -> bool:
    """True for an admission a person submitted, so somebody can be asked (or is waiting)."""
    return not is_unattended(row)


def lost_person_turns(rows):
    """The session's ``unknown`` turns a person submitted, in FIFO order: what pauses it."""
    return [row for row in rows if row['status'] == 'unknown' and _is_person(row)]


def waiting_person_rows(rows):
    """Queued admissions a person submitted: the proof that somebody is still waiting here."""
    return [row for row in rows if row['status'] == 'queued' and _is_person(row)]


def queued_automation_rows(rows):
    """Queued admissions no person submitted — heartbeats, standing-goal continuations, process
    completions. Disclosed in the notice; never cancelled here, because a process completion is
    information the person may want and only its sender knows whether it still matters."""
    return [row for row in rows if row['status'] == 'queued' and not _is_person(row)]


def _row_text(row) -> str:
    return str((row['payload'] or {}).get('text') or '')


def _dedupe_key(row):
    return row['principal_id'], _row_text(row)


def superseded_duplicates(rows):
    """Earlier copies of identical text the same person queued in a row, keeping the last.

    The last copy is kept, not the first: it is the message whose live delivery waiter is still
    parked (the one that just unblocked the FIFO), so the single answer threads to the person's
    most recent attempt instead of to a question they have already given up on. Adjacency is read
    over the queued rows in FIFO order, so an automation row committed between two identical
    messages breaks the run and both are answered.
    """
    queued = [row for row in rows if row['status'] == 'queued']
    superseded = []
    for current, following in itertools.pairwise(queued):
        if not _is_person(current) or not _row_text(current):
            continue
        if _dedupe_key(current) == _dedupe_key(following):
            superseded.append(current)
    return superseded


def person_waiting_on_pause(rows) -> bool:
    """True when this session is paused on a person's lost turn AND that person has a message
    queued behind it. The single predicate that decides whether today's pause still applies."""
    return bool(lost_person_turns(rows)) and bool(waiting_person_rows(rows))


def lost_turn_notice(lost, automation_queued, superseded=0) -> str:
    """The one-time chat copy for the reply that unblocked the FIFO.

    Names the lost turn and quotes its first line, because the person is being told about a
    message they sent long enough ago to have scrolled past, and "a previous turn" alone does not
    tell them which. Says plainly that it was not re-run, so nothing is assumed to have happened.
    """
    first = next((line.strip() for line in _row_text(lost[0]).splitlines() if line.strip()), '')
    quote = first if len(first) <= _QUOTE_LIMIT else first[:_QUOTE_LIMIT - 1].rstrip() + '…'
    extra = '' if len(lost) == 1 else f' ({len(lost)} turns were lost; this was the first.)'
    # Plain text, no markdown: this rides the ordinary reply path onto every messaging platform,
    # and ``pause_notice`` next to it does the same rather than trust per-platform rendering.
    parts = ['⚠️ Hermes restarted while answering an earlier message of yours, so that turn was '
             'lost and has now been dropped. It was not re-run, so send it again if you '
             f'still need it.{extra}',
             f'Lost message: “{quote}”' if quote else 'The lost message had no text.']
    if superseded:
        parts.append(f'You sent the same thing {superseded + 1} times while this conversation was '
                     'paused; answering once.')
    if automation_queued:
        parts.append(f'{len(automation_queued)} automated item(s) were already queued in this '
                     'conversation before the restart (a heartbeat or a standing-goal '
                     'continuation). They keep their place in the queue and still run, so a reply '
                     'you did not ask for may follow this one.')
    return '\n'.join(parts)


def take_lost_turn_notice(live, row) -> str | None:
    """Consume the pending notice for *row*'s reply, once, and only for a person's turn.

    Read at the point the chat-facing reply is assembled rather than when the turn is settled: the
    notice has to ride a message the person actually receives, and an automation row that happens
    to be claimed first would bury it in a reply they never asked for.
    """
    notice = getattr(live, 'lost_turn_notice', None)
    if notice is None or not _is_person(row):
        return None
    live.lost_turn_notice = None
    return notice


async def _drop_superseded_duplicates(authority, ref) -> int:
    """Cancel the earlier copies through the owner's own cancel path, so every observer of each
    dropped row settles (``_settle_cancelled`` resolves a live waiter with no reply) instead of
    waiting for a turn that will never run.

    Shielded in a tracked task exactly as ``cancel_queued`` does: a drain cancelled between the
    cancel's commit and its observer settlement would otherwise leave a terminal row whose waiting
    adapter task is never released. Authorization is not re-checked — this is the owner's own
    cancel of a row it just read, like ``resolve_unknown_as_owner`` beside it.
    """
    from gateway.session_ingress_media import release_admission_media
    from gateway.session_runtime_workers import track_mutation
    from hermes_state_runtime import RuntimeStoreError, list_session_admissions
    rows = await asyncio.to_thread(list_session_admissions, authority.db, session_id=ref.session_id)
    dropped = 0
    for row in superseded_duplicates(rows):
        try:
            await asyncio.shield(track_mutation(authority, authority._commit_cancel(
                ref, row['admission_id'], authority._receipt(row))))
        except RuntimeStoreError as exc:
            logger.warning('Duplicate admission %s left queued: %s', row['admission_id'], exc.reason)
            continue
        await asyncio.to_thread(release_admission_media, authority.db, row['admission_id'])
        dropped += 1
    return dropped


async def resume_for_waiting_person(authority, ref) -> bool:
    """Settle the person's lost turn(s) because that person has a message queued behind them.

    True when any settled, so the caller re-reads its FIFO instead of pausing. False leaves today's
    behaviour untouched: the session stays paused for ``/discard``.
    """
    from hermes_state_runtime import RuntimeStoreError, list_session_admissions
    rows = await asyncio.to_thread(list_session_admissions, authority.db, session_id=ref.session_id)
    if not person_waiting_on_pause(rows):
        return False
    settled = []
    for row in lost_person_turns(rows):
        try:
            # The ``/discard`` resolution exactly: no ``notice``, so the transcript gets the same
            # hedged boundary a person's own discard writes. The chat copy is this module's job.
            await authority.resolve_unknown_as_owner(ref, row['admission_id'], row['generation'])
        except RuntimeStoreError as exc:
            logger.warning('Lost turn %s left unknown: %s', row['admission_id'], exc.reason)
            continue
        settled.append(row)
    if not settled:
        return False
    superseded = await _drop_superseded_duplicates(authority, ref)
    remaining = await asyncio.to_thread(list_session_admissions, authority.db, session_id=ref.session_id)
    # ``.get``: the resolutions above await, and a delete can retire this live entry while they run.
    # The settlements are committed either way, so the drain still re-reads; only the chat copy is lost.
    live = authority.sessions.get(ref.session_id)
    if live is not None:
        live.lost_turn_notice = lost_turn_notice(settled, queued_automation_rows(remaining), superseded)
        # The FIFO is moving again, so the next pause is a new episode and gets its own notice.
        live.pause_notified = False
    logger.info('Session %s resumed: %d lost turn(s) settled for a waiting sender, %d duplicate(s) dropped',
                ref.session_id, len(settled), superseded)
    return True


def paused_person_turn_count(runner) -> int:
    """Live sessions whose FIFO is paused on a person's ``unknown`` turn, across every authority
    this process serves: what a fleet check reports so a silent chat is visible without somebody
    opening it. One query per authority over the ``unknown`` rows, not per session: this is read
    from a readiness probe that must stay bounded and must never compete with normal writers."""
    from gateway.session_authorities import all_authorities
    from hermes_state_runtime import _row
    paused = 0
    for authority in all_authorities(runner):
        live = getattr(authority, 'sessions', None) or {}
        if not live:
            continue
        try:
            with authority.db._read_ctx() as conn:
                rows = [_row(r) for r in conn.execute(
                    "SELECT * FROM session_admissions WHERE status='unknown'").fetchall()]
        except Exception:  # a readiness probe reports what it can; a degraded store is its own check
            logger.debug('Paused-session count skipped for profile %s', getattr(authority, 'profile_id', '?'),
                         exc_info=True)
            continue
        paused += len({row['target_session_id'] for row in rows
                       if _is_person(row) and row['target_session_id'] in live})
    return paused

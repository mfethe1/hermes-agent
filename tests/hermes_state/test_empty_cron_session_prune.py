"""ST-7: empty cron sessions are pruned as ghosts once no fire can still use them.

A canonical cron fire creates its session before the run's gates; a gated fire from before the
run_job fix left it open and empty forever (2,316 such rows by 2026-10-24), and the ghost prune
only ever matched TUI rows. The gateway, the store's single writer, prunes them on its
maintenance tick whatever ``sessions:`` says.
"""
import time
from contextlib import closing

import hermes_state_runtime as rt
from hermes_state import SessionDB

HOUR = 3600


def _cron(db, sid, *, age, messages=0, ended=False, title=None, parent=None, end_reason="cron_noop"):
    db.create_session(sid, source="cron", parent_session_id=parent)
    for i in range(messages):
        db.append_message(sid, role="user", content=f"m{i}")
    if title:
        db.set_session_title(sid, title)
    if ended:
        db.end_session(sid, end_reason)
    stamp = time.time() - age
    db._execute_write(lambda c: c.execute("UPDATE sessions SET started_at=? WHERE id=?", (stamp, sid)))


def _settled_fire(db, epoch, sid):
    accepted = rt.admit_session_input(db, epoch=epoch, principal_id="cron-owner", session_id=sid,
                                      request_id=f"cron:job:{sid}", payload={"text": ""})
    claim = rt.claim_session_input(db, epoch=epoch, session_id=sid)
    rt.settle_session_input(db, epoch=epoch, admission_id=accepted["admission_id"],
                            generation=claim["generation"], outcome="completed")
    return accepted["admission_id"]


def test_empty_cron_sessions_older_than_an_hour_are_pruned(tmp_path):
    with closing(SessionDB(tmp_path / "state.db")) as db:
        _cron(db, "open_ghost", age=2 * HOUR)  # the ST-7 backlog: never ended
        _cron(db, "ended_noop", age=2 * HOUR, ended=True)  # what a gated fire leaves now
        _cron(db, "fresh", age=10 * 60)  # its fire may not have reached the agent yet
        _cron(db, "ran", age=2 * HOUR, messages=1)  # a real run keeps its transcript
        _cron(db, "titled", age=2 * HOUR, title="kept on purpose")
        db.create_session("cli_empty", source="cli")
        db._execute_write(lambda c: c.execute(
            "UPDATE sessions SET started_at=? WHERE id='cli_empty'", (time.time() - 2 * HOUR,)))

        assert db.prune_empty_cron_sessions() == 2

        assert db.get_session("open_ghost") is None
        assert db.get_session("ended_noop") is None
        for kept in ("fresh", "ran", "titled", "cli_empty"):
            assert db.get_session(kept) is not None, kept


def test_empty_cron_session_with_live_work_is_kept(tmp_path):
    with closing(SessionDB(tmp_path / "state.db")) as db:
        epoch = rt.begin_runtime_epoch(db, instance_id="owner")
        _cron(db, "leased", age=2 * HOUR)
        assert db.try_acquire_session_turn_lease("leased", "cron-turn", ttl_seconds=300)
        _cron(db, "queued", age=2 * HOUR)
        rt.admit_session_input(db, epoch=epoch, principal_id="cron-owner", session_id="queued",
                               request_id="fire", payload={"text": ""})
        _cron(db, "started", age=2 * HOUR)
        rt.admit_session_input(db, epoch=epoch, principal_id="cron-owner", session_id="started",
                               request_id="fire", payload={"text": ""})
        assert rt.claim_session_input(db, epoch=epoch, session_id="started")["status"] == "started"

        assert db.prune_empty_cron_sessions() == 0

        for kept in ("leased", "queued", "started"):
            assert db.get_session(kept) is not None, kept


def test_an_empty_compression_segment_is_lineage_not_a_ghost(tmp_path):
    """A compressed cron run leaves its earlier segments ended 'compression', untitled and empty,
    with the transcript on the tip (33 such rows on the audited store). Deleting one breaks the
    chain, and its child's foreign key failed the DELETE, so one chain made every pass prune nothing."""
    with closing(SessionDB(tmp_path / "state.db")) as db:
        _cron(db, "seg1", age=3 * HOUR, ended=True, end_reason="compression")
        _cron(db, "seg2", age=3 * HOUR, ended=True, end_reason="compression", parent="seg1")
        _cron(db, "tip", age=3 * HOUR, messages=2, parent="seg2")
        _cron(db, "ghost", age=2 * HOUR)

        assert db.prune_empty_cron_sessions() == 1

        assert db.get_session("ghost") is None
        for kept in ("seg1", "seg2", "tip"):
            assert db.get_session(kept) is not None, kept


def test_a_pass_reclaims_the_deletion_tombstones_it_left_once_past_the_retry_horizon(tmp_path):
    """Each pruned fire leaves its terminal admission as a tombstone and a sweep marker in state_meta.
    A recurring prune that never reclaimed them would trade ~870 session rows a day for more rows."""
    from hermes_state_mutation_retirement import RETRY_HORIZON_S, retired_session

    with closing(SessionDB(tmp_path / "state.db")) as db:
        epoch = rt.begin_runtime_epoch(db, instance_id="owner")
        _cron(db, "ghost", age=2 * HOUR)
        admission_id = _settled_fire(db, epoch, "ghost")

        assert db.prune_empty_cron_sessions() == 1
        assert rt.get_session_admission(db, admission_id=admission_id)["status"] == "terminal"
        assert retired_session(db, "ghost")

        aged = time.time() - RETRY_HORIZON_S - HOUR
        db._execute_write(lambda c: c.execute(
            "UPDATE state_meta SET value=json_set(value, '$.retired_at', ?) "
            "WHERE key GLOB 'gateway.terminal_admission.v1.*' OR key GLOB 'gateway.retired_session.v1.*'",
            (aged,)))
        assert db.prune_empty_cron_sessions() == 0

        assert rt.get_session_admission(db, admission_id=admission_id) is None
        assert not retired_session(db, "ghost")


def test_gateway_maintenance_prunes_cron_ghosts_with_auto_prune_off():
    """The prune runs where maintenance runs: the gateway's state.db tick (startup and hourly), not the
    classic CLI's startup repairs, which nothing in production calls. auto_prune: false (the default,
    and the audited store's setting) used to return before any maintenance."""
    from gateway.run import _housekeeping_state_db_maintenance
    from hermes_constants import get_hermes_home

    home = get_hermes_home()
    (home / "config.yaml").write_text("sessions:\n  auto_prune: false\n  auto_archive: false\n", encoding="utf-8")
    transcript = home / "sessions" / "ghost.jsonl"
    transcript.parent.mkdir(parents=True, exist_ok=True)
    transcript.write_text("{}\n", encoding="utf-8")
    with closing(SessionDB(home / "state.db")) as db:
        epoch = rt.begin_runtime_epoch(db, instance_id="owner")
        _cron(db, "ghost", age=2 * HOUR)
        _settled_fire(db, epoch, "ghost")
        _cron(db, "queued", age=2 * HOUR)
        rt.admit_session_input(db, epoch=epoch, principal_id="cron-owner", session_id="queued",
                               request_id="fire", payload={"text": ""})
        _cron(db, "started", age=2 * HOUR)
        rt.admit_session_input(db, epoch=epoch, principal_id="cron-owner", session_id="started",
                               request_id="fire", payload={"text": ""})
        rt.claim_session_input(db, epoch=epoch, session_id="started")

        _housekeeping_state_db_maintenance()

        assert db.get_session("ghost") is None
        assert not transcript.exists()
        assert db.get_session("queued") is not None
        assert db.get_session("started") is not None

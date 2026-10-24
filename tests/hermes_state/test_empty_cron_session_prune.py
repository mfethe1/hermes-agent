"""ST-7: empty cron sessions are pruned as ghosts once no fire can still use them.

A canonical cron fire creates its session before the run's gates; a gated fire from before the
run_job fix left it open and empty forever (2,316 such rows by 2026-10-24), and the ghost prune
only ever matched TUI rows.
"""
import time
from contextlib import closing

import hermes_state_runtime as rt
from hermes_state import SessionDB

HOUR = 3600


def _cron(db, sid, *, age, messages=0, ended=False, title=None):
    db.create_session(sid, source="cron")
    for i in range(messages):
        db.append_message(sid, role="user", content=f"m{i}")
    if title:
        db.set_session_title(sid, title)
    if ended:
        db.end_session(sid, "cron_noop")
    stamp = time.time() - age
    db._execute_write(lambda c: c.execute("UPDATE sessions SET started_at=? WHERE id=?", (stamp, sid)))


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

        assert db.prune_empty_ghost_sessions() == 2

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

        assert db.prune_empty_ghost_sessions() == 0

        assert db.get_session("leased") is not None
        assert db.get_session("queued") is not None


def test_a_store_that_ran_the_tui_only_prune_runs_the_cron_extension_once(monkeypatch, tmp_path):
    """The ghost prune is a one-time startup repair latched in state_meta. Every existing store already
    latched the TUI-only version, so the extension needs a new latch or the backlog is never pruned."""
    from unittest.mock import MagicMock

    import cli
    import hermes_cli.config
    import hermes_constants

    monkeypatch.setattr(hermes_cli.config, "load_config", lambda: {"sessions": {"auto_prune": False}})
    monkeypatch.setattr(hermes_constants, "get_hermes_home", lambda: tmp_path)
    meta = {"ghost_session_prune_v1": "1", "orphaned_compression_finalize_v1": "1"}
    session_db = MagicMock()
    session_db.get_meta.side_effect = meta.get
    session_db.set_meta.side_effect = meta.__setitem__
    session_db.prune_empty_ghost_sessions.return_value = 2316

    cli._run_state_db_auto_maintenance(session_db)
    cli._run_state_db_auto_maintenance(session_db)

    assert session_db.prune_empty_ghost_sessions.call_count == 1

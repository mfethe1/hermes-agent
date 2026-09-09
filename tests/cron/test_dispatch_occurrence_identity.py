"""Dispatch occurrence identity must survive the built-in tick's pre-advance (#79531).

``scheduler.tick`` snapshots ``_scheduled_instant`` in ``get_due_jobs``, then calls
``advance_next_runs`` BEFORE ``claim_job_for_fire``. Re-deriving the occurrence from the
already-advanced record inside the claim (a) mislabels a ``trigger_job`` manual run as the NEXT
scheduled slot and (b) refuses it outright when that slot is already recorded completed. Manual
tool/CLI runs likewise must never stamp a future scheduled slot as completed.

Real store, real executions ledger, temp HERMES_HOME; only ``run_one_job`` (the executor
boundary) is stubbed where a tick is driven.
"""
import threading

import pytest


@pytest.fixture
def temp_home(tmp_path, monkeypatch):
    monkeypatch.setenv("HERMES_HOME", str(tmp_path))
    monkeypatch.delenv("HERMES_MACHINE_ID", raising=False)
    yield tmp_path


def _seed_completed_next_slot(job):
    """A prior run stamped against the job's UPCOMING cron slot (the live-DB shape)."""
    from cron import executions, jobs
    from cron.occurrences import scheduled_instant

    nxt = jobs.compute_next_run(job["schedule"], jobs._hermes_now().isoformat())
    instant = scheduled_instant(nxt)
    row = executions.create_execution(job["id"], source="direct", scheduled_instant=instant)
    executions.mark_execution_running(row["id"])
    executions.finish_execution(row["id"], success=True)
    return instant


def _trigger_and_snapshot(job):
    from cron import jobs

    jobs.trigger_job(job["id"])
    due = [j for j in jobs.get_due_jobs() if j["id"] == job["id"]]
    assert due and due[0]["_scheduled_instant"] is None
    return due[0]


def test_builtin_order_claim_keeps_manual_identity_after_advance(temp_home):
    """advance -> claim (tick order) with the next slot pre-completed still claims, as manual."""
    from cron import jobs

    job = jobs.create_job(name="A", prompt="noop", schedule="* * * * *")
    _seed_completed_next_slot(job)
    snap = _trigger_and_snapshot(job)
    jobs.advance_next_runs([job["id"]])

    claimed = jobs.claim_job_for_fire(
        job["id"], return_job=True, occurrence=snap["_scheduled_instant"])
    assert isinstance(claimed, dict), "manual dispatch refused because NEXT slot was completed"
    assert claimed["_scheduled_instant"] is None
    assert claimed["fire_claim"]


def test_builtin_order_claim_does_not_borrow_future_slot_identity(temp_home):
    from cron import jobs
    from cron.occurrences import scheduled_instant

    job = jobs.create_job(name="B", prompt="noop", schedule="* * * * *")
    snap = _trigger_and_snapshot(job)
    jobs.advance_next_runs([job["id"]])
    future = scheduled_instant(jobs.get_job(job["id"])["next_run_at"])

    claimed = jobs.claim_job_for_fire(
        job["id"], return_job=True, occurrence=snap["_scheduled_instant"])
    assert isinstance(claimed, dict)
    assert claimed["_scheduled_instant"] is None
    assert claimed["_scheduled_instant"] != future


def test_process_due_job_starts_manual_run_when_next_slot_completed(temp_home, monkeypatch):
    """Built-in tick path (_submit_with_guard shape -> _process_due_job) reaches run_one_job."""
    from cron import executions, jobs, scheduler

    job = jobs.create_job(name="tick", prompt="noop", schedule="* * * * *")
    _seed_completed_next_slot(job)
    snap = _trigger_and_snapshot(job)
    jobs.advance_next_runs([job["id"]])
    execution = executions.create_execution(
        job["id"], source="builtin", scheduled_instant=snap.get("_scheduled_instant"))
    dispatched = dict(snap, execution_id=execution["id"])

    seen = {}

    def fake_run_one_job(claimed_job, **_kw):
        seen["job"] = claimed_job
        return True

    monkeypatch.setattr(scheduler, "run_one_job", fake_run_one_job)
    assert scheduler._process_due_job(dispatched, None, None, False) is True
    assert "job" in seen, "execution finished as 'Fire claim lost' without starting"
    assert seen["job"]["_scheduled_instant"] is None
    row = executions.get_execution(execution["id"])
    assert row["status"] != "failed", row


def test_full_tick_dispatches_triggered_job_with_completed_next_slot(temp_home, monkeypatch):
    """Real scheduler.tick(sync=True): get_due_jobs -> advance -> claim -> run_one_job."""
    from cron import executions, jobs, scheduler

    job = jobs.create_job(name="fulltick", prompt="noop", schedule="* * * * *")
    _seed_completed_next_slot(job)
    jobs.trigger_job(job["id"])
    seen = []

    def fake_run_one_job(claimed_job, **_kw):
        seen.append(claimed_job)
        jobs.mark_job_run(claimed_job["id"], True,
                          expected_fire_owner=claimed_job["fire_claim"]["by"])
        executions.finish_execution(claimed_job["execution_id"], success=True)
        return True

    monkeypatch.setattr(scheduler, "run_one_job", fake_run_one_job)
    assert scheduler.tick(verbose=False, sync=True) >= 1
    assert len(seen) == 1, "triggered job never reached the executor boundary"
    assert seen[0]["_scheduled_instant"] is None
    rows = [r for r in executions.list_executions(job_id=job["id"]) if r["source"] == "builtin"]
    assert rows and all(r["status"] == "completed" for r in rows), rows
    assert all(r["scheduled_instant"] is None for r in rows)


def test_refused_claim_records_accurate_reason(temp_home, monkeypatch):
    """A paused job losing its claim is recorded as such, not as a lost claim."""
    from cron import executions, jobs, scheduler

    job = jobs.create_job(name="paused", prompt="noop", schedule="* * * * *")
    snap = _trigger_and_snapshot(job)
    execution = executions.create_execution(job["id"], source="builtin")
    dispatched = dict(snap, execution_id=execution["id"])
    jobs.pause_job(job["id"])
    monkeypatch.setattr(scheduler, "run_one_job",
                        lambda *a, **k: pytest.fail("must not run a paused job"))
    assert scheduler._process_due_job(dispatched, None, None, False) is True
    row = executions.get_execution(execution["id"])
    assert row["status"] == "failed"
    assert "lost" not in (row["error"] or "").lower()
    assert "paused" in (row["error"] or "").lower()


def test_explicit_scheduled_occurrence_still_dedupes(temp_home):
    """An explicit scheduled snapshot keeps completed-occurrence suppression."""
    from cron import executions, jobs
    from cron.occurrences import scheduled_instant

    job = jobs.create_job(name="dedupe", prompt="noop", schedule="* * * * *")
    slot = jobs.get_job(job["id"])["next_run_at"]
    instant = scheduled_instant(slot)
    row = executions.create_execution(job["id"], source="builtin", scheduled_instant=instant)
    executions.finish_execution(row["id"], success=True)

    assert jobs.claim_job_for_fire(job["id"], return_job=True, occurrence=instant) is False
    assert jobs.get_job(job["id"]).get("fire_claim") is None

    # A not-yet-completed explicit slot claims and carries exactly that identity.
    fresh = jobs.create_job(name="fresh", prompt="noop", schedule="* * * * *")
    fresh_slot = scheduled_instant(jobs.get_job(fresh["id"])["next_run_at"])
    claimed = jobs.claim_job_for_fire(fresh["id"], return_job=True, occurrence=fresh_slot)
    assert isinstance(claimed, dict)
    assert claimed["_scheduled_instant"] == fresh_slot


def test_omitted_occurrence_keeps_legacy_derivation(temp_home):
    from cron import jobs
    from cron.occurrences import scheduled_instant

    job = jobs.create_job(name="legacy", prompt="noop", schedule="* * * * *")
    slot = scheduled_instant(jobs.get_job(job["id"])["next_run_at"])
    claimed = jobs.claim_job_for_fire(job["id"], return_job=True)
    assert claimed["_scheduled_instant"] == slot

    manual = jobs.create_job(name="legacy-manual", prompt="noop", schedule="* * * * *")
    jobs.trigger_job(manual["id"])
    claimed = jobs.claim_job_for_fire(manual["id"], return_job=True)
    assert claimed["_scheduled_instant"] is None


def test_manual_tool_claim_never_consumes_scheduled_slot(temp_home):
    """tools/CLI manual run: manual identity even when the upcoming slot is completed."""
    from cron import jobs
    from tools.cronjob_tools import _claim_for_manual_run

    job = jobs.create_job(name="tool", prompt="noop", schedule="* * * * *")
    _seed_completed_next_slot(job)
    claimed, err = _claim_for_manual_run(job["id"], "test")
    assert err is None, err
    assert claimed["_scheduled_instant"] is None

    # A second run while the claim is live is refused with the in-flight reason.
    _, err = _claim_for_manual_run(job["id"], "test")
    assert err is not None and "already" in err["error"].lower()


def test_manual_claim_refuses_paused_job(temp_home):
    from cron import jobs

    job = jobs.create_job(name="p", prompt="noop", schedule="* * * * *")
    jobs.pause_job(job["id"])
    assert jobs.claim_job_for_fire(job["id"], return_job=True, occurrence=None) is False


def test_competing_manual_claims_exactly_one_wins(temp_home):
    from cron import jobs

    job = jobs.create_job(name="race", prompt="noop", schedule="* * * * *")
    jobs.trigger_job(job["id"])
    results = []
    barrier = threading.Barrier(4)

    def claimant():
        barrier.wait(timeout=5)
        results.append(jobs.claim_job_for_fire(job["id"], return_job=True, occurrence=None))

    threads = [threading.Thread(target=claimant) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=10)
    assert sum(1 for r in results if isinstance(r, dict)) == 1
    assert sum(1 for r in results if r is False) == 3


@pytest.mark.parametrize("invalid", ["bad", "2026-09-09T12:00:00", 123, ""])
def test_invalid_explicit_identity_cannot_become_manual(temp_home, invalid):
    from cron import jobs

    job = jobs.create_job(name="invalid", prompt="noop", schedule="* * * * *")
    before = jobs.get_job(job["id"])
    with pytest.raises(ValueError, match="explicit occurrence"):
        jobs.claim_job_for_fire(job["id"], return_job=True, occurrence=invalid)
    assert jobs.get_job(job["id"]) == before


def test_refused_scheduled_claim_does_not_invent_completion(temp_home, monkeypatch):
    from cron import executions, jobs, scheduler
    from cron.occurrences import scheduled_instant

    job = jobs.create_job(name="refused", prompt="noop", schedule="* * * * *")
    instant = scheduled_instant(job["next_run_at"])
    row = executions.create_execution(job["id"], source="builtin", scheduled_instant=instant)
    dispatched = dict(job, execution_id=row["id"], _scheduled_instant=instant)
    monkeypatch.setattr(scheduler, "claim_job_for_fire", lambda *a, **kw: False)
    assert scheduler._process_due_job(dispatched, None, None, False)
    assert "completed" not in executions.get_execution(row["id"])["error"]

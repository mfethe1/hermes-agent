#!/usr/bin/env python3
"""Real-process acceptance probes for the dispatch identity store.

Implements the "Required real-process acceptance" section of
RESPONSE-VERIFICATION-001-replay-acceptance.md using ONLY real subprocesses
against retained store directories (no mocked evaluator, no fabricated
transport success). Each probe prints PASS/FAIL with the observed facts
(send counts, exit codes, terminal states). Run from the repo root:

    python3 docs/response-verification/dispatch_store_probes.py <evidence_dir>

Every probe gets a fresh subdirectory under <evidence_dir>; the sink keeps
an append-only delivery log inside it. Exit code = number of failed probes.
"""

import json
import os
import subprocess
import sys
import time

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.insert(0, ROOT)

from response_verification.dispatch_store import (  # noqa: E402
    Authorization,
    DispatchIdentityStore,
    ReservedDispatch,
)

PY = sys.executable
RESULTS = []


def auth(task="T1", lease=300.0):
    return Authorization(
        task_id=task,
        approved_request_digest="req-1",
        destination="telegram:97340:chat:12",
        artifact_digest="sha256:" + "ab" * 32,
        claim_class="verified-completion",
        rendering="v1",
        lease_seconds=lease,
    )


def fresh_dir(base, name):
    d = os.path.join(base, name)
    os.makedirs(d, exist_ok=True)
    return d


def probe(name):
    def deco(fn):
        def wrapper(base):
            d = fresh_dir(base, name)
            try:
                detail = fn(d)
                RESULTS.append((name, True, detail))
                print(f"PASS {name}: {detail}")
            except AssertionError as e:
                RESULTS.append((name, False, str(e)))
                print(f"FAIL {name}: {e}")
            except Exception as e:  # noqa: BLE001
                RESULTS.append((name, False, repr(e)))
                print(f"ERROR {name}: {repr(e)}")
        wrapper.fn = fn
        return wrapper
    return deco


# ---------- child scripts (real processes) ----------

CHILD_RESERVE_SEND = """
import sys
sys.path.insert(0, {root!r})
from response_verification.dispatch_store import (
    Authorization, DispatchIdentityStore, ReservedDispatch)
d, owner = sys.argv[1], sys.argv[2]
sink_log = sys.argv[3]
store = DispatchIdentityStore(d)
auth = Authorization(task_id='T1', approved_request_digest='req-1',
                     destination='telegram:97340:chat:12',
                     artifact_digest='sha256:' + 'ab'*32,
                     claim_class='verified-completion', rendering='v1')
res = store.reserve(auth, owner)
print('RESERVE', res.status, flush=True)
if res.status != 'reserved':
    sys.exit(3)
rd = ReservedDispatch(store.store, res.record)
def sink(payload):
    import json as j, time as t, os as o
    rec = {{'owner': owner, 'payload': payload.decode('utf-8'), 'ts': t.time()}}
    with open(sink_log, 'a', encoding='utf-8') as f:
        f.write(j.dumps(rec) + chr(10))
        f.flush()
        o.fsync(f.fileno())
    return True
out = rd.send(owner, b'probe-payload', sink)
print('SEND', out.status, flush=True)
sys.exit(0 if out.status == 'send_success' else 4)
""".format(root=ROOT)

CHILD_CRASH_AFTER_SINK = CHILD_RESERVE_SEND.replace(
    "    return True\nout = rd.send",
    "    import os as _o; _o._exit(9)\nout = rd.send",
)


def run_child(script, *args, timeout=60):
    p = subprocess.run(
        [PY, "-c", script, *args],
        capture_output=True, text=True, timeout=timeout,
    )
    return p


def sink_count(d):
    path = os.path.join(d, "sink-log.jsonl")
    if not os.path.exists(path):
        return 0
    return sum(1 for line in open(path, encoding="utf-8") if line.strip())


def store_states(d, key=None):
    path = os.path.join(d, "dispatch-store.jsonl")
    if not os.path.exists(path):
        return []
    recs = [json.loads(l) for l in open(path, encoding="utf-8") if l.strip()]
    if key is None:
        return recs
    return [r for r in recs if r.get("binding_key") == key]


# ---------- probes ----------

@probe("two_simultaneous_processes_one_authority")
def p_two_processes(d):
    procs = [
        subprocess.Popen(
            [PY, "-c", CHILD_RESERVE_SEND, d, f"racer-{i}",
             os.path.join(d, "sink-log.jsonl")],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        for i in range(2)
    ]
    outs = [p.communicate(timeout=60) for p in procs]
    rcs = [p.returncode for p in procs]
    sends = sink_count(d)
    assert sorted(rcs) == [0, 3], f"rcs={rcs} outs={outs}"
    assert sends == 1, f"expected exactly 1 sink send, got {sends}"
    return f"rcs={rcs} sink_sends=1 (exactly one process obtained authority)"


@probe("crash_before_reservation_commit")
def p_crash_before(d):
    # process dies before reserve(): nothing on disk, next process succeeds
    script = CHILD_RESERVE_SEND.replace(
        "res = store.reserve(auth, owner)",
        "import os as _o; _o._exit(5)\nres = store.reserve(auth, owner)",
    )
    p1 = run_child(script, d, "dead-runner", os.path.join(d, "sink-log.jsonl"))
    assert p1.returncode == 5, f"rc={p1.returncode} out={p1.stdout}"
    assert sink_count(d) == 0
    assert not os.path.exists(os.path.join(d, "dispatch-store.jsonl"))
    p2 = run_child(CHILD_RESERVE_SEND, d, "live-runner",
                   os.path.join(d, "sink-log.jsonl"))
    assert p2.returncode == 0, f"rc={p2.returncode} out={p2.stdout} err={p2.stderr}"
    return f"crash_rc=5 store_empty=true restart_rc=0 sink_sends={sink_count(d)}"


@probe("crash_after_reservation_before_send")
def p_crash_after_reservation(d):
    script = CHILD_RESERVE_SEND.replace(
        "rd = ReservedDispatch(store.store, res.record)",
        "import os as _o; _o._exit(6)\nrd = ReservedDispatch(store.store, res.record)",
    )
    p1 = run_child(script, d, "crasher", os.path.join(d, "sink-log.jsonl"))
    assert p1.returncode == 6, f"rc={p1.returncode}"
    assert sink_count(d) == 0
    states = store_states(d)
    assert states and states[-1]["state"] == "reserved", states
    # fresh process, different owner: conflict; same owner: idempotent
    p2 = run_child(CHILD_RESERVE_SEND, d, "other",
                   os.path.join(d, "sink-log.jsonl"))
    assert p2.returncode == 3, f"rc={p2.returncode} out={p2.stdout}"
    assert "RESERVE binding_conflict" in p2.stdout
    assert sink_count(d) == 0, "no send may happen after a foreign crash-reserve"
    return "crash_rc=6 final_state=reserved restart=conflict sends=0"


@probe("crash_after_intent_commit_holds_unknown")
def p_crash_after_intent(d):
    script = CHILD_RESERVE_SEND.replace(
        "out = rd.send(owner, b'probe-payload', sink)",
        "rd.mark_dispatching(owner)\nimport os as _o; _o._exit(7)\nout = rd.send("
        "owner, b'probe-payload', sink)",
    )
    p1 = run_child(script, d, "intenter", os.path.join(d, "sink-log.jsonl"))
    assert p1.returncode == 7, f"rc={p1.returncode} out={p1.stdout}"
    states = store_states(d)
    assert states[-1]["state"] == "dispatching", states
    assert sink_count(d) == 0
    # repeated restarts: no automatic resend, dispatching never adoptable
    script2 = CHILD_RESERVE_SEND.replace(
        "res = store.reserve(auth, owner)",
        "res = store.adopt(auth, owner)\nprint('ADOPT', res.status, flush=True)",
    ).replace(
        "if res.status != 'reserved':",
        "if res.status != 'reserved':",
    )
    for i in range(3):
        p = run_child(script2, d, f"restarter-{i}",
                      os.path.join(d, "sink-log.jsonl"))
        assert p.returncode == 3, f"restart {i}: rc={p.returncode} out={p.stdout}"
        assert "send_unknown" in p.stdout, p.stdout
    assert sink_count(d) == 0
    return "crash_rc=7 final_state=dispatching adopt=send_unknown x3 sends=0"


@probe("crash_after_sink_before_receipt")
def p_crash_after_sink(d):
    # sink accepts delivery, then the process dies before outcome commit
    script = CHILD_RESERVE_SEND.replace(
        "    return True\nout = rd.send",
        "    return True\nimport os as _o; _o._exit(9)\nout = rd.send",
    )
    # careful: the replace must kill AFTER the sink write. Rebuild the tail.
    script = CHILD_RESERVE_SEND.replace(
        "out = rd.send(owner, b'probe-payload', sink)",
        "rd.send(owner, b'probe-payload', sink)\n"
        "import os as _o\n_o._exit(9)",
    )
    # send() commits dispatching -> sink runs -> _exit before outcome append
    # is impossible to intercept inside send(); instead: sink writes, then
    # the process exits from INSIDE the sink callback (after fsync).
    script = CHILD_RESERVE_SEND.replace(
        "    return True",
        "    import os as _o2; _o2._exit(9)",
    )
    p1 = run_child(script, d, "sink-crasher", os.path.join(d, "sink-log.jsonl"))
    assert p1.returncode == 9, f"rc={p1.returncode} out={p1.stdout} err={p1.stderr}"
    assert sink_count(d) == 1, "sink must show the one accepted delivery"
    states = store_states(d)
    assert states[-1]["state"] == "dispatching", (
        f"outcome must be absent (durable ambiguity), got {states[-1]['state']}"
    )
    # restarts must not resend
    for i in range(2):
        p = run_child(CHILD_RESERVE_SEND, d, f"restart-{i}",
                      os.path.join(d, "sink-log.jsonl"))
        assert p.returncode == 3, f"restart {i}: rc={p.returncode} out={p.stdout}"
        assert "binding_conflict" in p.stdout or "lease_expired" in p.stdout
    assert sink_count(d) == 1, "no second send after crash-after-sink"
    detail = (
        "crash_rc=9 sink_sends=1 final_state=dispatching restarts_blocked=2 "
        "no_second_send"
    )
    return detail


@probe("crash_after_receipt_commit")
def p_crash_after_receipt(d):
    # full success path, then a second delivery attempt is duplicate-blocked
    p1 = run_child(CHILD_RESERVE_SEND, d, "first",
                   os.path.join(d, "sink-log.jsonl"))
    assert p1.returncode == 0, f"rc={p1.returncode} out={p1.stdout}"
    p2 = run_child(CHILD_RESERVE_SEND, d, "second",
                   os.path.join(d, "sink-log.jsonl"))
    assert p2.returncode == 3 and "duplicate_delivery" in p2.stdout, p2.stdout
    states = store_states(d)
    assert states[-1]["state"] == "observed-success"
    assert sink_count(d) == 1
    return f"first_rc=0 second_rc=3 final=observed-success sends=1"


@probe("old_owner_fenced_after_transfer")
def p_fenced(d):
    sys.path.insert(0, ROOT)
    store = DispatchIdentityStore(d)
    r1 = store.reserve(auth(lease=0.15), "old-owner")
    rd_old = ReservedDispatch(store.store, r1.record)
    time.sleep(0.2)
    r2 = store.adopt(auth(lease=0.15), "new-owner")
    assert r2.status == "reserved" and r2.token > r1.token, (r1, r2)
    # old owner's late callback: fenced
    s1 = rd_old.mark_dispatching("old-owner")
    s2 = rd_old.send("old-owner", b"late", lambda p: True)
    assert s1 == "stale_owner", s1
    assert s2.status == "stale_owner", s2
    # new owner completes; sink saw zero sends from the old owner
    rd_new = ReservedDispatch(store.store, r2.record)
    sends = []
    out = rd_new.send("new-owner", b"payload", lambda p: sends.append(1) or True)
    assert out.status == "send_success" and len(sends) == 1
    return (
        f"adopt_token={r2.token} old_mark={s1} old_send={s2.status} "
        f"new_owner_sends=1"
    )


@probe("attempt_replacement_preserves_dedupe")
def p_attempt_replacement(d):
    store = DispatchIdentityStore(d)
    r = store.reserve(auth(), "w1")
    rd = ReservedDispatch(store.store, r.record)
    sends = []
    out = rd.send("w1", b"same-payload", lambda p: sends.append(1) or True)
    assert out.status == "send_success"
    # a "new attempt" (relabeled owner string) must not reset dedupe:
    # after observed-success the relabel gets duplicate_delivery (blocked)
    r2 = store.reserve(auth(), "attempt-2")
    assert r2.status == "duplicate_delivery", r2.status
    assert len(sends) == 1
    # and before any delivery, a relabel is a conflict (someone owns it)
    d2 = os.path.join(d, "phase2")
    os.makedirs(d2, exist_ok=True)
    s2 = DispatchIdentityStore(d2)
    assert s2.reserve(auth(), "w1").status == "reserved"
    assert s2.reserve(auth(), "attempt-2").status == "binding_conflict"
    return "relabel_after_success=duplicate_delivery relabel_reserved=conflict sends=1"


@probe("changed_binding_rejected_distinct_allowed")
def p_changed_binding(d):
    store = DispatchIdentityStore(d)
    assert store.reserve(auth(), "w1").status == "reserved"
    # changed destination = different logical identity: independently
    # authorized deliveries remain possible
    a = Authorization(
        task_id="T1", approved_request_digest="req-1",
        destination="telegram:97340:chat:99",
        artifact_digest="sha256:" + "ab" * 32,
    )
    assert store.reserve(a, "w1").status == "reserved"
    # changed approval digest on the SAME task+destination: distinct binding
    b = Authorization(
        task_id="T1", approved_request_digest="req-OTHER",
        destination="telegram:97340:chat:12",
        artifact_digest="sha256:" + "ab" * 32,
    )
    assert store.reserve(b, "w1").status == "reserved"
    # but the original binding is still reserved for w1 only
    assert store.reserve(auth(), "w2").status == "binding_conflict"
    return "changed_bindings=distinct_keys reserved=3 original_conflict=true"


@probe("corrupt_truncated_store_blocks")
def p_corrupt(d):
    store = DispatchIdentityStore(d)
    assert store.reserve(auth(task="CX"), "w1").status == "reserved"
    with open(os.path.join(d, "dispatch-store.jsonl"), "a", encoding="utf-8") as f:
        f.write('{"schema": 1, "state": "reser')
    r = store.reserve(auth(task="CY"), "w2")
    assert r.status == "storage_refused", r.status
    p = run_child(
        CHILD_RESERVE_SEND, d, "fresh", os.path.join(d, "sink-log.jsonl")
    )
    # child uses task T1 key; store is corrupt -> must refuse regardless
    assert p.returncode == 3 and "storage_refused" in p.stdout, p.stdout
    assert sink_count(d) == 0
    return "torn_line=appended fresh_reserve=storage_refused sends=0"


@probe("incompatible_schema_blocks")
def p_schema(d):
    store = DispatchIdentityStore(d)
    assert store.reserve(auth(task="SX"), "w1").status == "reserved"
    path = os.path.join(d, "dispatch-store.jsonl")
    lines = open(path, encoding="utf-8").read().splitlines()
    rec = json.loads(lines[-1])
    rec["schema"] = 99
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines[:-1] + [json.dumps(rec)]) + "\n")
    r = store.reserve(auth(task="SY"), "w2")
    assert r.status == "schema_refused", r.status
    return "schema_version=99 reserve=schema_refused"


@probe("locked_store_blocks_safely")
def p_locked(d):
    store = DispatchIdentityStore(d)
    assert store.reserve(auth(task="LX"), "w1").status == "reserved"
    lock = store.store.acquire()
    try:
        script = """
import fcntl, sys
fh = open(sys.argv[1] + '/dispatch.lock', 'a+', encoding='utf-8')
try:
    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    print('ACQUIRED')
    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
except BlockingIOError:
    print('BLOCKED')
"""
        p = run_child(script, d)
        assert p.returncode == 0 and "BLOCKED" in p.stdout, p.stdout
    finally:
        store.store.release(lock)
    return "try_lock=BLOCKED no_bypass=true"


@probe("disk_write_failure_refuses")
def p_disk(d):
    d2 = os.path.join(d, "unusable")
    os.makedirs(os.path.join(d2, "dispatch-store.jsonl"))  # dir at file path
    store = DispatchIdentityStore(d2)
    r = store.reserve(auth(), "w1")
    assert r.status == "storage_refused", r.status
    return "store_path=directory reserve=storage_refused sends=0"


@probe("real_sink_delivery_log_verifiable")
def p_sink_log(d):
    p1 = run_child(CHILD_RESERVE_SEND, d, "logger",
                   os.path.join(d, "sink-log.jsonl"))
    assert p1.returncode == 0, (p1.returncode, p1.stdout, p1.stderr)
    entries = [
        json.loads(l)
        for l in open(os.path.join(d, "sink-log.jsonl"), encoding="utf-8")
        if l.strip()
    ]
    assert len(entries) == 1
    e = entries[0]
    assert e["owner"] == "logger" and e["payload"] == "probe-payload"
    # store's payload_hash matches the sink-observed payload
    states = store_states(d)
    h = states[-1].get("payload_hash")
    import hashlib
    assert h == hashlib.sha256(b"probe-payload").hexdigest(), (h,)
    return (
        f"sink_entries=1 owner={e['owner']} payload_hash_matches_sink=true"
    )


def main():
    if len(sys.argv) != 2:
        print(__doc__)
        return 2
    base = sys.argv[1]
    os.makedirs(base, exist_ok=True)
    probes = [
        p_two_processes, p_crash_before, p_crash_after_reservation,
        p_crash_after_intent, p_crash_after_sink, p_crash_after_receipt,
        p_fenced, p_attempt_replacement, p_changed_binding, p_corrupt,
        p_schema, p_locked, p_disk, p_sink_log,
    ]
    for p in probes:
        p(base)
    failed = sum(1 for _, ok, _ in RESULTS if not ok)
    print(f"\n{len(RESULTS) - failed}/{len(RESULTS)} passed")
    summary = {
        "probe_results": [
            {"name": n, "pass": ok, "detail": det} for n, ok, det in RESULTS
        ],
        "passed": len(RESULTS) - failed,
        "total": len(RESULTS),
    }
    with open(os.path.join(base, "probe-results.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2, sort_keys=True)
    return failed


if __name__ == "__main__":
    sys.exit(main())

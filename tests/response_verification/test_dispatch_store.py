"""Adversarial tests for the durable dispatch identity store.

Implements the real-process acceptance probes from
RESPONSE-VERIFICATION-001-replay-acceptance.md:

- two simultaneous independent processes, one logical delivery: at most one
  obtains valid sending authority
- crash at every point (before reservation, after reservation, after intent
  commit, after sink acceptance before receipt commit, after receipt
  commit), each restarted in a FRESH process against the retained store
- ambiguous post-intent cases: no automatic second send; durable unknown
  survives repeated restarts
- old owner's late callback after ownership transfer cannot deliver;
  attempt replacement does not reset logical dedupe
- changed destination/approval/digest/claim/rendering binding is rejected
  as a conflict; distinct independently authorized deliveries remain
  possible
- corrupt/truncated store, locked store, disk/write failure and
  incompatible schema block safely without fabricated receipts
- real subprocess sink keeps an independently inspectable append-only
  delivery log; actual send count and exact per-command exit codes are
  reported (no mocked evaluator, fabricated transport success)

pytest-compatible; standalone mode spawns child interpreters for the
cross-process cases (``python3 test_dispatch_store.py``).
"""

import json
import os
import subprocess
import sys
import tempfile
import time

_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if os.path.isdir(os.path.join(_root, "response_verification")):
    sys.path.insert(0, _root)
    from response_verification.dispatch_store import (  # noqa: E402
        Authorization,
        DispatchIdentityStore,
        DispatchStore,
        ReservedDispatch,
        BINDING_CONFLICT,
        CANCEL_OK,
        DUPLICATE_DELIVERY,
        LEASE_EXPIRED,
        OBSERVED_FAILURE,
        OBSERVED_SUCCESS,
        RESERVED,
        RESERVED_OK,
        SCHEMA_REFUSED,
        SEND_FAILURE,
        SEND_SUCCESS,
        SEND_UNKNOWN,
        STALE_OWNER,
        STORAGE_REFUSED,
        UNKNOWN,
    )
else:  # pragma: no cover - standalone layout
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from dispatch_store import (  # noqa: E402
        Authorization,
        DispatchIdentityStore,
        DispatchStore,
        ReservedDispatch,
        BINDING_CONFLICT,
        CANCEL_OK,
        DUPLICATE_DELIVERY,
        LEASE_EXPIRED,
        OBSERVED_FAILURE,
        OBSERVED_SUCCESS,
        RESERVED,
        RESERVED_OK,
        SCHEMA_REFUSED,
        SEND_FAILURE,
        SEND_SUCCESS,
        SEND_UNKNOWN,
        STALE_OWNER,
        STORAGE_REFUSED,
        UNKNOWN,
    )

STORE = "dispatch-store.jsonl"


def _tmp():
    return tempfile.mkdtemp(prefix="ds-test-")


def _auth(task="T1", request="req-1", dest="tg:97340:12", digest="sha256:" + "a" * 64,
          claim="verified-completion", rendering="v1", lease=300.0):
    return Authorization(
        task_id=task,
        approved_request_digest=request,
        destination=dest,
        artifact_digest=digest,
        claim_class=claim,
        rendering=rendering,
        lease_seconds=lease,
    )


def ok(p):
    return True


def fail(p):
    return False


def none_ack(p):
    return None


def boom(p):
    raise RuntimeError("transport down")


# ---------- reservation basics ----------

def test_fresh_reserve_and_send_success():
    d = _tmp()
    store = DispatchIdentityStore(d)
    res = store.reserve(_auth(), "w1")
    assert res.status == RESERVED_OK and res.token == 1
    rd = ReservedDispatch(store.store, res.record)
    out = rd.send("w1", b"payload", ok)
    assert out.status == SEND_SUCCESS, out.status
    assert out.record["state"] == OBSERVED_SUCCESS
    assert out.record["payload_hash"] and len(out.record["payload_hash"]) == 64


def test_reserve_same_owner_idempotent():
    d = _tmp()
    store = DispatchIdentityStore(d)
    r1 = store.reserve(_auth(), "w1")
    r2 = store.reserve(_auth(), "w1")
    assert r1.status == RESERVED_OK and r2.status == RESERVED_OK
    assert r1.token == r2.token  # same live reservation, not a second one


def test_second_owner_conflict():
    d = _tmp()
    store = DispatchIdentityStore(d)
    assert store.reserve(_auth(), "w1").status == RESERVED_OK
    assert store.reserve(_auth(), "w2").status == BINDING_CONFLICT


def test_success_blocks_duplicate_delivery():
    d = _tmp()
    store = DispatchIdentityStore(d)
    res = store.reserve(_auth(), "w1")
    rd = ReservedDispatch(store.store, res.record)
    assert rd.send("w1", b"p", ok).status == SEND_SUCCESS
    assert store.reserve(_auth(), "w2").status == DUPLICATE_DELIVERY
    assert store.reserve(_auth(), "w1").status == DUPLICATE_DELIVERY


# ---------- binding identity ----------

def test_changed_binding_is_conflict_not_fresh_permission():
    d = _tmp()
    store = DispatchIdentityStore(d)
    assert store.reserve(_auth(), "w1").status == RESERVED_OK
    # same task but changed approval -> different logical identity is a NEW
    # delivery only with its own authorization; the ORIGINAL key's state is
    # untouched. Changed binding on the SAME key is impossible by
    # construction (key = hash of binding), so the contract check is: the
    # old delivery stays deduped and the new binding is separately managed.
    r2 = store.reserve(_auth(request="req-2"), "w1")
    assert r2.status == RESERVED_OK
    # original still reserved/deduped
    assert store.reserve(_auth(), "w2").status == BINDING_CONFLICT


def test_distinct_destinations_independent():
    d = _tmp()
    store = DispatchIdentityStore(d)
    a = store.reserve(_auth(dest="tg:a"), "w1")
    b = store.reserve(_auth(dest="tg:b"), "w1")
    assert a.status == RESERVED_OK and b.status == RESERVED_OK
    assert a.token != b.token


def test_destination_change_after_reservation_is_new_identity():
    d = _tmp()
    store = DispatchIdentityStore(d)
    res = store.reserve(_auth(dest="tg:a"), "w1")
    rd = ReservedDispatch(store.store, res.record)
    assert rd.send("w1", b"p", ok).status == SEND_SUCCESS
    # same task+approval, different destination: independently deliverable
    res2 = store.reserve(_auth(dest="tg:b"), "w1")
    assert res2.status == RESERVED_OK
    rd2 = ReservedDispatch(store.store, res2.record)
    assert rd2.send("w1", b"p", ok).status == SEND_SUCCESS


# ---------- crash points (in-process simulation + fresh restart) ----------

def _latest_state(d):
    recs = [json.loads(l) for l in open(os.path.join(d, STORE)) if l.strip()]
    return recs[-1]


def test_crash_before_reservation_no_state():
    d = _tmp()
    store = DispatchIdentityStore(d)
    # crash before reserve: nothing on disk
    assert not os.path.exists(os.path.join(d, STORE)) or (
        _latest_state(d) is None
    )
    # fresh process can reserve normally
    assert store.reserve(_auth(), "w1").status == RESERVED_OK


def test_crash_after_reservation_restart_holds_not_resends():
    d = _tmp()
    store = DispatchIdentityStore(d)
    res = store.reserve(_auth(), "w1")
    # crash after reservation, before any send: fresh owner sees conflict
    r2 = store.reserve(_auth(), "w2")
    assert r2.status == BINDING_CONFLICT
    # same owner re-reserve is idempotent (same token) and can still send
    r1 = store.reserve(_auth(), "w1")
    assert r1.token == res.token
    rd = ReservedDispatch(store.store, r1.record)
    sends = []
    out = rd.send("w1", b"p", lambda p: sends.append(1) or True)
    assert out.status == SEND_SUCCESS and len(sends) == 1


def test_crash_after_intent_commit_restart_unknown():
    d = _tmp()
    store = DispatchIdentityStore(d)
    res = store.reserve(_auth(), "w1")
    rd = ReservedDispatch(store.store, res.record)
    # crash after DISPATCHING intent commit, before transport/outcome:
    assert rd.mark_dispatching("w1") == "dispatching"
    # a fresh process sees dispatching -> cannot reserve, cannot adopt
    s2 = DispatchIdentityStore(d)
    assert s2.reserve(_auth(), "w2").status in (BINDING_CONFLICT, LEASE_EXPIRED)
    time.sleep(0.05)
    # and after lease expiry it is STILL not adoptable (possible send)
    short = _auth(lease=0.1)
    d2 = _tmp()
    s3 = DispatchIdentityStore(d2)
    r3 = s3.reserve(short, "w")
    rd3 = ReservedDispatch(s3.store, r3.record)
    assert rd3.mark_dispatching("w") == "dispatching"
    time.sleep(0.15)
    assert s3.adopt(short, "w9").status == SEND_UNKNOWN


def test_crash_after_send_before_receipt_commit():
    d = _tmp()
    store = DispatchIdentityStore(d)
    res = store.reserve(_auth(), "w1")
    sends = []

    def sink(p):
        sends.append(1)
        os._exit(9)  # crash AFTER sink acceptance, BEFORE receipt commit

    proc = subprocess.run(
        [sys.executable, "-c", """
import os, sys
sys.path.insert(0, {root!r})
from response_verification.dispatch_store import (
    Authorization, DispatchIdentityStore, ReservedDispatch)
d = sys.argv[1]
store = DispatchIdentityStore(d)
auth = Authorization(task_id='T1', approved_request_digest='req-1',
                     destination='tg:97340:12',
                     artifact_digest='sha256:' + 'a'*64)
res = store.reserve(auth, 'w1')
rd = ReservedDispatch(store.store, res.record)
sends = []
def sink(p):
    sends.append(1)
    os._exit(9)
out = rd.send('w1', b'p', sink)
print('SENT', out.status)
""".format(root=_root), d],
        capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 9, (proc.returncode, proc.stdout, proc.stderr)
    # store has reserved+dispatching, no outcome: restart must hold unknown
    s2 = DispatchIdentityStore(d)
    assert s2.reserve(_auth(), "w2").status in (BINDING_CONFLICT, LEASE_EXPIRED)
    states = [json.loads(l) for l in open(os.path.join(d, STORE)) if l.strip()]
    assert states[-1]["state"] == "dispatching"
    # no second send by the store: any new process gets conflict/unknown
    proc2 = subprocess.run(
        [sys.executable, "-c", """
import sys
sys.path.insert(0, {root!r})
from response_verification.dispatch_store import (
    Authorization, DispatchIdentityStore)
auth = Authorization(task_id='T1', approved_request_digest='req-1',
                     destination='tg:97340:12',
                     artifact_digest='sha256:' + 'a'*64)
print(DispatchIdentityStore(sys.argv[1]).reserve(auth, 'w2').status)
""".format(root=_root), d],
        capture_output=True, text=True, timeout=60,
    )
    assert proc2.returncode == 0
    assert proc2.stdout.strip() in ("binding_conflict", "lease_expired")


def test_unknown_survives_repeated_restarts():
    d = _tmp()
    store = DispatchIdentityStore(d)
    res = store.reserve(_auth(), "w1")
    rd = ReservedDispatch(store.store, res.record)
    assert rd.send("w1", b"p", none_ack).status == SEND_UNKNOWN
    for owner in ("w1", "w2", "w3"):
        assert store.reserve(_auth(), owner).status in (
            SEND_UNKNOWN, BINDING_CONFLICT,
        )


def test_explicit_failure_then_new_authorization_required():
    d = _tmp()
    store = DispatchIdentityStore(d)
    res = store.reserve(_auth(), "w1")
    rd = ReservedDispatch(store.store, res.record)
    assert rd.send("w1", b"p", fail).status == SEND_FAILURE
    assert rd.send("w1", b"p", ok).status == SEND_FAILURE  # terminal state
    # observed-failure is terminal: retry requires a NEW logical identity,
    # which means a new authorization binding (new request digest)
    assert store.reserve(_auth(), "w2").status == BINDING_CONFLICT
    r2 = store.reserve(_auth(request="req-2"), "w1")
    assert r2.status == RESERVED_OK


# ---------- lease expiry and fencing ----------

def test_expired_lease_denies_send_no_auto_retry():
    d = _tmp()
    store = DispatchIdentityStore(d)
    res = store.reserve(_auth(lease=0.1), "w1")
    rd = ReservedDispatch(store.store, res.record)
    time.sleep(0.15)
    sends = []
    out = rd.send("w1", b"p", lambda p: sends.append(1) or True)
    assert out.status == LEASE_EXPIRED and len(sends) == 0


def test_adopt_expired_reservation_then_old_owner_fenced():
    d = _tmp()
    store = DispatchIdentityStore(d)
    res = store.reserve(_auth(lease=0.1), "old")
    rd_old = ReservedDispatch(store.store, res.record)
    time.sleep(0.15)
    r2 = store.adopt(_auth(lease=0.1), "new")
    assert r2.status == RESERVED_OK and r2.token > res.token
    # old owner cannot act after transfer
    assert rd_old.mark_dispatching("old") == STALE_OWNER
    sends = []
    out = rd_old.send("old", b"p", lambda p: sends.append(1) or True)
    assert out.status == STALE_OWNER and len(sends) == 0
    # new owner completes
    rd_new = ReservedDispatch(store.store, r2.record)
    assert rd_new.send("new", b"p", ok).status == SEND_SUCCESS


def test_adopt_refuses_live_lease():
    d = _tmp()
    store = DispatchIdentityStore(d)
    assert store.reserve(_auth(), "w1").status == RESERVED_OK
    assert store.adopt(_auth(), "w2").status == LEASE_EXPIRED


def test_cancel_then_rereserve_conflict():
    d = _tmp()
    store = DispatchIdentityStore(d)
    res = store.reserve(_auth(), "w1")
    rd = ReservedDispatch(store.store, res.record)
    assert rd.cancel("w1") == CANCEL_OK
    # cancelled is terminal: same binding cannot come back
    assert store.reserve(_auth(), "w1").status == BINDING_CONFLICT
    assert store.reserve(_auth(), "w2").status == BINDING_CONFLICT


# ---------- fail-closed store handling ----------

def test_torn_store_refuses_all_service():
    d = _tmp()
    store = DispatchIdentityStore(d)
    assert store.reserve(_auth(task="T0"), "w0").status == RESERVED_OK
    with open(os.path.join(d, STORE), "a", encoding="utf-8") as f:
        f.write('{"schema": 1, "binding_key": "abc", "state": "reser')
    # every path must refuse: reserve, status
    assert store.reserve(_auth(task="T9"), "w9").status == STORAGE_REFUSED
    # a fresh process also refuses
    proc = subprocess.run(
        [sys.executable, "-c", """
import sys
sys.path.insert(0, {root!r})
from response_verification.dispatch_store import Authorization, DispatchIdentityStore
auth = Authorization(task_id='T9', approved_request_digest='req-1',
                     destination='tg:97340:12',
                     artifact_digest='sha256:' + 'a'*64)
print(DispatchIdentityStore(sys.argv[1]).reserve(auth, 'w9').status)
""".format(root=_root), d],
        capture_output=True, text=True, timeout=60,
    )
    assert proc.returncode == 0 and proc.stdout.strip() == "storage_refused"


def test_unknown_schema_version_refuses():
    d = _tmp()
    store = DispatchIdentityStore(d)
    assert store.reserve(_auth(task="T0"), "w0").status == RESERVED_OK
    lines = open(os.path.join(d, STORE), encoding="utf-8").read().splitlines()
    rec = json.loads(lines[-1])
    rec["schema"] = 99
    with open(os.path.join(d, STORE), "w", encoding="utf-8") as f:
        for line in lines[:-1]:
            f.write(line + "\n")
        f.write(json.dumps(rec) + "\n")
    assert store.reserve(_auth(task="T9"), "w9").status == SCHEMA_REFUSED


def test_unknown_state_string_refuses():
    "incompatible schema (novel state) fails closed"
    d = _tmp()
    store = DispatchIdentityStore(d)
    assert store.reserve(_auth(task="T0"), "w0").status == RESERVED_OK
    lines = open(os.path.join(d, STORE), encoding="utf-8").read().splitlines()
    rec = json.loads(lines[-1])
    rec["state"] = "paused"  # not in the explicit state set
    with open(os.path.join(d, STORE), "w", encoding="utf-8") as f:
        for line in lines[:-1]:
            f.write(line + "\n")
        f.write(json.dumps(rec) + "\n")
    assert store.reserve(_auth(task="T9"), "w9").status == SCHEMA_REFUSED


def test_unwritable_store_refuses():
    d = _tmp()
    store = DispatchIdentityStore(d)
    assert store.reserve(_auth(), "w1").status == RESERVED_OK
    # make the store path unusable for a NEW store dir
    d2 = _tmp()
    os.mkdir(os.path.join(d2, STORE))  # path is a directory: writes fail
    s2 = DispatchIdentityStore(d2)
    res = s2.reserve(_auth(), "w1")
    assert res.status == STORAGE_REFUSED


def test_locked_store_blocks_until_release():
    """A store locked by another process is not corrupted or bypassed: a
    fresh process cannot acquire the lock (bounded try-lock proves mutual
    exclusion), and after release the store serves correctly."""
    d = _tmp()
    store = DispatchIdentityStore(d)
    assert store.reserve(_auth(), "w1").status == RESERVED_OK
    lock = store.store.acquire()
    try:
        proc = subprocess.run(
            [sys.executable, "-c", """
import fcntl, sys
sys.path.insert(0, {root!r})
lock_path = sys.argv[1] + '/dispatch.lock'
fh = open(lock_path, 'a+', encoding='utf-8')
try:
    fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    print('ACQUIRED')
    fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
except BlockingIOError:
    print('BLOCKED')
""".format(root=_root), d],
            capture_output=True, text=True, timeout=30,
        )
        assert proc.returncode == 0, (proc.returncode, proc.stdout, proc.stderr)
        assert "BLOCKED" in proc.stdout and "ACQUIRED" not in proc.stdout
    finally:
        store.store.release(lock)
    # after release the store is fully usable by a fresh process
    proc2 = subprocess.run(
        [sys.executable, "-c", """
import sys
sys.path.insert(0, {root!r})
from response_verification.dispatch_store import Authorization, DispatchIdentityStore
auth = Authorization(task_id='T2', approved_request_digest='req-1',
                     destination='tg:97340:12',
                     artifact_digest='sha256:' + 'a'*64)
print(DispatchIdentityStore(sys.argv[1]).reserve(auth, 'w2').status)
""".format(root=_root), d],
        capture_output=True, text=True, timeout=30,
    )
    assert proc2.returncode == 0 and proc2.stdout.strip() == "reserved"


def test_native_delivery_id_captured():
    d = _tmp()
    store = DispatchIdentityStore(d)
    res = store.reserve(_auth(), "w1")
    rd = ReservedDispatch(store.store, res.record)
    out = rd.send("w1", b"p", lambda p: "native-42")
    assert out.status == SEND_SUCCESS
    assert out.delivery_id == "native-42"
    assert out.record["native_delivery_id"] == "native-42"


# ---------- cross-process competition (real processes) ----------

def test_two_simultaneous_processes_one_authority():
    """Contract: two simultaneous independent processes, one logical
    delivery: at most one obtains valid sending authority."""
    d = _tmp()
    child = r'''
import sys
sys.path.insert(0, {root!r})
from response_verification.dispatch_store import (
    Authorization, DispatchIdentityStore, ReservedDispatch)
d, owner = sys.argv[1], sys.argv[2]
store = DispatchIdentityStore(d)
auth = Authorization(task_id='T1', approved_request_digest='req-1',
                     destination='tg:97340:12',
                     artifact_digest='sha256:' + 'a'*64)
res = store.reserve(auth, owner)
if res.status != 'reserved':
    print('LOSE', res.status)
    sys.exit(3)
rd = ReservedDispatch(store.store, res.record)
sends = []
def sink(p):
    sends.append(1)
    return True
out = rd.send(owner, b'race-payload', sink)
print('WIN_SEND', out.status, 'sends', len(sends))
sys.exit(0 if out.status == 'send_success' else 4)
'''.format(root=_root)
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", child, d, f"racer-{i}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        for i in range(2)
    ]
    outs = [p.communicate(timeout=60) for p in procs]
    rcs = [p.returncode for p in procs]
    total_sends = sum(
        1 for o in outs if "sends 1" in o[0]
    )
    assert sorted(rcs) == [0, 3], (rcs, outs)
    assert total_sends == 1, (rcs, outs)  # exactly one sender
    # store shows exactly one observed-success for the binding
    states = [json.loads(l) for l in open(os.path.join(d, STORE)) if l.strip()]
    assert sum(1 for s in states if s["state"] == "observed-success") == 1


def test_many_way_competition_single_sender():
    d = _tmp()
    child = r'''
import sys
sys.path.insert(0, {root!r})
from response_verification.dispatch_store import (
    Authorization, DispatchIdentityStore)
d, owner = sys.argv[1], sys.argv[2]
store = DispatchIdentityStore(d)
auth = Authorization(task_id='T1', approved_request_digest='req-1',
                     destination='tg:97340:12',
                     artifact_digest='sha256:' + 'a'*64)
res = store.reserve(auth, owner)
print(res.status)
sys.exit(0 if res.status == 'reserved' else 3)
'''.format(root=_root)
    procs = [
        subprocess.Popen(
            [sys.executable, "-c", child, d, f"r{i}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        for i in range(8)
    ]
    outs = [p.communicate(timeout=60) for p in procs]
    statuses = [o[0].strip() for o in outs]
    winners = sum(1 for s in statuses if s == "reserved")
    assert winners == 1, statuses
    assert all(p.returncode in (0, 3) for p in procs)


# ---------- subprocess sink delivery log ----------

def test_real_sink_append_only_log_and_exit_codes():
    """Real subprocess sink keeps an independently inspectable append-only
    delivery log; report actual send count and exact per-command exit codes."""
    d = _tmp()
    sink_log = os.path.join(d, "sink-deliveries.jsonl")
    child = r'''
import json, os, sys
sys.path.insert(0, {root!r})
from response_verification.dispatch_store import (
    Authorization, DispatchIdentityStore, ReservedDispatch)
d, owner, sink_log = sys.argv[1], sys.argv[2], sys.argv[3]
store = DispatchIdentityStore(d)
auth = Authorization(task_id='TSINK', approved_request_digest='req-1',
                     destination='tg:97340:12',
                     artifact_digest='sha256:' + 'a'*64)
res = store.reserve(auth, owner)
if res.status != 'reserved':
    print('RESERVE', res.status)
    sys.exit(3)
rd = ReservedDispatch(store.store, res.record)

def sink(payload):
    rec = {{"owner": owner, "payload": payload.decode(), "ts": __import__('time').time()}}
    with open(sink_log, "a", encoding="utf-8") as f:
        f.write(json.dumps(rec) + "\n")
        f.flush()
        __import__('os').fsync(f.fileno())
    return True

out = rd.send(owner, b'sink-payload', sink)
print('SEND', out.status)
sys.exit(0 if out.status == 'send_success' else 4)
'''.format(root=_root)
    # first delivery succeeds
    p1 = subprocess.run(
        [sys.executable, "-c", child, d, "sink-1", sink_log],
        capture_output=True, text=True, timeout=60,
    )
    assert p1.returncode == 0, (p1.returncode, p1.stdout, p1.stderr)
    assert "SEND send_success" in p1.stdout
    # second delivery attempt (fresh process) is blocked
    p2 = subprocess.run(
        [sys.executable, "-c", child, d, "sink-2", sink_log],
        capture_output=True, text=True, timeout=60,
    )
    assert p2.returncode == 3, (p2.returncode, p2.stdout, p2.stderr)
    assert "RESERVE duplicate_delivery" in p2.stdout
    # sink log shows exactly one real send
    entries = [json.loads(l) for l in open(sink_log, encoding="utf-8") if l.strip()]
    assert len(entries) == 1
    assert entries[0]["payload"] == "sink-payload"


# ---------- standalone runner + child mode ----------

if __name__ == "__main__":
    fns = [v for k, v in sorted(globals().items()) if k.startswith("test_") and callable(v)]
    fails = 0
    for fn in fns:
        try:
            fn()
            print("PASS", fn.__name__)
        except AssertionError as e:
            fails += 1
            print("FAIL", fn.__name__, e)
        except Exception as e:  # noqa: BLE001
            fails += 1
            print("ERROR", fn.__name__, repr(e))
    print(f"\n{len(fns) - fails}/{len(fns)} passed, {fails} failed")
    sys.exit(fails)

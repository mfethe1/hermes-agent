"""Adversarial tests for durable dispatch (replay-acceptance contract).

pytest-compatible; also runs standalone (``python3 test_pilot_dispatch.py``)
where the cross-process case spawns a child interpreter.
"""
import os
import sys
import time

_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if os.path.isdir(os.path.join(_root, "response_verification")):
    sys.path.insert(0, _root)
    from response_verification.pilot_dispatch import (  # noqa: E402
        DurableDispatcher, DispatchStatus, content_key,
    )
else:
    sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
    from pilot_dispatch import (  # noqa: E402
        DurableDispatcher, DispatchStatus, content_key,
    )

JOURNAL = "dispatch-journal.jsonl"


def _tmp():
    import tempfile
    return tempfile.mkdtemp(prefix="dd-test-")


def delivered(p): return True
def none_ack(p): return None


def _intent(d, owner, payload=b"p"):
    key = content_key("t1", "a1", "dest", payload)
    DurableDispatcher(d, owner).ledger._append(
        {"dispatch_id": "dc1", "content_key": key, "phase": "intent",
         "outcome": None, "owner": owner, "ts": time.time()}
    )
    return key


# ---------- journal-order / fresh dispatch ----------

def test_fresh_delivered():
    d = _tmp()
    st = DurableDispatcher(d, "w1").dispatch("dc1", "t1", "a1", "dest", b"p", delivered)
    assert st == DispatchStatus.DELIVERED, st


def test_journal_order():
    import json
    d = _tmp()
    DurableDispatcher(d, "w1").dispatch("dc1", "t1", "a1", "dest", b"p", delivered)
    lines = open(os.path.join(d, JOURNAL)).read().splitlines()
    recs = [json.loads(l) for l in lines]
    assert recs[0]["phase"] == "intent", "intent must be first journal entry"
    assert recs[-1]["phase"] == "outcome" and recs[-1]["outcome"] == "delivered", recs[-1]


# ---------- duplicate suppression across instances ----------

def test_cross_instance_duplicate_blocked():
    d = _tmp()
    a = DurableDispatcher(d, "inst-a").dispatch("dc1", "t1", "a1", "dest", b"p", delivered)
    b = DurableDispatcher(d, "inst-b").dispatch("dc1", "t1", "a1", "dest", b"p", delivered)
    assert a == DispatchStatus.DELIVERED and b == DispatchStatus.DUPLICATE_BLOCKED, (a, b)


def test_cross_process_duplicate_blocked():
    import subprocess
    d = _tmp()
    DurableDispatcher(d, "parent").dispatch("dc1", "t1", "a1", "dest", b"payload", delivered)
    child = subprocess.run(
        [sys.executable, os.path.abspath(__file__), "--child-dispatch", d],
        capture_output=True, text=True, timeout=60,
    )
    assert child.returncode == 3, (child.returncode, child.stdout, child.stderr)
    assert "DUPLICATE_BLOCKED" in child.stdout, child.stdout


# ---------- crash / unknown outcome semantics ----------

def test_crash_after_intent_holds_unknown():
    d = _tmp()
    _intent(d, "w1")  # simulate crash between intent and outcome
    st = DurableDispatcher(d, "w2").dispatch("dc1", "t1", "a1", "dest", b"p", delivered)
    assert st == DispatchStatus.UNKNOWN_HOLD, st


def test_transport_exception_unknown():
    def boom(p): raise RuntimeError("transport down")
    d = _tmp()
    st = DurableDispatcher(d, "w1").dispatch("dc1", "t1", "a1", "dest", b"p", boom)
    assert st == DispatchStatus.UNKNOWN_HOLD, st


def test_no_ack_unknown():
    d = _tmp()
    st = DurableDispatcher(d, "w1").dispatch("dc1", "t1", "a1", "dest", b"p", none_ack)
    assert st == DispatchStatus.UNKNOWN_HOLD, st


def test_outcome_storage_failure_holds_unknown():
    d = _tmp()
    disp = DurableDispatcher(d, "w1")
    original = disp.ledger._append
    calls = []
    def flaky(rec):
        calls.append(rec.get("phase"))
        if len(calls) >= 2:
            raise OSError("disk full")
        original(rec)
    disp.ledger._append = flaky
    st = disp.dispatch("dc1", "t1", "a1", "dest", b"p", delivered)
    assert st == DispatchStatus.UNKNOWN_HOLD, st
    assert calls and calls[0] == "intent", calls  # intent durable before send


# ---------- storage refusal BEFORE send ----------

def test_intent_storage_refused_no_send():
    d = _tmp()
    os.mkdir(os.path.join(d, JOURNAL))  # journal path unusable
    called = []
    st = DurableDispatcher(d, "w1").dispatch(
        "dc1", "t1", "a1", "dest", b"p", lambda p: called.append(1) or True)
    assert st == DispatchStatus.STORAGE_REFUSED, st
    assert not called, "transport must not run without durable intent"


# ---------- torn tail ----------

def test_torn_tail_fails_closed():
    d = _tmp()
    _intent(d, "w1")
    with open(os.path.join(d, JOURNAL), "a") as f:
        f.write('{"phase": "outco')  # torn write from crash
    st = DurableDispatcher(d, "w2").dispatch("dc1", "t1", "a1", "dest", b"p", delivered)
    # torn line anywhere in the store is unattributable ambiguity: refuse
    assert st == DispatchStatus.STORAGE_REFUSED, st


# ---------- attempt / destination scoping ----------

def test_different_attempt_same_logical_blocked():
    d = _tmp()
    a = DurableDispatcher(d, "w1").dispatch("dc1", "t1", "a1", "dest", b"p", delivered)
    b = DurableDispatcher(d, "w1").dispatch("dc2", "t1", "a2", "dest", b"p", delivered)
    # same task+destination+payload, relabeled attempt: logical dedupe must
    # not silently resend (contract: separate logical key from attempt identity)
    assert a == DispatchStatus.DELIVERED, a
    assert b == DispatchStatus.DUPLICATE_BLOCKED, b


def test_different_destination_new_dispatch():
    d = _tmp()
    a = DurableDispatcher(d, "w1").dispatch("dc1", "t1", "a1", "d1", b"p", delivered)
    b = DurableDispatcher(d, "w1").dispatch("dc2", "t1", "a1", "d2", b"p", delivered)
    assert a == DispatchStatus.DELIVERED and b == DispatchStatus.DELIVERED, (a, b)


# ---------- standalone runner + child mode ----------

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--child-dispatch":
        st = DurableDispatcher(sys.argv[2], "child-proc").dispatch(
            "dc1", "t1", "a1", "dest", b"payload", lambda p: True
        )
        print("CHILD_DISPATCHED", st)
        sys.exit(0 if st == DispatchStatus.DELIVERED else 3)
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

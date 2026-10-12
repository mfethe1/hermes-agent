"""Clickable goal controls must be useful for legacy goals and never bypass a boundary."""
import time
from concurrent.futures import ThreadPoolExecutor
from copy import deepcopy

import pytest

from hermes_cli import goals
from hermes_cli.goal_command import dispatch_goal_command, is_goal_control


@pytest.fixture
def manager():
    mgr = goals.GoalManager("menu-test", default_max_turns=8)
    mgr.set("Verify the browser product", contract=goals.GoalContract(verification="browser acceptance", constraints="no deploy"))
    mgr.state.last_reason = "Browser verification needs more disk space"
    mgr.state.turns_used = 2
    mgr.pause("blocked on disk")
    return mgr


def command(mgr, arg, authorize=lambda: None):
    return dispatch_goal_command(mgr, arg, authorize_gate=authorize)


def click(mgr, card, action):
    return command(mgr, f"unblock {card.menu['id']} {action}")


def test_legacy_paused_goal_has_compact_durable_menu(manager):
    card = command(manager, "unblock")
    assert hasattr(card, "menu") and card.menu
    assert 3 <= len(card.menu["choices"]) <= 5
    actions = {c["id"] for c in card.menu["choices"]}
    assert {"resume", "diagnose", "hold", "details"} <= actions
    assert len(card.menu["text"]) < 1800
    assert "disk space" in card.menu["text"]
    loaded = goals.GoalManager(manager.session_id)
    assert loaded.state.unblock_menu["id"] == card.menu["id"]
    assert loaded.state.status == "paused"
    assert loaded.state.turns_used == 2
    assert is_goal_control("unblock")


def test_resume_retains_budget_contract_and_authority(manager):
    card = command(manager, "unblock")
    result = click(manager, card, "resume")
    assert not result.error and result.prompt
    assert manager.state.status == "active"
    assert manager.state.turns_used == 2 and manager.state.max_turns == 8
    assert manager.state.contract.constraints == "no deploy"
    assert "no deploy" in result.prompt


def test_diagnose_is_read_only_first_and_keeps_original_checks(manager):
    result = click(manager, command(manager, "unblock"), "diagnose")
    assert not result.error and result.prompt
    assert "read-only" in result.prompt.lower()
    assert "browser acceptance" in result.prompt
    assert manager.state.turns_used == 2


def test_hold_does_not_resume_or_consume_turn(manager):
    result = click(manager, command(manager, "unblock"), "hold")
    assert not result.error and not result.prompt
    assert manager.state.status == "paused" and manager.state.turns_used == 2


def test_exhausted_budget_requires_explicit_20_turn_choice(manager):
    manager.state.turns_used = 8; manager._save()
    card = command(manager, "unblock")
    assert "budget20" in {c["id"] for c in card.menu["choices"]}
    assert "resume" not in {c["id"] for c in card.menu["choices"]}
    assert "20" in card.menu["text"]
    result = click(manager, card, "budget20")
    assert not result.error and result.prompt
    assert manager.state.turns_used == 8 and manager.state.max_turns == 28


@pytest.mark.parametrize("mutation", ["goal", "contract", "turn", "budget", "expired", "token"])
def test_stale_or_expired_menu_does_not_resume(manager, mutation):
    card = command(manager, "unblock")
    token = card.menu["id"]
    changes = {
        "goal": lambda: setattr(manager.state, "goal", "other goal"),
        "contract": lambda: setattr(manager.state.contract, "constraints", "new restriction"),
        "turn": lambda: setattr(manager.state, "turns_used", manager.state.turns_used+1),
        "budget": lambda: setattr(manager.state, "max_turns", manager.state.max_turns+1),
        "expired": lambda: manager.state.unblock_menu.update(expires_at=time.time()-1),
        "token": lambda: None,
    }
    changes[mutation]()
    token = "wrong" if mutation == "token" else token
    manager._save()
    result = command(manager, f"unblock {token} resume")
    assert result.error and not result.prompt
    assert manager.state.status == "paused"


def test_old_menu_cannot_select_new_menu(manager):
    first = command(manager, "unblock")
    second = command(manager, "unblock")
    assert first.menu["id"] != second.menu["id"]
    assert click(manager, first, "resume").error


def test_other_session_and_unauthorized_selection_rejected(manager):
    card = command(manager, "unblock")
    other = goals.GoalManager("other-menu"); other.set("other"); other.pause()
    assert click(other, card, "resume").error
    result = command(manager, f"unblock {card.menu['id']} resume", authorize=lambda: "not authorized")
    assert result.error and manager.state.status == "paused"


def test_concurrent_clicks_enqueue_at_most_one_continuation(manager):
    card = command(manager, "unblock")
    def select(_):
        return click(goals.GoalManager(manager.session_id), card, "resume")
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(select, range(2)))
    assert sum(bool(r.prompt) for r in results) == 1
    assert sum(r.error for r in results) == 1


def test_details_are_read_only_and_do_not_consume_menu(manager):
    card = command(manager, "unblock")
    result = click(manager, card, "details")
    assert not result.error and not result.prompt
    assert "browser acceptance" in result.output
    assert not manager.state.unblock_menu["consumed"]


def test_applicable_recovery_paths_have_risk_labels_and_no_forbidden_button(manager, monkeypatch):
    from hermes_cli.goal_recovery import _assessment, binding
    option = dict(id="alternate", route="alternative_authorized", plan="Use the existing authorized loopback preview.",
                  verification="Verify browser acceptance there.", effects=["read_only"], risk="low", authorization="authorized",
                  reversibility="reversible", progress="useful", evidence="sufficient")
    denied = deepcopy(option); denied.update(id="forbidden", authorization="forbidden")
    manager.state.recovery = _assessment(dict(selected="alternate", automatic=False, options=[option, denied]), manager, binding(manager))
    manager._save()
    card = command(manager, "unblock")
    assert any(c["id"] == "palternate" and "low" in c["label"] for c in card.menu["choices"])
    assert not any(c["id"] == "pforbidden" for c in card.menu["choices"])
    result = click(manager, card, "palternate")
    assert result.prompt and "loopback preview" in result.prompt
    assert "Tool approvals" in result.prompt


def test_compare_and_set_meta_claim_is_unique_across_processes():
    import subprocess
    import sys
    db = goals._get_session_db()
    db.set_meta("goal-ui-process-claim", "old")
    code = ("from hermes_state import SessionDB; db=SessionDB(); "
            "print(int(db.compare_and_set_meta('goal-ui-process-claim','old','claimed'))); db.close()")
    children = [subprocess.Popen([sys.executable, "-c", code], stdout=subprocess.PIPE, stderr=subprocess.PIPE) for _ in range(2)]
    outputs = [child.communicate(timeout=30) for child in children]
    assert all(child.returncode == 0 for child in children)
    assert sum(int(stdout.strip()) for stdout, _ in outputs) == 1


def test_compare_and_set_meta_is_atomic_and_does_not_overwrite_newer_data():
    db = goals._get_session_db()
    db.set_meta("menu-cas-test", "old")
    assert hasattr(db, "compare_and_set_meta"), "atomic menu claim not implemented"
    assert db.compare_and_set_meta("menu-cas-test", "old", "first")
    assert not db.compare_and_set_meta("menu-cas-test", "old", "second")
    assert db.get_meta("menu-cas-test") == "first"

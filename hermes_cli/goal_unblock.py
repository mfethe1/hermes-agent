"""Compact, durable goal menus. Every mutation is a snapshot-checked atomic commit."""
from __future__ import annotations

from dataclasses import replace
import hashlib
import json
import time
import uuid

from hermes_cli import goals
from hermes_cli.goal_command import GoalCommandResult
from hermes_cli.goal_recovery import binding, prepare_choice, _notify, render_options

LABELS = {"continue_authorized": "Continue approved path", "alternative_authorized": "Try alternate path",
          "independent_work": "Do independent work", "gather_evidence": "Inspect evidence", "wait": "Wait for dependency"}


def _binding(manager):
    state = manager.state
    recovery = state.recovery or {}
    from hermes_constants import hermes_home_key, get_hermes_home
    value = [hermes_home_key(get_hermes_home()), binding(manager), state.status, state.max_turns, state.paused_reason,
             state.waiting_on_pid, state.waiting_on_session, state.waiting_until,
             recovery.get("id"), recovery.get("consumed")]
    return hashlib.sha256(json.dumps(value, sort_keys=True).encode()).hexdigest()


def _load(manager):
    db = goals._get_session_db()
    if db is None:
        raise RuntimeError("goal storage unavailable; no action applied")
    key = goals._meta_key(manager.session_id)
    raw = db.get_meta(key)
    if not raw:
        raise RuntimeError("no goal set")
    manager._state = goals.GoalState.from_json(raw)
    manager._require_goal()
    return db, key, raw


def _short(value, limit):
    from agent.redact import redact_for_egress
    text = " ".join(redact_for_egress(str(value or "")).split())
    return text if len(text) <= limit else text[:limit-1] + "…"


def _choices(manager):
    state = manager.state
    remaining = state.max_turns - state.turns_used
    recovery = state.recovery or {}
    current = (recovery and not recovery.get("consumed") and recovery.get("expires_at", 0) > time.time()
               and recovery.get("goal_binding") == binding(manager))
    choices = []
    if current and remaining > 0:
        available = [o for o in recovery["options"] if o["route"] in LABELS and o.get("authorization") != "forbidden"]
        available.sort(key=lambda o: o["id"] != recovery["selected"])
        for option in available[:2]:
            risk = option.get("risk", "unknown")
            label = "Review required step" if option.get("authorization") == "approval_required" else LABELS[option["route"]]
            choices.append(dict(id="p"+option["id"], label=f"{label} · {risk}", kind="path", risk=risk,
                                description=_short(option["plan"], 170), recovery_id=recovery["id"], option_id=option["id"]))
    if remaining <= 0:
        choices.append(dict(id="budget20", label="Resume · +20 turns", kind="budget20", risk="model usage",
                            description="Add exactly 20 model turns and continue within the original permissions."))
    elif not choices:
        choices.append(dict(id="resume", label="Retry approved work", kind="resume", risk="guarded retry",
                            description="Retry after resolving the blocker; all existing approvals and checks still apply."))
    if remaining > 0:
        choices.append(dict(id="diagnose", label="Diagnose & retry", kind="diagnose", risk="read-only first",
                            description="Inspect the current blocker read-only, then continue only within existing authority."))
    waiting = state.status == "active"
    choices.append(dict(id="hold", label="Keep waiting" if waiting else "Keep paused", kind="hold", risk="none",
                        description="Keep the current wait/paused state; no work is started."))
    choices.append(dict(id="details", label="View blocker details", kind="details", risk="none",
                        description="Show the full reason, completion contract and saved path assessments."))
    return choices[:5]


def _view(manager, menu):
    state = manager.state
    headline = "Goal paused" if state.status == "paused" else "Goal waiting"
    reason = state.last_reason or state.paused_reason or "No blocker details recorded."
    lines = [f"⏸ {headline}: {_short(state.goal, 140)}", f"Blocker: {_short(reason, 240)}",
             f"Budget: {state.turns_used}/{state.max_turns} turns. Choose a next step:"]
    for n, choice in enumerate(menu["choices"], 1):
        lines.append(f"{n}. {choice['label']} — {choice['description']}")
    lines.append("Selection never bypasses tool approvals or changes completion checks. /goal unblock reopens this menu.")
    title = "\n".join(lines)
    return {**menu, "text": title}


def open_menu(manager):
    db, key, raw = _load(manager)
    state = manager.state
    if state.status == "active" and not (state.waiting_on_pid or state.waiting_on_session or state.waiting_until):
        return GoalCommandResult(f"{manager.status_line()}\nThe goal is already running; no unblock is needed.")
    now = time.time()
    menu = dict(id=uuid.uuid4().hex[:16], binding=_binding(manager), expires_at=now+86400,
                consumed=False, choices=_choices(manager))
    state.unblock_menu = menu
    if not db.compare_and_set_meta(key, raw, state.to_json()):
        raise ValueError("goal changed while opening the menu; /goal unblock to refresh")
    view = _view(manager, menu)
    commands = "\n".join(f"/goal unblock {menu['id']} {c['id']}" for c in menu["choices"])
    return GoalCommandResult(view["text"] + "\n\n" + commands, menu=view)


def _resume_state(manager):
    state = manager.state
    state.status = "active"
    state.paused_reason = None
    state.clear_wait()
    if state.recovery:
        state.recovery["consumed"] = True


def _control(manager, choice):
    state = manager.state
    kind = choice["kind"]
    if kind not in {"hold", "resume", "budget20", "diagnose"}:
        raise ValueError("invalid goal control action")
    if kind == "hold":
        return GoalCommandResult("⏸ Goal remains paused." if state.status == "paused" else "⏳ Goal remains waiting.", clear_pending="pause" if state.status == "paused" else None)
    if kind == "budget20":
        if state.turns_used < state.max_turns:
            raise ValueError("the budget is not exhausted; refresh the menu")
        state.max_turns = state.turns_used + 20
    elif state.turns_used >= state.max_turns:
        raise ValueError("budget exhausted; choose the explicit +20 turns option")
    _resume_state(manager)
    prompt = manager.next_continuation_prompt()
    if kind == "diagnose":
        prompt += ("\n[Unblock diagnostic step: read-only first]\nInspect the exact current blocker and existing saved progress/checks. "
                   "Do not delete, spend, alter credentials/security, deploy or publish. Do not duplicate workers. "
                   "Continue only if a concrete next step is already authorized; otherwise report the precise missing decision.")
    return GoalCommandResult("▶ Goal resumed within existing permissions; completion checks unchanged.", prompt=prompt)


def hold_after_admission_failure(manager):
    """Rollback only our own committed snapshot; never overwrite a newer goal change."""
    db = goals._get_session_db()
    key = goals._meta_key(manager.session_id)
    expected = manager.state.to_json()
    manager.state.status = "paused"
    manager.state.paused_reason = "unblock continuation was not admitted"
    if db is None or not db.compare_and_set_meta(key, expected, manager.state.to_json()):
        return GoalCommandResult("No continuation started; the goal changed. Check /goal status.", error=True)
    card = open_menu(manager)
    return replace(card, output="Could not start the next step; the goal remains paused.\n" + card.output, error=True)


def select_action(manager, token, action):
    db, key, raw = _load(manager)
    menu = manager.state.unblock_menu
    if (not menu or token != menu["id"] or menu.get("consumed") or time.time() >= menu["expires_at"]
            or menu["binding"] != _binding(manager)):
        raise ValueError("this menu is expired, used or stale; /goal unblock to reopen")
    choice = next((c for c in menu["choices"] if c["id"] == action), None)
    if choice is None:
        raise ValueError("this action was not offered by the goal menu")
    if choice["kind"] == "details":
        return GoalCommandResult(f"{manager.status_line()}\n{manager.render_contract()}\n{render_options(manager)}")
    notice = None
    if choice["kind"] == "path":
        result, assessment, option = prepare_choice(manager, choice["recovery_id"], choice["option_id"])
        notice = assessment, option
    else:
        result = _control(manager, choice)
    menu["consumed"] = True
    if not db.compare_and_set_meta(key, raw, manager.state.to_json()):
        manager._state = goals.load_goal(manager.session_id)
        raise ValueError("goal changed or this choice was already used; /goal unblock to refresh")
    if notice:
        _notify(manager, *notice, "human")
    return replace(result, menu=None)

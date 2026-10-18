"""Explicit /stop must prevent standing goals from waking themselves again."""
import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import threading
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from hermes_cli import goals


@pytest.fixture
def active_goal(tmp_path, monkeypatch):
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    goals._DB_CACHE.clear()
    goals._get_session_db()
    manager = goals.GoalManager('stop-this-goal')
    manager.set('Finish authorized work', max_turns=4)
    manager.wait_for_seconds(1)
    manager.state.waiting_until = 1
    goals.save_goal(manager.session_id, manager.state)
    yield manager
    goals._DB_CACHE.clear()


def test_stop_pauses_parked_goal_without_resetting_budget(active_goal):
    from gateway.goal_stop import pause_goal_if_active
    active_goal.state.turns_used = 2
    goals.save_goal(active_goal.session_id, active_goal.state)
    assert pause_goal_if_active(active_goal.session_id)
    stored = goals.load_goal(active_goal.session_id)
    assert stored.status == 'paused'
    assert stored.turns_used == 2
    assert stored.max_turns == 4
    assert not pause_goal_if_active(active_goal.session_id)


@pytest.mark.asyncio
async def test_idle_gateway_stop_pauses_goal(active_goal):
    from gateway.run import GatewayRunner
    from gateway.config import GatewayConfig, Platform
    from gateway.platforms.event import MessageEvent, MessageType
    from gateway.session import SessionSource
    runner = GatewayRunner(config=GatewayConfig())
    source = SessionSource(platform=Platform.TELEGRAM, chat_id='goal-stop', chat_type='dm', user_id='owner')
    entry = await runner.async_session_store.get_or_create_session(source)
    mgr = await asyncio.to_thread(goals.GoalManager, entry.session_id)
    await asyncio.to_thread(mgr.set, 'Do not restart after /stop')
    await asyncio.to_thread(mgr.wait_for_seconds, 1)
    event = MessageEvent(text='/stop', message_type=MessageType.TEXT, source=source)
    await runner._handle_stop_command(event)
    assert (await asyncio.to_thread(goals.load_goal, entry.session_id)).status == 'paused'


@pytest.mark.parametrize('control', ['stop', 'pause', 'clear', 'replace', 'repark', 'reset-budget'])
def test_durable_control_wins_over_inflight_judge(active_goal, monkeypatch, control):
    from gateway.goal_stop import pause_goal_if_active

    active_goal.state.turns_used = 2
    goals.save_goal(active_goal.session_id, active_goal.state)
    evaluator = goals.GoalManager(active_goal.session_id)
    entered, release = threading.Event(), threading.Event()

    def blocked_judge(*args, **kwargs):
        entered.set()
        assert release.wait(5), 'bounded judge test did not release'
        return 'continue', 'More work remains', False, None, False

    monkeypatch.setattr(goals, 'judge_goal', blocked_judge)
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(evaluator.evaluate_after_turn, 'Finished this turn')
        try:
            assert entered.wait(5), 'judge was never entered'
            manager = goals.GoalManager(active_goal.session_id)
            assert manager.state.turns_used == 3
            mutations = {
                'stop': lambda: pause_goal_if_active(manager.session_id),
                'pause': manager.pause,
                'clear': manager.clear,
                'replace': lambda: manager.set('A replacement goal', max_turns=7),
                'repark': lambda: manager.wait_for_seconds(3600, 'New external dependency'),
                'reset-budget': manager.resume,
            }
            mutations[control]()
            accepted = goals.load_goal(manager.session_id).to_json()
        finally:
            release.set()
        decision = pending.result(timeout=5)
    assert goals.load_goal(active_goal.session_id).to_json() == accepted
    assert evaluator.state.to_json() == accepted
    assert decision['should_continue'] is False
    assert decision['continuation_prompt'] is None


@pytest.mark.parametrize('verdict', ['done', 'blocked', 'wait'])
def test_stop_wins_over_other_inflight_judge_outcomes(active_goal, monkeypatch, verdict):
    from gateway.goal_stop import pause_goal_if_active

    evaluator = goals.GoalManager(active_goal.session_id)
    entered, release = threading.Event(), threading.Event()

    def blocked_judge(*args, **kwargs):
        entered.set()
        assert release.wait(5), 'bounded judge test did not release'
        directive = {'seconds': 600} if verdict == 'wait' else None
        return verdict, 'A stale judge outcome', False, directive, False

    monkeypatch.setattr(goals, 'judge_goal', blocked_judge)
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(evaluator.evaluate_after_turn, 'Finished this turn')
        try:
            assert entered.wait(5), 'judge was never entered'
            assert pause_goal_if_active(active_goal.session_id)
            accepted = goals.load_goal(active_goal.session_id).to_json()
        finally:
            release.set()
        decision = pending.result(timeout=5)
    assert goals.load_goal(active_goal.session_id).to_json() == accepted
    assert decision['should_continue'] is False
    assert decision['continuation_prompt'] is None
    assert 'Goal achieved' not in decision['message']


def test_already_stopped_snapshot_does_not_call_judge(active_goal, monkeypatch):
    from gateway.goal_stop import pause_goal_if_active

    evaluator = goals.GoalManager(active_goal.session_id)
    assert pause_goal_if_active(active_goal.session_id)
    judge = Mock(return_value=('continue', 'stale', False, None, False))
    monkeypatch.setattr(goals, 'judge_goal', judge)
    decision = evaluator.evaluate_after_turn('A delayed turn callback')
    judge.assert_not_called()
    assert decision['should_continue'] is False
    assert goals.load_goal(active_goal.session_id).status == 'paused'


@pytest.mark.asyncio
async def test_gateway_does_not_enqueue_when_stop_lands_during_judge(active_goal, monkeypatch):
    from gateway.goal_stop import pause_goal_if_active
    from gateway.run_goals import GatewayGoalsMixin

    evaluator = goals.GoalManager(active_goal.session_id)
    entered, release = threading.Event(), threading.Event()

    def blocked_judge(*args, **kwargs):
        entered.set()
        assert release.wait(5), 'bounded judge test did not release'
        return 'continue', 'More work remains', False, None, False

    monkeypatch.setattr(goals, 'judge_goal', blocked_judge)
    monkeypatch.setattr(goals, 'gather_background_processes', lambda **kwargs: [])
    monkeypatch.setattr(goals, 'count_active_delegations', lambda sid: 0)

    async def execute(fn, *args):
        return await asyncio.to_thread(fn, *args)

    runner = SimpleNamespace(
        _post_turn_manager=AsyncMock(return_value=evaluator),
        _run_in_executor_with_context=execute,
        _goal_max_turns_from_config=lambda: 4,
        _defer_goal_status_notice_after_delivery=AsyncMock(),
        _delivery_adapter_for=lambda source: object(),
        _session_key_for_source=lambda source: 'stopped-goal',
        _synthetic_prompt_event=lambda source, prompt: prompt,
        _enqueue_fifo=Mock(),
    )
    pending = asyncio.create_task(GatewayGoalsMixin._post_turn_goal_continuation(
        runner, session_entry=SimpleNamespace(session_id=active_goal.session_id),
        source=SimpleNamespace(profile=None), final_response='Finished this turn',
    ))
    try:
        assert await asyncio.to_thread(entered.wait, 5), 'judge was never entered'
        assert await asyncio.to_thread(pause_goal_if_active, active_goal.session_id)
    finally:
        release.set()
    await asyncio.wait_for(pending, timeout=5)
    runner._enqueue_fifo.assert_not_called()
    assert (await asyncio.to_thread(goals.load_goal, active_goal.session_id)).status == 'paused'


def test_stop_between_conditional_read_and_write_wins(active_goal, monkeypatch):
    from gateway.goal_stop import pause_goal_if_active

    evaluator = goals.GoalManager(active_goal.session_id)
    db = goals._get_session_db()
    original_write = db._write_rowcount
    injected = False

    def write_after_stop(sql, params=(), **kwargs):
        nonlocal injected
        if not injected:
            injected = True
            assert pause_goal_if_active(active_goal.session_id)
        return original_write(sql, params, **kwargs)

    judge = Mock(return_value=('continue', 'stale', False, None, False))
    monkeypatch.setattr(db, '_write_rowcount', write_after_stop)
    monkeypatch.setattr(goals, 'judge_goal', judge)
    decision = evaluator.evaluate_after_turn('A delayed turn callback')
    assert injected
    judge.assert_not_called()
    assert decision['should_continue'] is False
    assert goals.load_goal(active_goal.session_id).status == 'paused'


def test_legacy_goal_row_can_still_be_evaluated(active_goal, monkeypatch):
    db = goals._get_session_db()
    db.set_meta(goals._meta_key('legacy-goal'), json.dumps({
        'goal': 'Legacy objective', 'status': 'active', 'max_turns': 4,
    }))
    evaluator = goals.GoalManager('legacy-goal')
    monkeypatch.setattr(goals, 'judge_goal', Mock(return_value=(
        'continue', 'More work remains', False, None, False,
    )))
    decision = evaluator.evaluate_after_turn('A completed turn')
    assert decision['should_continue'] is True
    assert goals.load_goal('legacy-goal').turns_used == 1


def test_stop_wins_over_inflight_quality_gate(active_goal, monkeypatch, tmp_path):
    from gateway.goal_stop import pause_goal_if_active

    active_goal.add_gate('bounded-test-gate')
    evaluator = goals.GoalManager(active_goal.session_id)
    entered, release = threading.Event(), threading.Event()

    def blocked_gate(*args, **kwargs):
        entered.set()
        assert release.wait(5), 'bounded gate test did not release'
        return False, 1, 'A stale gate failure'

    monkeypatch.setattr(goals, '_gate_workspace', lambda: (str(tmp_path), None))
    monkeypatch.setattr(goals, 'run_gate', blocked_gate)
    with ThreadPoolExecutor(max_workers=1) as executor:
        pending = executor.submit(evaluator.evaluate_after_turn, 'Finished this turn')
        try:
            assert entered.wait(5), 'gate was never entered'
            assert pause_goal_if_active(active_goal.session_id)
            accepted = goals.load_goal(active_goal.session_id).to_json()
        finally:
            release.set()
        decision = pending.result(timeout=5)
    assert goals.load_goal(active_goal.session_id).to_json() == accepted
    assert decision['should_continue'] is False
    assert decision['continuation_prompt'] is None


@pytest.mark.parametrize('control', ['spend', 'replace', 'clear'])
def test_stop_preserves_control_between_its_load_and_write(active_goal, monkeypatch, control):
    from gateway.goal_stop import pause_goal_if_active

    active_goal.state.turns_used = 2
    goals.save_goal(active_goal.session_id, active_goal.state)
    original_load = goals.load_goal
    injected = False
    accepted = None

    def load_before_control(session_id):
        nonlocal injected, accepted
        state = original_load(session_id)
        if not injected:
            injected = True
            manager = goals.GoalManager(session_id)
            if control == 'spend':
                manager.state.turns_used = 3
                goals.save_goal(session_id, manager.state)
            elif control == 'replace':
                manager.set('Replacement goal must survive', max_turns=7)
            else:
                manager.clear()
            accepted = original_load(session_id).to_json()
        return state

    monkeypatch.setattr(goals, 'load_goal', load_before_control)
    stopped = pause_goal_if_active(active_goal.session_id)
    stored = original_load(active_goal.session_id)
    assert injected
    if control == 'spend':
        assert stopped
        assert stored.status == 'paused'
        assert stored.turns_used == 3
        assert stored.max_turns == 4
    else:
        assert not stopped
        assert stored.to_json() == accepted

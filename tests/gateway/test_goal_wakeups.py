"""Parked gateway goals resume without user nudges or losing their durable barrier."""
import asyncio
import threading
from contextlib import asynccontextmanager, contextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from hermes_cli import goals


@pytest.fixture
def parked(tmp_path, monkeypatch):
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    goals._DB_CACHE.clear()
    goals._get_session_db()
    mgr = goals.GoalManager('parked-gateway')
    mgr.set('Write and verify the requested artifact', max_turns=4)
    mgr.wait_for_seconds(1)
    mgr.state.waiting_until = 1
    goals.save_goal(mgr.session_id, mgr.state)
    yield mgr
    goals._DB_CACHE.clear()


def runner_for(mgr):
    source = SimpleNamespace(profile=None)
    entry = SimpleNamespace(session_key='gateway-key', session_id=mgr.session_id,
                            suspended=False, origin=source)
    adapter = SimpleNamespace(_active_sessions=set(), handle_message=AsyncMock(), is_connected=True)
    store = SimpleNamespace(list_sessions=lambda: [entry], peek_session_id=lambda key: entry.session_id)
    runner = SimpleNamespace(session_store=store, _restored_source=lambda entry: entry.origin,
                             _profile_scope_for_source=lambda source: nullcontext(),
                             _profile_scope_key_for_source=lambda source: source.profile,
                             _delivery_adapter_for=lambda source: adapter,
                             _is_session_running=lambda key: False,
                             _queue_depth=lambda key, **kw: 0,
                             _resume_owner_authorized=lambda key, source: True,
                             _synthetic_prompt_event=lambda source, text: SimpleNamespace(source=source, text=text))
    async def execute(fn, *args, **kwargs):
        return await asyncio.to_thread(fn, *args, **kwargs)
    runner._run_in_executor_with_context = execute
    @asynccontextmanager
    async def scope(source):
        yield
    runner._async_profile_scope_for_source = scope
    runner._synthetic_prompt_event = lambda source, text: SimpleNamespace(source=source, text=text, metadata={})
    return runner, adapter, entry


@pytest.mark.asyncio
async def test_expired_barrier_wakes_once_and_remains_durable_until_turn(parked, monkeypatch):
    from gateway.goal_wakeups import wake_parked_goals_once
    runner, adapter, _ = runner_for(parked)
    loop_thread = threading.get_ident()
    original = goals.load_goal
    def checked_load(sid):
        assert threading.get_ident() != loop_thread
        return original(sid)
    monkeypatch.setattr(goals, 'load_goal', checked_load)
    await wake_parked_goals_once(runner)
    adapter.handle_message.assert_awaited_once()
    assert 'Write and verify' in adapter.handle_message.call_args.args[0].text
    assert adapter.handle_message.call_args.args[0].metadata['gateway_session_key'] == 'gateway-key'
    # A crash before native turn admission must leave something to resume from disk.
    state = await asyncio.to_thread(original, parked.session_id)
    assert state.waiting_until == 1
    assert state.turns_used == 0
    await wake_parked_goals_once(runner)
    adapter.handle_message.assert_awaited_once()  # bounded backoff between failed admissions


@pytest.mark.parametrize('blocked', ['paused', 'budget', 'busy', 'queued', 'unauthorized', 'reset', 'suspended', 'estop'])
@pytest.mark.asyncio
async def test_wakeup_preserves_admission_boundaries(parked, blocked, monkeypatch):
    from gateway.goal_wakeups import wake_parked_goals_once
    runner, adapter, entry = runner_for(parked)
    def exhaust_budget():
        parked.state.turns_used = parked.state.max_turns
        goals.save_goal(parked.session_id, parked.state)
    mutations = {
        'paused': parked.pause,
        'budget': exhaust_budget,
        'busy': lambda: setattr(runner, '_is_session_running', lambda key: True),
        'queued': lambda: setattr(runner, '_queue_depth', lambda key, **kw: 1),
        'unauthorized': lambda: setattr(runner, '_resume_owner_authorized', lambda *args: False),
        'reset': lambda: setattr(runner.session_store, 'peek_session_id', lambda key: 'new-session'),
        'suspended': lambda: setattr(entry, 'suspended', True),
        'estop': lambda: monkeypatch.setattr('agent.estop.check_paused', lambda *args: True),
    }
    mutations[blocked]()
    await wake_parked_goals_once(runner)
    adapter.handle_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_future_wait_and_unparked_goals_do_not_run(parked):
    from gateway.goal_wakeups import wake_parked_goals_once
    parked.wait_for_seconds(3600)
    runner, adapter, _ = runner_for(parked)
    await wake_parked_goals_once(runner)
    parked.stop_waiting()
    await wake_parked_goals_once(runner)
    adapter.handle_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_delivery_failure_and_process_restart_keep_wait_for_recovery(parked):
    from gateway.goal_wakeups import wake_parked_goals_once
    runner, adapter, _ = runner_for(parked)
    adapter.handle_message.side_effect = RuntimeError('adapter not ready')
    await wake_parked_goals_once(runner)
    assert (await asyncio.to_thread(goals.load_goal, parked.session_id)).waiting_until == 1
    # Fresh runner simulates a restarted supervisor against the same persisted goal.
    restarted, ready_adapter, _ = runner_for(parked)
    await wake_parked_goals_once(restarted)
    ready_adapter.handle_message.assert_awaited_once()


@pytest.mark.parametrize('change', ['pause', 'budget', 'repark', 'replace'])
@pytest.mark.asyncio
async def test_goal_mutation_after_scan_cancels_stale_admission(parked, change):
    from gateway.goal_wakeups import _parked_candidates, wake_parked_goals_once
    runner, adapter, _ = runner_for(parked)
    execute = runner._run_in_executor_with_context
    async def mutate_after_scan(fn, *args, **kwargs):
        result = await execute(fn, *args, **kwargs)
        if fn is _parked_candidates:
            def change_goal():
                mgr = goals.GoalManager(parked.session_id)
                if change == 'pause': mgr.pause()
                elif change == 'budget':
                    mgr.state.turns_used = mgr.state.max_turns
                    goals.save_goal(mgr.session_id, mgr.state)
                elif change == 'repark': mgr.wait_for_seconds(3600)
                else: mgr.set('A replacement goal')
            await asyncio.to_thread(change_goal)
        return result
    runner._run_in_executor_with_context = mutate_after_scan
    await wake_parked_goals_once(runner)
    adapter.handle_message.assert_not_awaited()


@pytest.mark.asyncio
async def test_wakeup_execution_fence_rejects_pause_after_adapter_admission(parked):
    from gateway.goal_wakeups import goal_wakeup_allowed, wake_parked_goals_once
    runner, adapter, _ = runner_for(parked)
    await wake_parked_goals_once(runner)
    event = adapter.handle_message.call_args.args[0]
    assert await asyncio.to_thread(goal_wakeup_allowed, event, parked.session_id)
    await asyncio.to_thread(parked.pause)
    assert not await asyncio.to_thread(goal_wakeup_allowed, event, parked.session_id)


async def fail_three_admissions(runner, adapter, clock):
    from gateway.goal_wakeups import wake_parked_goals_once

    adapter.handle_message.side_effect = RuntimeError('transport admission unavailable')
    for _ in range(4):
        clock[0] += 1000
        await wake_parked_goals_once(runner)
    assert adapter.handle_message.await_count == 3


@pytest.mark.asyncio
async def test_unavailable_goal_read_preserves_retry_cap_and_warns_once(parked, monkeypatch, caplog):
    from gateway.goal_wakeups import wake_parked_goals_once

    clock = [0.0]
    monkeypatch.setattr('gateway.goal_wakeups.time.monotonic', lambda: clock[0])
    runner, adapter, _ = runner_for(parked)
    await fail_three_admissions(runner, adapter, clock)
    with monkeypatch.context() as unavailable:
        unavailable.setattr(goals, 'load_goal', lambda sid: None)
        await wake_parked_goals_once(runner)
    for _ in range(3):
        clock[0] += 1000
        await wake_parked_goals_once(runner)
    assert adapter.handle_message.await_count == 3
    assert sum('exhausted' in record.message for record in caplog.records) == 1


@pytest.mark.parametrize('recovery', ['replacement', 'reconnection'])
@pytest.mark.asyncio
async def test_verified_transport_recovery_rearms_one_bounded_retry_cycle(
    parked, monkeypatch, caplog, recovery,
):
    from gateway.goal_wakeups import wake_parked_goals_once

    clock = [0.0]
    monkeypatch.setattr('gateway.goal_wakeups.time.monotonic', lambda: clock[0])
    runner, original, _ = runner_for(parked)
    await fail_three_admissions(runner, original, clock)
    if recovery == 'replacement':
        recovered = SimpleNamespace(_active_sessions=set(), handle_message=AsyncMock(), is_connected=True)
        runner._delivery_adapter_for = lambda source: recovered
    else:
        recovered = original
        original.is_connected = False
        clock[0] += 1000
        await wake_parked_goals_once(runner)
        assert original.handle_message.await_count == 3
        original.is_connected = True
    recovered.handle_message.side_effect = RuntimeError('a new bounded transport failure')
    for _ in range(5):
        clock[0] += 1000
        await wake_parked_goals_once(runner)
    expected = 3 if recovery == 'replacement' else 6
    assert recovered.handle_message.await_count == expected
    assert sum('exhausted' in record.message for record in caplog.records) == 2


@pytest.mark.asyncio
async def test_profile_scan_enters_each_runtime_scope_once(parked, tmp_path):
    from gateway.goal_wakeups import _parked_candidates
    from hermes_constants import reset_hermes_home_override, set_hermes_home_override

    runner, _, _ = runner_for(parked)
    homes = {profile: tmp_path / profile for profile in ('alpha', 'beta')}
    entered = []
    entries = []

    @contextmanager
    def scope(source):
        entered.append(source.profile)
        token = set_hermes_home_override(str(homes[source.profile]))
        try:
            yield
        finally:
            reset_hermes_home_override(token)

    for profile in ('alpha', 'alpha', 'beta'):
        sid = f'{profile}-{len(entries)}'
        source = SimpleNamespace(profile=profile)
        with scope(source):
            mgr = goals.GoalManager(sid)
            mgr.set(f'Finish {profile} task')
            mgr.wait_for_seconds(1)
            mgr.state.waiting_until = 1
            goals.save_goal(sid, mgr.state)
        entries.append(SimpleNamespace(session_key=sid, session_id=sid, suspended=False, origin=source))
    entered.clear()
    runner.session_store.list_sessions = lambda: entries
    runner._profile_scope_for_source = scope
    runner._profile_scope_key_for_source = lambda source: homes[source.profile]
    candidates = await asyncio.to_thread(_parked_candidates, runner)
    assert entered == ['alpha', 'beta']
    assert len(candidates) == len(entries)
    assert all(f'Finish {source.profile} task' in prompt for _, _, source, prompt, _ in candidates)


@pytest.mark.parametrize('changed', ['busy', 'queued', 'unauthorized', 'adapter'])
@pytest.mark.asyncio
async def test_changes_during_admission_probe_are_rechecked(parked, changed):
    from gateway.goal_wakeups import goal_wakeup_allowed, wake_parked_goals_once

    runner, original, _ = runner_for(parked)
    replacement = SimpleNamespace(_active_sessions=set(), handle_message=AsyncMock(), is_connected=True)
    execute = runner._run_in_executor_with_context

    async def change_after_probe(fn, *args, **kwargs):
        result = await execute(fn, *args, **kwargs)
        if fn is goal_wakeup_allowed:
            if changed == 'busy':
                runner._is_session_running = lambda key: True
            elif changed == 'queued':
                runner._queue_depth = lambda key, **kw: 1
            elif changed == 'unauthorized':
                runner._resume_owner_authorized = lambda *args: False
            else:
                runner._delivery_adapter_for = lambda source: replacement
        return result

    runner._run_in_executor_with_context = change_after_probe
    await wake_parked_goals_once(runner)
    original.handle_message.assert_not_awaited()
    replacement.handle_message.assert_not_awaited()

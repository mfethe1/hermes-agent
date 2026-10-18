"""Explicit /stop must prevent standing goals from waking themselves again."""
import asyncio
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

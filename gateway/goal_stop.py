"""Explicit user stops also stop the standing goal's automatic continuation."""
import logging

from hermes_cli.goals import GoalManager, _save_goal_if_unchanged

logger = logging.getLogger(__name__)


def pause_goal_if_active(session_id: str) -> bool:
    """Called off-loop under the owning profile; preserve consumed budget and goal evidence."""
    if not isinstance(session_id, str) or not session_id:
        return False
    manager = GoalManager(session_id)
    if not manager.is_active():
        return False
    identity = (manager.state.created_at, manager.state.goal)
    for _ in range(3):
        state = manager.state
        if not manager.is_active() or (state.created_at, state.goal) != identity:
            return False
        expected = state.to_json()
        state.status = 'paused'
        state.paused_reason = 'user-stop'
        state.clear_wait()
        if _save_goal_if_unchanged(session_id, state, expected):
            return True
        # Judging may have reserved a turn since the first read. Preserve its debit,
        # but never let a delayed stop replace a newer goal or a terminal state.
        manager = GoalManager(session_id)
    logger.warning('Goal stop could not persist after concurrent changes for %s', session_id)
    return False

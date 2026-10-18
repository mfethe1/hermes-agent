"""Explicit user stops also stop the standing goal's automatic continuation."""
from hermes_cli.goals import GoalManager


def pause_goal_if_active(session_id: str) -> bool:
    """Called off-loop under the owning profile; preserve consumed budget and goal evidence."""
    if not isinstance(session_id, str) or not session_id:
        return False
    manager = GoalManager(session_id)
    if not manager.is_active():
        return False
    manager.pause(reason='user-stop')
    return True

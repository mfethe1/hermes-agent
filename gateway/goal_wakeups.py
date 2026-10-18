"""Wake currently routed, parked goals when their durable wait barrier lifts.

A barrier remains on disk until a real turn evaluates it. A failed adapter admission or
process restart therefore cannot consume the wakeup. This does not unpause goals or sweep
arbitrary historic active goals into execution.
"""
from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass

logger = logging.getLogger('gateway.run')
_MAX_ADMISSIONS_PER_BARRIER = 3
_RETRY_SECONDS = 30.0


def _barrier_identity(state):
    return (state.created_at, state.waiting_since, state.waiting_until,
            state.waiting_on_pid, state.waiting_on_session)


def _ready_manager(session_id, identity=None):
    from hermes_cli.goals import GoalManager

    mgr = GoalManager(session_id)
    state = mgr.state
    if state is None or not mgr.is_active() or state.turns_used >= state.max_turns:
        return None
    if not any(_barrier_identity(state)[2:]):
        return None
    if identity is not None and _barrier_identity(state) != identity:
        return None
    return None if mgr.is_waiting(clear_if_ready=False) else mgr


def goal_wakeup_allowed(event, session_id):
    """Revalidate a synthetic wake after turn preparation, including a concurrent /goal pause."""
    expected = getattr(event, '_goal_wakeup_identity', None)
    if expected is None:
        return True
    expected_sid, barrier = expected
    return session_id == expected_sid and _ready_manager(session_id, barrier) is not None


def _parked_candidates(runner):
    """All storage and scoped barrier probes run on a worker, never the gateway loop."""
    from hermes_constants import get_hermes_home, reset_hermes_home_override, set_hermes_home_override

    candidates = []
    home = getattr(runner.session_store, '_routing_home', None) or get_hermes_home()
    token = set_hermes_home_override(str(home))
    try:
        by_scope = {}
        for entry in runner.session_store.list_sessions():
            if entry.suspended or not entry.session_id or entry.origin is None:
                continue
            try:
                source = runner._restored_source(entry)
                if source is not None:
                    by_scope.setdefault(runner._profile_scope_key_for_source(source), []).append((entry, source))
            except Exception:
                logger.debug('Parked goal routing failed for %s', entry.session_key, exc_info=True)
        for group in by_scope.values():
            try:
                with runner._profile_scope_for_source(group[0][1]):
                    candidates.extend(_ready_candidates(group))
            except Exception:
                logger.warning('Parked goal profile scan failed for %s', group[0][0].session_key, exc_info=True)
    finally:
        reset_hermes_home_override(token)
    return candidates


def _ready_candidates(group):
    candidates = []
    for entry, source in group:
        try:
            mgr = _ready_manager(entry.session_id)
            prompt = mgr.next_continuation_prompt() if mgr is not None else None
            if prompt:
                candidates.append((entry.session_key, entry.session_id, source, prompt, _barrier_identity(mgr.state)))
        except Exception:
            logger.debug('Parked goal read failed for %s', entry.session_key, exc_info=True)
    return candidates


@dataclass
class _AdmissionAttempts:
    count: int = 0
    retry_at: float = 0.0
    adapter: object = None
    connected: bool = False
    exhausted_logged: bool = False

    def observe_transport(self, adapter):
        """Only a replacement or an observed reconnection starts a new bounded cycle."""
        connected = adapter is not None and adapter.is_connected
        recovered = connected and self.adapter is not None and (
            adapter is not self.adapter or not self.connected
        )
        if adapter is not None:
            self.adapter = adapter
        self.connected = connected
        if recovered:
            self.count = 0
            self.retry_at = 0.0
            self.exhausted_logged = False
        return connected

    def exhausted(self, key):
        if self.count < _MAX_ADMISSIONS_PER_BARRIER:
            return False
        if not self.exhausted_logged:
            logger.warning('Parked goal admission exhausted for %s after %d attempts; '
                           'waiting for a goal change or verified transport recovery', key, self.count)
            self.exhausted_logged = True
        return True


async def _wake_candidate(runner, candidate, attempts):
    """Revalidate after every executor hop; intake claims the exact route synchronously.

    Adapter intake performs its usual claim and authorization. There is no await between
    the final idle/owner check and intake, matching the existing heartbeat admission path.
    """
    from agent.estop import check_paused

    key, sid, source, prompt, barrier = candidate
    token = (key, sid, barrier)
    state = attempts.setdefault(token, _AdmissionAttempts())
    async with runner._async_profile_scope_for_source(source):
        adapter = runner._delivery_adapter_for(source)
        if not state.observe_transport(adapter) or state.exhausted(key) or time.monotonic() < state.retry_at:
            return
        event = runner._synthetic_prompt_event(source, prompt)
        event.metadata['gateway_session_key'] = key
        event.metadata['gateway_session_id'] = sid
        event.metadata['gateway_session_strict'] = True
        event._goal_wakeup_identity = (sid, barrier)
        if not await runner._run_in_executor_with_context(goal_wakeup_allowed, event, sid):
            return
        current_adapter = runner._delivery_adapter_for(source)
        if current_adapter is not adapter or not adapter.is_connected:
            state.observe_transport(current_adapter)
            return
        if check_paused('goal', logger) or runner.session_store.peek_session_id(key) != sid:
            return
        if (runner._is_session_running(key) or key in adapter._active_sessions
                or runner._queue_depth(key, adapter=adapter) > 0):
            return
        if not runner._resume_owner_authorized(key, source):
            return
        # An observed new barrier, unlike an unavailable read, proves older retry counters obsolete.
        for previous in list(attempts):
            if previous[0] == key and previous != token:
                attempts.pop(previous)
        state.retry_at = time.monotonic() + _RETRY_SECONDS * 2 ** state.count
        state.count += 1
        try:
            await adapter.handle_message(event)
            logger.info('Submitted ready parked goal wakeup for %s (attempt %d/%d)',
                        key, state.count, _MAX_ADMISSIONS_PER_BARRIER)
        except Exception:
            logger.warning('Parked goal admission failed for %s; barrier retained', key, exc_info=True)
        state.exhausted(key)


async def wake_parked_goals_once(runner):
    """Bound retries per durable barrier, rearming only after verified transport recovery."""
    attempts = getattr(runner, '_goal_wakeup_attempts', None)
    if attempts is None:
        attempts = runner._goal_wakeup_attempts = {}
    candidates = await runner._run_in_executor_with_context(_parked_candidates, runner)
    for candidate in candidates:
        try:
            await _wake_candidate(runner, candidate, attempts)
        except Exception:
            logger.warning('Parked goal wakeup failed for %s; barrier retained', candidate[0], exc_info=True)


async def watch_parked_goals(runner, *, interval: float = 5.0):
    """Supervised poller; current routing and goal state are restored after every restart."""
    while runner._running:
        try:
            await wake_parked_goals_once(runner)
        except Exception:
            logger.warning('Parked goal scan failed; retrying on next poll', exc_info=True)
        await asyncio.sleep(interval)

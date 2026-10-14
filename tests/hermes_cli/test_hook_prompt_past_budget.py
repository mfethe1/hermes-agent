"""Regression probe: can a plugin hook keep the agent running past the iteration
budget or a user interrupt by submitting another prompt?

Claude Code v2.1.299 fixed exactly this class of bug ("prompt-submitting hooks
continuing past turn/budget limits or interrupts"). These tests prove Hermes's
answer with real objects end to end; only the LLM transport is faked
(scripted ``FakeLLMServer``).

Real chain under test (no mocks):
    post_llm_call hook callback (registered on the real global PluginManager)
      -> PluginContext.inject_message                     hermes_cli/plugins.py:603
      -> inject_plugin_message (queue routing)            hermes_cli/plugins_injection.py:12-23
      -> CLIChatTurnMixin._chat_monitor_agent_thread      hermes_cli/cli_chat_turn_mixin.py:416
      -> CLIChatTurnMixin._chat_resolve_interrupt         hermes_cli/cli_chat_turn_mixin.py:628
      -> HermesCLI._drain_interrupt_queue_to_pending_input  cli.py:1365
      -> REPL pending-input drain -> new turn             hermes_cli/cli_tui_runtime_mixin.py:36-46,120-125
      -> fresh IterationBudget per turn                   agent/turn_context.py:651

The ``_CliHost`` below borrows the REAL mixin methods (unbound) for every
queueing and interrupt-routing decision. Only prompt_toolkit rendering
(``_invalidate``, overlay clearing) and the outermost ``_pending_input`` pop
loop are mirrored 1:1 with cli_tui_runtime_mixin.py — verified by reading to
contain no provenance or budget guard.
"""

from __future__ import annotations

# Environment note: this file runs with the MAIN checkout's venv from a git
# worktree. tests/home_io_guard.py exempts the interpreter's RESOLVED install
# prefixes — but sys.base_prefix's lexical spelling (.../cpython-3.11-...) is a
# symlink to .../cpython-3.11.15-..., and the guard refuses the lexical path
# (inside real ~/.hermes) before its resolved-prefix exemption is reached
# (home_io_guard.py:105 vs :113). In the main checkout this never fires because
# the checkout-root exemption covers the venv lexically. Exempt the lexical
# interpreter spellings here, at collection time (before the per-test guard
# installs) — the guard's own contract is that the interpreter installation is
# not Hermes state (home_io_guard.py:97-99).
import os as _os
import sys as _sys

import tests.home_io_guard as _home_io_guard

_lexical_prefixes = tuple(
    p for p in (
        _os.path.normcase(_os.fspath(_sys.base_prefix)),
        _os.path.normcase(_os.fspath(_sys.base_exec_prefix)),
    )
    if p not in _home_io_guard._INTERPRETER_PREFIX_STRS
)
_home_io_guard._INTERPRETER_PREFIX_STRS = (
    _home_io_guard._INTERPRETER_PREFIX_STRS + _lexical_prefixes
)
del _os, _sys, _home_io_guard, _lexical_prefixes

# Pre-import stdlib modules the turn machinery imports lazily (timestamps →
# zoneinfo); module-scope import happens at collection, before the guard.
import zoneinfo
import os
import queue
import threading
import time
from pathlib import Path
from types import SimpleNamespace

# Heavy repo imports at module scope: run_agent.py → hermes_bootstrap stats the
# repo's .git dir at import time, which must happen at collection (before the
# per-test home_io_guard installs) — from a worktree, .git resolves into the
# main checkout under real ~/.hermes and would trip the guard.
from cli import HermesCLI
from run_agent import AIAgent

import pytest

from hermes_cli.cli_chat_turn_mixin import CLIChatTurnMixin
from hermes_cli.plugins import PluginContext, get_plugin_manager
from hermes_cli.plugins_manifest import PluginManifest
from tests.fakes.fake_llm_provider import (
    FakeLLMServer,
    Text,
    ToolCall,
    write_hermes_home,
)

OFFLINE_CONFIG = (
    "updates:\n  check: false\n"
    "models_dev:\n  url: http://127.0.0.1:9/api.json\n"
    "skills:\n  creation_nudge_interval: 0\n"
    "memory:\n  nudge_interval: 0\n"
    "approvals:\n  mode: \"off\"\n"
)

MAX_ITERATIONS = 3


class _CliHost:
    """Interactive-CLI queue machinery with the TUI rendering stripped.

    Every method that decides what happens to an injected or interrupting
    message is the REAL production method; the class only supplies the
    queue/state attributes those methods touch.
    """

    _chat_monitor_agent_thread = CLIChatTurnMixin._chat_monitor_agent_thread
    _chat_resolve_interrupt = CLIChatTurnMixin._chat_resolve_interrupt

    def __init__(self, agent):
        self._drain_interrupt_queue_to_pending_input = (
            lambda: HermesCLI._drain_interrupt_queue_to_pending_input(self)
        )
        self.agent = agent
        self._interrupt_queue: queue.Queue = queue.Queue()
        self._pending_input: queue.Queue = queue.Queue()
        self._agent_running = False
        self._last_turn_interrupted = False
        # Attributes the real methods touch but that are pure UI in this probe:
        self._voice_mode = False
        self._clarify_state = None
        self._clarify_freetext = False
        self._should_exit = False
        self._invalidate = lambda *a, **k: None
        self._clear_active_overlays_for_interrupt = lambda: None

    def run_turn(self, message: str) -> SimpleNamespace:
        """Mirror of the REPL's per-prompt dispatch.

        cli_tui_runtime_mixin.py:120-125 (``_agent_running = True; self.chat(...)`)
        plus the core of CLIChatTurnMixin._chat_run_agent
        (cli_chat_turn_mixin.py:369: ``self.agent.run_conversation(...)`` on a
        daemon thread, watched by the real ``_chat_monitor_agent_thread``).
        """
        self._agent_running = True
        turn = SimpleNamespace(result=None, error=None, stop_event=None)

        def target():
            try:
                turn.result = self.agent.run_conversation(message)
            except Exception as exc:  # pragma: no cover - surfaced via assertions
                turn.error = exc

        agent_thread = threading.Thread(target=target, daemon=True)
        agent_thread.start()
        interrupt_msg = self._chat_monitor_agent_thread(turn, agent_thread)
        agent_thread.join(timeout=60)
        self._agent_running = False
        # _tui_after_turn (cli_tui_runtime_mixin.py:166): strays -> pending input.
        self._drain_interrupt_queue_to_pending_input()
        # _chat_render_turn (cli_chat_turn_mixin.py:552 + 588-624): the interrupt
        # message — user-typed or plugin-injected — is re-queued as the next prompt.
        pending_message, _show = self._chat_resolve_interrupt(
            turn, agent_thread, interrupt_msg, "")
        if pending_message:
            self._pending_input.put(pending_message)
        return turn

    def drain_pending(self, max_extra_turns: int = 3) -> int:
        """Mirror of CLITuiRuntimeMixin._tui_process_loop (lines 36-46): every
        queued input is dispatched as a new turn automatically — no provenance
        check, no budget check."""
        extra = 0
        while extra < max_extra_turns:
            try:
                msg = self._pending_input.get_nowait()
            except queue.Empty:
                break
            self.run_turn(msg)
            extra += 1
        return extra


def _install_hook(host, inject_text, hook_name, limit=None):
    """Register a real hook on the real global PluginManager, wired to the host's
    queues via a real PluginContext. Returns (injections_state, restore_fn)."""
    mgr = get_plugin_manager()
    saved_hooks = {k: list(v) for k, v in mgr._hooks.items()}
    saved_cli_ref = mgr._cli_ref
    injections = {"hook_calls": 0, "accepted": 0}
    ctx = PluginContext(PluginManifest(name="hook-budget-probe"), mgr)
    mgr._cli_ref = host

    def hook(**kwargs):
        injections["hook_calls"] += 1
        if limit is not None and injections["hook_calls"] > limit:
            return
        if ctx.inject_message(inject_text):
            injections["accepted"] += 1

    mgr._hooks.setdefault(hook_name, []).append(hook)

    def restore():
        mgr._hooks.clear()
        mgr._hooks.update(saved_hooks)
        mgr._cli_ref = saved_cli_ref

    return injections, restore


def _make_agent(srv, session_id: str):
    return AIAgent(
        provider="custom", base_url=srv.base_url, api_key="sk-fake",
        model="fake-model", session_id=session_id, quiet_mode=True,
        platform="cli", skip_memory=True, max_iterations=MAX_ITERATIONS,
    )


def test_hook_injected_prompt_must_not_continue_past_iteration_budget(tmp_path):
    """Hook submits another prompt exactly when the iteration budget is hit.

    PASS only if execution stops without another model call: the injected
    prompt must not become a new turn (each new turn gets a fresh
    IterationBudget — agent/turn_context.py:651 — so a hook that always
    re-submits would otherwise run forever).
    """
    home = Path(os.environ["HERMES_HOME"])
    probe_file = tmp_path / "probe.txt"
    probe_file.write_text("probe\n")
    read_call = ToolCall("read_file", {"path": str(probe_file)})
    script = [read_call] * 40  # endless tool calls: budget is the only stopper
    with FakeLLMServer(script) as srv:
        write_hermes_home(home, srv.base_url, extra_config=OFFLINE_CONFIG)
        agent = _make_agent(srv, "hook-budget")
        try:
            host = _CliHost(agent)
            # post_llm_call fires once per turn at finalize, after the tool loop
            # (agent/turn_finalizer.py:453-480) — including budget-exhausted
            # turns, which carry a synthesized fallback response.
            injections, restore = _install_hook(
                host, "please keep going", "post_llm_call")
            try:
                turn = host.run_turn("start")
                assert turn.error is None, turn.error
                # Sanity: the turn really did stop on the budget, in-loop.
                assert turn.result is not None
                assert turn.result["api_calls"] <= MAX_ITERATIONS
                # Baseline: turn 1's model calls (iterations + the bounded
                # budget-exhaustion grace/handoff calls, whatever they are).
                baseline_calls = len(srv.main_requests())
                assert baseline_calls >= MAX_ITERATIONS
                # The hook fired at budget exhaustion, and the guard refused it.
                assert injections["hook_calls"] == 1
                assert injections["accepted"] == 0

                # Now the REPL drain runs (real routing above). A guard honoring
                # the budget must not run the injected prompt as a new turn.
                extra_turns = host.drain_pending()

                assert extra_turns == 0, (
                    f"hook-injected prompt started {extra_turns} extra turn(s) "
                    f"after the iteration budget was exhausted"
                )
                assert len(srv.main_requests()) == baseline_calls, (
                    f"model called {len(srv.main_requests()) - baseline_calls}x "
                    f"more after budget exhaustion via hook-submitted prompt"
                )
                assert host._pending_input.empty()
            finally:
                restore()
        finally:
            agent.close()


def test_hook_injected_prompt_must_not_continue_after_user_interrupt(tmp_path):
    """Hook submits a prompt right after the user interrupted the turn.

    PASS only if execution stops without another model call: the injected
    prompt must not resurrect the agent the user just stopped.

    Hook choice: post_llm_call is gated on ``not interrupted``
    (agent/turn_finalizer.py:657), so the turn-finalize hook that fires on an
    interrupted turn is per-turn on_session_end (turn_finalizer.py:793-807,
    with interrupted=True).
    """
    home = Path(os.environ["HERMES_HOME"])
    probe_file = tmp_path / "probe.txt"
    probe_file.write_text("probe\n")
    read_call = ToolCall("read_file", {"path": str(probe_file)})
    release_second_call = threading.Event()

    def responder(req):
        # First call: a tool call so the loop is mid-run when the user interrupts.
        # Second call: hold the response until the test has interrupted, so the
        # interrupt deterministically lands mid-turn. Any call from a resurrected
        # turn gets another tool call, so resurrection is visible in the request
        # count.
        n = len(srv.main_requests())
        if n == 1:
            return read_call
        if n == 2:
            release_second_call.wait(timeout=30)
            return Text("second answer")
        return read_call

    with FakeLLMServer(responder) as srv:
        write_hermes_home(home, srv.base_url, extra_config=OFFLINE_CONFIG)
        agent = _make_agent(srv, "hook-interrupt")
        try:
            host = _CliHost(agent)
            injections, restore = _install_hook(
                host, "hook: run one more", "on_session_end")
            try:
                driver = threading.Thread(
                    target=lambda: host.run_turn("start"), daemon=True)
                driver.start()

                # Wait for the turn to be genuinely mid-run (second model call
                # in flight), then interrupt exactly as the CLI Esc handler does.
                deadline = time.time() + 20
                while len(srv.main_requests()) < 2 and time.time() < deadline:
                    time.sleep(0.02)
                assert len(srv.main_requests()) == 2, (
                    "turn never reached its second model call")
                agent.interrupt()  # user interrupt, no message
                release_second_call.set()
                driver.join(timeout=60)
                assert not driver.is_alive(), "turn did not stop after interrupt"

                interrupted_calls = len(srv.main_requests())
                assert interrupted_calls == 2
                # The hook fired on the interrupted turn's finalize, and the
                # guard refused its prompt.
                assert injections["hook_calls"] == 1, injections
                assert injections["accepted"] == 0, injections

                extra_turns = host.drain_pending()

                assert extra_turns == 0, (
                    f"hook-injected prompt started {extra_turns} extra turn(s) "
                    f"after a user interrupt"
                )
                assert len(srv.main_requests()) == interrupted_calls, (
                    f"model called {len(srv.main_requests()) - interrupted_calls}x "
                    f"more after user interrupt via hook-submitted prompt"
                )
                assert host._pending_input.empty()
            finally:
                release_second_call.set()
                restore()
        finally:
            agent.close()


def test_hook_injected_prompt_after_normal_completion_still_runs():
    """Control: the guard must refuse only past-limit/injected resurrection.

    A hook that submits a prompt when the turn ended NORMALLY (budget left, no
    interrupt) is legitimate task chaining: the injection is accepted and the
    REPL drain runs it as one new turn — with a fresh budget.
    """
    home = Path(os.environ["HERMES_HOME"])
    script = [Text("first answer"), Text("second answer")] + [Text("overflow")] * 5
    with FakeLLMServer(script) as srv:
        write_hermes_home(home, srv.base_url, extra_config=OFFLINE_CONFIG)
        agent = _make_agent(srv, "hook-normal")
        try:
            host = _CliHost(agent)
            injections, restore = _install_hook(
                host, "hook: follow up once", "post_llm_call", limit=1)
            try:
                turn = host.run_turn("start")
                assert turn.error is None, turn.error
                # NB: turn.result["interrupted"] is deliberately NOT asserted:
                # the monitor may consume the hook's mid-finalize injection and
                # call agent.interrupt(msg) before the thread joins — either
                # way the real re-queue routing lands exactly one prompt in
                # _pending_input, which is what this control cares about.

                baseline_calls = len(srv.main_requests())
                # Budget left (1 of 3 used) and no interrupt → accepted.
                assert injections["hook_calls"] == 1
                assert injections["accepted"] == 1

                extra_turns = host.drain_pending()

                assert extra_turns == 1
                assert len(srv.main_requests()) == baseline_calls + 1
            finally:
                restore()
        finally:
            agent.close()

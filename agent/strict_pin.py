"""Strict model pin enforcement (buzz-79398 recommendation F).

Contract (task buzz-79398, Telegram thread 79398):
  * A job that explicitly pins model and/or provider must NOT complete on a
    substituted route: the runtime fallback chain (global ``fallback_providers``
    in config.yaml, activated by ``try_activate_fallback`` on 429/5xx) overrides
    the dispatcher's pin today. Unpinned jobs RETAIN approved fallback.
  * Requested AND actual model/provider must be persisted in run evidence
    (_FireAudit usage line + execution record), so substitution is visible.
  * Strict-pinned runs must block/defer respecting cooldowns, not tight-retry.
  * Do not remove global fallback for other jobs.

Mechanism:
  * ``strict_pin_from_job(job, cfg)`` reads the per-job ``strict_pin`` flag
    (or config default) and returns a StrictPin describing the requested route.
  * ``_resolve_cron_agent_setup`` (cron/scheduler.py) sets
    ``agent._strict_pin`` when the job is strictly pinned; a plain (non-strict)
    job leaves the runtime untouched.
  * ``try_activate_fallback`` checks ``agent._strict_pin`` FIRST: when set, it
    refuses to activate any fallback, arms the cooldown, and returns False so
    the run fails loudly on the pinned route instead of silently substituting.
  * The audit line persists requested + actual model/provider so post-hoc
    mismatch detection (independent reviewer) is possible.

The mechanism intentionally does NOT remove the global fallback chain: it is
per-job opt-in strictness layered over the existing approved fallback policy.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

logger = logging.getLogger(__name__)

_STRICT_PIN_KEY = "strict_pin"


@dataclass(frozen=True)
class StrictPin:
    """Requested route for a strictly-pinned job."""

    provider: Optional[str] = None
    model: Optional[str] = None
    reason: str = "explicit job pin"

    def matches(self, provider: Optional[str], model: Optional[str]) -> bool:
        p_ok = not self.provider or (provider or "").strip().lower() == self.provider.strip().lower()
        m_ok = not self.model or (model or "").strip().lower() == self.model.strip().lower()
        return p_ok and m_ok

    def describe(self) -> str:
        return f"{self.model or '*'}@{self.provider or '*'}"


def _norm(s: Optional[str]) -> Optional[str]:
    v = (s or "").strip().lower()
    return v or None


def strict_pin_from_job(job: Dict[str, Any], cfg: Dict[str, Any]) -> Optional[StrictPin]:
    """Return the StrictPin when the job opts in, else None.

    Opt-in sources (first wins):
      1. job["strict_pin"] == True and job["model"]/["provider"] carry the pin
      2. job["strict_pin"] is a dict {"provider": ..., "model": ...}
      3. config cron.strict_pin_default: true — every job with an explicit
         model/provider pin becomes strict.
    """
    flag = job.get(_STRICT_PIN_KEY)
    if isinstance(flag, dict):
        p, m = _norm(flag.get("provider")), _norm(flag.get("model"))
        if p or m:
            return StrictPin(provider=p, model=m, reason="job strict_pin dict")
        return None
    if flag is True:
        p, m = _norm(job.get("provider")), _norm(job.get("model"))
        if p or m:
            return StrictPin(provider=p, model=m, reason="job strict_pin flag")
        # strict_pin=True with no explicit axis: nothing to enforce — unpinned
        return None
    cfg_flag = (cfg.get("cron") or {}).get("strict_pin_default") if isinstance(cfg, dict) else None
    if cfg_flag is True:
        p, m = _norm(job.get("provider")), _norm(job.get("model"))
        if p or m:
            return StrictPin(provider=p, model=m, reason="cron.strict_pin_default")
    return None


def pin_state_from_agent(agent: Any) -> Optional[StrictPin]:
    """Read back the strict-pin state the scheduler set on the agent."""
    sp = getattr(agent, "_strict_pin", None)
    return sp if isinstance(sp, StrictPin) else None


def fallback_blocked_by_pin(agent: Any, reason: Any = None) -> bool:
    """True when strict pin forbids activating any fallback for this agent.

    Called at the top of ``try_activate_fallback``: when strict-pinned, no
    fallback is activated — the run fails on the pinned route and the caller's
    existing cooldown/backoff logic governs retry timing (no tight retry).
    """
    sp = pin_state_from_agent(agent)
    if sp is None:
        return False
    from agent.fallback_cooldown import _arm_rate_limit_cooldown
    try:
        _arm_rate_limit_cooldown(agent, reason)
    except Exception:
        logger.debug("strict-pin cooldown arm failed", exc_info=True)
    logger.warning(
        "STRICT-PIN: fallback refused for pinned route %s (%s) — failing on the "
        "pinned route rather than substituting", sp.describe(), sp.reason)
    try:
        from agent.chat_completion_helpers import _buffer_fallback_notice
        _buffer_fallback_notice(
            agent,
            f"🔒 Strict pin {sp.describe()} ({sp.reason}): fallback refused — "
            f"this run cannot complete on a substituted route.")
    except Exception:
        pass
    return True

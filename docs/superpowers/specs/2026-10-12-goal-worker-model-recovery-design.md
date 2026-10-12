# Same-worker model recovery

## User decision

Michael requests smooth ongoing delivery for Ironstone, Ecker, Construct Pro, the BuildBid replacement, Agent Household, pi-warden and Jev-related work, with resumability and a few alternate-model attempts when an agent blocks on small issues. On October 12 he explicitly selected **the original worker with a different model**, not replacement workers or advice-only reviews.

Worker identity means the same owning profile, gateway/host, session ID, session key, goal, repository claim and checkpoint. It does not require retaining a Python AIAgent object when Hermes' supported model-switch lifecycle evicts/reconstructs that object. No new implementation worker, duplicate lease or replacement conversation may be created.

## Outcome

At a completed-turn boundary, a recoverable goal block can trigger a session-only change to a different available, approved, compatible worker model. That worker reads its existing checkpoint, takes one bounded authorized next step and reports an evidence-backed recommended path. At most **three model-handoff attempts per recovery episode** are permitted. Genuine approvals, safety limits, ownership conflicts, exhausted goal budgets and unresolved hard resource floors remain enforced.

## Current implementation to extend

- `hermes_cli/goal_recovery.py`: vendor-neutral recovery assessment, revision bindings, saved options, explicit selections and two distinct safe-plan recovery attempts.
- Local `plugins/goal-policy/`: proposal generation, Clef/OpenRouter classification, direct Jev/TypeSafe uncertainty review, private decision audit and preference context. Current provider probes pass; calibration is unvalidated.
- `gateway/slash_commands_model.py`: supported in-place `AIAgent.switch_model`, session model persistence, next-turn notes, provider resolution, failure rollback and next-turn cache eviction.
- `gateway/run_agent_cache.py`: profile/session overrides, restoration, credential re-resolution and intentional-switch detection.
- Existing session leases and routing remain the ownership authority.

Extend these mechanisms. Do not inject fake human slash commands, expose a new model tool, patch live frames/core globals from a plugin, write credential-bearing model overrides directly into SQLite, or implement another independent worker scheduler.

## Recovery decision

1. Capture exact owning-session identity, goal revision, final response, blocker reason, current model route, saved checkpoint and existing permissions.
2. Evaluate whether this is recoverable technical/service/dependency/interpretation failure with a concrete next diagnostic or repair step. A refusal involving deletion, spending, credential/security changes, release/publication, missing consent or exclusive ownership is not an authorization to change models until one agrees.
3. Preserve the existing recommended recovery options and their risk, authorization, effects, reversibility and verification. Existing confident safety/authority vetoes cannot be erased by another model. Continue independent authorized work when available rather than declaring the whole objective impossible.
4. If a worker-model handoff is eligible, select the next distinct approved route that has passed generation/capability checks. Skip the current route and routes already attempted in the episode. Model choice alone is not evidence a blocker is resolved.
5. Reserve an attempt and persist a checkpoint-bound pending handoff before applying it. Revalidate goal revision, owning profile, session/lease and current user control state at the completed-turn boundary.
6. Apply the existing session-only model switch. Do not change global defaults, profile ownership, tools/approvals, goal requirements or turn caps. Only after a committed switch may the original worker receive its continuation.
7. The continuation starts with Hermes' canonical standing-goal prefix, names the actual new route, retains the blocker and constraints, and directs one bounded next step with verification. No budget reset or status-only polling is allowed.
8. On success, keep the working session model and record actual progress. On another qualifying block, consider the next untried route. After three attempts, stop switching and present the best supported recommendation plus the exact unresolved input; retain all work for explicit resumption.

## Model routes and availability

Routes are operator-configured provider/model identifiers from already configured Hermes providers. No proposal can introduce an API key, endpoint, global provider change or new subscription. A provider listing/Doctor models probe alone does not establish generation access.

Before enabling a route, make one bounded benign generation probe through that profile's normal runtime credential resolver, require the actual model/provider provenance and check required tool/context capabilities. Candidate routes are drawn from the configured Codex, Kimi and GLM paths; Claude is eligible only when its requested route is independently generation-qualified in the owning profile. Do not invent model IDs, borrow credentials from another profile or silently fall back after a failed/unknown probe.

The configured ordered allow-list, minus unavailable/incompatible/current/previously attempted routes, determines the chain. If none remains, hold with a truthful diagnostic rather than pretending a model changed.

## Persistence and concurrency

Persist non-secret requested and actual route, episode ID, source goal binding, attempt ordinal, prior route, checkpoint binding, transition state and outcome. Credentials remain in the existing scoped resolver. An episode retains its attempt count across restarts; automation cannot clear it by calling resume. A deliberate authorized user control action can start a new episode without automatically extending the original goal budget.

Transitions are: `reserved -> committed -> continued -> progressing/blocked`, or `reserved -> failed/cancelled`. No fourth reservation is possible. Transport/commit failures consume a bounded attempt so they cannot form an infinite retry loop. Same-target replays are deduplicated. A restart never interprets an unknown outcome as permission to launch another actor.

Changing, pausing, clearing or completing a goal during assessment cancels a stale handoff. Do not overwrite newer user model choices. A busy in-flight turn is never interrupted to apply a recovery switch. Cross-profile targeting fails closed. No active worktree ownership is transferred.

## Failure behavior

- Provider/auth/capability failure: retain or restore the prior valid session route through existing rollback semantics; report the unavailable target without deleting keys or modifying guardrails.
- Durable-write failure: do not claim a restart-surviving switch; retain work and a precise repair diagnostic.
- Repeated block: retain actual failed attempts and evidence, produce a recommended path, then hold at the cap.
- Real resource floor: do not consume a new worker/model attempt to bypass it. Send the scoped resource facts to the existing authorized cleanup owner, retain the checkpoint, perform independent safe work where available and retry admission only after a materially changed condition.
- Real ownership/approval boundary: recommend independent authorized work or request the specific decision. No writer takeover, fabricated approval or automatic publication/deployment.

## Cache and compatibility

Switching model/provider can lose that provider's prompt cache; Michael explicitly chose this tradeoff for bounded blocker recovery. Do not otherwise rewrite canonical history, inject changing system prompts, reload memories/toolsets or weaken verification to make a new route fit. Use the existing provider-aware history conversion and model-switch capability/context checks. Report the actual route, not a requested alias that silently ran on another backend.

Primary scope is the current authoritative gateway workers for the named programs. Shared goal state remains vendor-neutral. CLI/TUI/Desktop consumers must either support the same deferred switch contract or display a truthful unsupported-handoff choice; never pretend a gateway-only implementation covers every surface.

## User-visible output

Each handoff reports `attempt N/3`, prior and actual new model, original session identity, reason, recommended next action and required verification. A hold reports whether it is a real prerequisite, unavailable route or exhausted recovery sequence, and the exact way to resume without rebuilding the project. Recommendations and completion claims stay distinct.

## Acceptance contracts

- A synthetic small technical block switches only the original idle-boundary session; original session ID/key, goal/contract/gates, claim and budget caps remain identical.
- Three distinct handoff reservations succeed at most; a fourth, duplicate, stale or cross-profile request is refused.
- New user pause/clear/model change wins over pending automation.
- Auth/transport/commit failure does not leave a broken persistent override or duplicate work.
- Real approval, destructive/spend/security/publish/deploy, ownership and resource-floor blocks are retained even when another model recommends ignoring them.
- Restart replays a pending transition safely and never resets its attempt count.
- Actual model provenance is independently verified before enabling the chain and on the pilot switch.
- A small authorized live pilot verifies unchanged worker identity, continuing work and Telegram feedback before rollout to additional major goals.
- Existing goal, model-switch, profile-isolation and authorization tests pass through the canonical runner against isolated homes. No tests copy/write the live 15.2 GB session database.

## Doctor is a separate remediation track

Doctor currently exits 1 with 25 findings, not an all-clear. Remediation must not be replaced by model switching. Prioritize the documented IPv4 transport setting and semantics-preserving obsolete-config cleanup; verify provider/key/endpoint intent rather than destroying working Coding Plan credentials; stage reviewed lockfile/PM dependency fixes; resolve shared-bot topology deliberately.

Do not blindly invoke Doctor `--fix`: its active-store health probe copies the approximately 15.2 GB database, while free disk is only a few GB. No history pruning, busy WAL checkpoint, forced gateway restart, VM deletion or credential removal is authorized merely to make Doctor print green. Capacity-qualified maintenance and any exact destructive/security/topology decisions remain explicit.

## Rollout and rollback

Implement and test in `/Users/mfethe/.hermes/worktrees/goal-model-recovery-20261012`, based on the existing goal-recovery commit. Keep the live checkout, local Telegram flood-control fixes and running program work untouched during development. Enable only generation-qualified scoped routes after acceptance and an explicit, checkpointed activation plan. Rollback disables further model handoffs; it preserves goal/session history, current work, attempt audit and valid manually chosen routes.

## Review status

The same-worker/different-model direction is confirmed by Michael. This written boundary/rollout spec is saved for his review before implementation begins. No new model-handoff chain has been installed yet.

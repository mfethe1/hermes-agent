# RESPONSE-VERIFICATION-001 bounded core result

Status: PARTIAL. The bounded core is implemented; this is not fleet enforcement.

## Boundary

This leaf implements only a deterministic completion-eligibility core. Approved
criteria and evidence receipts enter through explicit trusted input types. No gateway,
delivery adapter, runtime configuration, credential, cron, or provider is changed.

The core never accepts or releases model-authored draft prose. Its renderer selects
only fixed status text. Receipt hashes detect changed receipt fields; they do not
authenticate an issuer or defend against a same-UID attacker. A future independent
collector must authenticate issuers and construct the trusted receipt records before
calling this core.

## Implemented contract

- Completion requires a non-empty, exact match to criteria held in an
  `ApprovedContext` supplied outside the model envelope.
- Task, attempt, artifact, environment, and approved-request digests must match
  across context, envelope, and every receipt.
- Receipt content hashes, freshness, independent-collector origin, and configured
  issuer membership are checked explicitly. Model/agent self-reports are rejected.
- State criteria require one verified observation. External-effect criteria require
  one verified effect receipt and one verified read-back linked to its exact hash.
- Duplicate or unexpected evidence fails closed. Unknown external outcomes and
  missing read-backs produce a controlled uncertain status and never completion.
- The renderer emits one of three fixed strings and has no draft-text input.

## Verification

Command:

```text
HERMES_PYTHON=/opt/homebrew/bin/python3 scripts/run_tests.sh tests/response_verification/test_core.py --tb=short
```

Result: `23 passed, 0 failed` in the canonical per-file runner. The interpreter was
the existing Homebrew Python 3.14 with pytest 9.0.2 because the worktree and shared
Hermes Python 3.11 venv lack pytest. Python 3.11 and 3.14 compilation,
`git diff --check`, Ruff 0.15.0 lint, and Ruff formatting checks passed. Ty was
unavailable; no dependencies were installed.

## Uncovered criteria

- No gateway or adapter calls the core, buffers output, or handles verifier
  crash/timeout. No response path is enforced.
- No independent evidence collector, authenticated issuer transport, signing key,
  hardware identity, or separate service identity exists in this leaf.
- The trusted approval layer that proves request authority and constructs
  `ApprovedContext` is not implemented. Echoing its digest is only a binding check.
- Tests supply trusted records as inputs; they do not independently observe world
  state or establish same-UID security.
- Freshness is enforced within one evaluation, but durable receipt replay tracking is
  not implemented.
- Lean conformance, fleet rollout, runtime activation, deployment, and already-running
  session handling remain for later bounded work.

## Repository checkpoint

- Base SHA: `f03ed94a34f47ebca57e4a1b0a890bc2aeb5e140`
- Package: `response_verification/`
- Tests: `tests/response_verification/`
- Verification: canonical targeted suite and Ruff checks green; ty unavailable
- Commit: pending

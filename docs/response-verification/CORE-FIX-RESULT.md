# RESPONSE-VERIFICATION-001 bounded core corrections

Task: `fleet-4c3a038fa1f54669a9f2d2acd4b9918a`.
Mack is sole implementation owner. Reviewed base:
`25d976b19f401931a8cb5ffef0e0e90c0edf7f22`.
Branch: `feature/mack/RESPONSE-VERIFICATION-001`; initially clean.
The final local commit SHA is returned in the bridge result; this report and all
listed evidence are part of that commit (`git log -1 --format=%H -- docs/response-verification/CORE-FIX-RESULT.md`).

## Result and scope

F1/F2 are corrected in the isolated core. This is an implementation self-report,
not independent acceptance or fleet enforcement. Fresh independent review of the
resulting commit is requested before any integration work. The earlier independent
REJECT remains the review of the base; this work does not supersede it by self-approval.

Changed paths are `response_verification/core.py`,
`tests/response_verification/test_core.py`, and this report plus four evidence files
under `docs/response-verification/`: `core-fix-red.log`, `core-fix-green.log`,
`core-fix-checks.json`, and `core-fix-old-probe.json`.
No adapter, provider, credentials, configuration, cron, dependencies, deployment,
shared/main checkout, or running service was changed. No push, merge, or handoff.
The canonical test runner writes its ignored local duration cache as usual.

## Corrections

- F1: a read-back earlier than its effect yields `read_back_precedes_effect` and
  cannot complete, even with individually fresh timestamps and a matching effect
  hash. Comparisons use UTC instants, including differently offset timestamps.
- Timestamp contract: timezone-aware Python datetimes are required; the canonical
  hash encodes UTC with six fractional digits (microseconds). The evaluator adds
  no rounding or skew allowance. Read-back time must be greater than or equal to
  effect time. Equality deliberately remains eligible at representable precision,
  preserving same-instant fixtures. Equality does not prove causality or distinguish
  events collapsed by a coarse collector clock. Collectors must establish actual
  post-effect observation through operation/version association, preserve available
  precision, and reconcile clocks; they must not fabricate ordering by relabeling
  cached pre-effect state. Finer-than-microsecond ordering and clock uncertainty
  remain admission-boundary requirements. Receipt age is inclusive at max age;
  one microsecond older is stale. Chronology still applies at that boundary.
- F2: eligibility requires the exact `EvidenceOutcome.VERIFIED` member. Genuine
  UNKNOWN effects retain `external_effect_unknown` and UNKNOWN status and also
  receive `evidence_not_verified`; FAILED remains INCOMPLETE. Neither status
  authorizes retry. Unknown outcomes require reconciliation, never automatic replay.
- Receipt admission checks exact origin/kind/outcome enum classes before indexing,
  hashing or grouping. Strings, foreign enums (including matching `verified`,
  `failed`, and `unknown` values), and foreign PENDING are rejected with
  `receipt_schema_invalid`. Criterion kinds also require the exact enum class.
  Hash helpers remain integrity helpers, not admission or authentication APIs.
- Bounded malformed-input checks now deny the review's dictionary criterion,
  string receipt timestamp, and None context identifier without crashing. Checks
  also validate criterion collection shape, trusted issuer collection shape,
  context time/age types, and receipt string-field types before unsafe operations.

## Reproduction and verification

1. New corrected regression assertions ran against the unchanged reviewed core:
   `HERMES_PYTHON=/opt/homebrew/bin/python3 scripts/run_tests.sh tests/response_verification/test_core.py --tb=short`
   exited 1 with **8 failed, 36 passed**. Failures were pre-effect read-back,
   four foreign effect outcomes, and the three reported malformed inputs. Full output: `core-fix-red.log`.
2. After correction, the same canonical command exited 0 with **57 passed, 0
   failed**, including added offset-zone chronology, enum-kind/origin, and inclusive
   age-boundary cases. Full output: `core-fix-green.log`. The initial 44 regression
   cases passed after correction before the additional boundary cases were added.
3. `ruff check --no-cache response_verification tests/response_verification` and
   `ruff format --check --no-cache response_verification tests/response_verification`
   both exited 0 (Ruff 0.15.0; three files formatted).
4. `git diff --check` exited 0. Final staged scope and whitespace were checked before
   committing locally. Machine-readable commands/results: `core-fix-checks.json`.
5. All three package/test Python files compiled in memory using `compile()` under
   `/opt/homebrew/bin/python3.14` (**3.14.7**) and
   `/Users/mfethe/.hermes/hermes-agent/venv/bin/python` (**3.11.15**), both exit 0.
   Exact `-B -c` commands and version output are retained in `core-fix-checks.json`.
   Compilation is syntax verification, not a Python 3.11 test-suite execution.
6. Applied the gstack review checklist locally to the bounded diff, focusing on
   enum consumers, trust boundaries, time ordering, and type coercion. This is not
   an independent reviewer or a fleet delivery check.

## Independent probe preservation and expected divergence

The independent report and `probe_core.py` under
`/Users/mfethe/.scratch/response-verification-001-review/` were not edited. Their
SHA-256 hashes before/after the probe run match and are retained in
`core-fix-old-probe.json`.

Running `/opt/homebrew/bin/python3 -B /Users/mfethe/.scratch/response-verification-001-review/probe_core.py`
now exits 1 at **age boundary 2**: that case puts the read-back ten minutes before
now while leaving the effect twenty seconds before now, so the corrected result
is INCOMPLETE with `read_back_precedes_effect`. The old script asserts COMPLETE
because it records the buggy base behavior. It stops there; this report does not
claim all 74 old observations ran after correction.

The later F1 and foreign F2 reproductions must also change from COMPLETE to denial;
the three malformed-input reproductions must change from exceptions to controlled
denial. Separate corrected assertions live in the repository test suite, preserving
the original independent reproduction script and expectations. Genuine enum
VERIFIED/FAILED/UNKNOWN and equal/after-effect positive cases remain covered.

## Remaining boundaries and review request

This typed Python core is still not a strict versioned wire decoder. A future
boundary must reject malformed JSON, unknown/missing fields and enum values,
construct exact enum/dataclass values, bound collection/string sizes, and own
context, clock and receipt provenance outside model input. Arbitrary top-level
objects, hostile Python subclasses/timezones, throwing or unbounded iterables,
and resource exhaustion are not fully normalized into core decisions. The sender
still needs an exception/timeout-safe buffered wrapper with no draft-byte leakage.

Issuer strings and hashes do not authenticate collectors or prove world state.
No durable replay/revocation policy, approval-generation binding, causal object
version proof, freshness recheck at delayed rendering, or gateway enforcement was
implemented. Callers must never deserialize a model-supplied CompletionDecision;
rendering trusts evaluator-owned decisions. No live external effect or broad repo
suite was exercised. No standalone typechecker was run; required targeted gates
were available and passed.

Local and ancestor AGENTS.md, fleet COORDINATION.md and federation charter were read.
The current task's explicit ownership and no-config-change limits govern. Agent-context
MCP tools are unavailable, and its CLI has a missing Python interpreter; recall and
check-action could not run. No installation or environment repair was attempted.

Requested next action: a fresh independent review of the returned exact local SHA,
with independent F1/F2 and malformed-input assertions, before considering the
separate buffered adapter integration slice. No rollout approval is requested here.

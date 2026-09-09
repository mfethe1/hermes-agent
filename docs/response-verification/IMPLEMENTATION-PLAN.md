# RESPONSE-VERIFICATION-001 implementation plan

Task: `fleet-08f810c9fe1145888585b2f21db8598a`
Owner: Mack, sole implementation owner
Base: Hermes `f03ed94a34f47ebca57e4a1b0a890bc2aeb5e140`
Release path: Path B, local branch and review-ready commits only

## Outcome

Build a versioned, fail-closed response-completion verifier and prove it on one real
Hermes gateway delivery boundary. The pilot must never release the unverified draft.
It must return a fixed qualification response when verification rejects, crashes, or
times out. It does not assert that arbitrary natural-language claims are true.

## Data flow

```text
model completion request (references only)
            |
            v
   isolated verifier process <--- trusted approval context
            |                  <--- collector-admitted receipts
       +----+----+
       |         |
     accept    reject/error/timeout
       |         |
       v         v
fixed completion fixed safe qualification
    status
       |         |
       +----+----+
            |
            v
      platform adapter send
```

## Phases

1. Inventory exact local and fleet runtime entry points, versions, active-session
   behavior, and instruction-only versus enforced coverage. Consult official docs for
   supported hooks and settings.
2. Add the v1 response contract, immutable acceptance-spec digest rules, evidence
   receipt schema, threat model, and coverage ledger.
3. Implement a standalone verifier with strict task, attempt, artifact, environment,
   requirement-digest, time, issuer, receipt-integrity, and read-back binding. Issuer
   authentication remains the independent collector boundary's responsibility.
4. Add adversarial tests for missing criteria, mismatched hashes, stale receipts,
   self-issued receipts, unknown external effects, verifier crash/timeout, changed
   requirements, and output outside the gate.
5. Formalize only the narrow completion-eligibility invariants in Lean. Build with no
   `sorry` or custom axioms and keep world-truth claims outside the theorem.
6. Wire the verified-or-qualified result into one real Hermes gateway response path in
   an isolated test deployment. Exercise accept, reject, crash, and timeout paths and
   retain the emitted artifacts.
7. Run targeted and repository gates, commit locally, and prepare a private
   review-ready patch. Do not activate, push, merge, restart gateways, or alter cron.

## Architecture decisions

- The acceptance spec is supplied by a trusted approval layer outside the model
  envelope and digest-bound; changing requirements invalidates all prior receipts.
  A digest echoed by a caller proves only equality, never approval authority.
- Receipt issuers are explicit trusted identities configured outside model output.
  A receipt whose issuer is the agent/model identity is rejected even if structurally
  valid.
- External mutations require a read-back receipt. An unknown outcome is never eligible
  for completion or automatic retry.
- Claim classification is explicit input. The verifier validates bindings and
  evidence; it does not infer truth from prose or regex-match words such as `done`.
- An empty or missing approved-criteria set can never authorize completion. The
  model cannot release arbitrary prose by declaring that it made no substantive
  claims; this core emits only controlled completion/status text from a trusted
  approval context and boundary-supplied evidence receipts.
- No streaming bytes are released before the full response is accepted. Streaming
  support therefore requires buffer-then-gate or an application-specific preflight;
  partial post-hoc filtering is unsafe.

## Test coverage

```text
verify(envelope)
  +-- schema invalid ---------------------------- reject
  +-- task/attempt/environment mismatch --------- reject
  +-- requirements/artifact digest mismatch ----- reject
  +-- missing/stale/untrusted/self receipt ------- reject
  +-- external effect without read-back ---------- hold_unknown
  +-- all criteria independently evidenced ------- accept
  +-- empty or missing approved criteria ---------- reject

gateway gate
  +-- verifier accepts --------------------------- deliver verified text
  +-- verifier rejects --------------------------- deliver fixed qualification
  +-- verifier crashes --------------------------- deliver fixed qualification
  +-- verifier times out -------------------------- deliver fixed qualification
  +-- draft attempts direct adapter send ---------- blocked by boundary test
```

## NOT in scope

- Live fleet activation, gateway restart, cron enablement, provider or credential
  changes, merge, deploy, and public content.
- Rosie/OpenClaw source or configuration mutations without Rosie-owner coordination.
- Retrofitting already-running sessions. Instruction/config changes affect only fresh
  sessions unless the owning runtime documents a safe reload path.
- Automatic semantic fact checking of arbitrary natural language.
- Same-UID host compromise resistance in the local pilot. Production signer isolation
  requires a separate service identity or hardware-backed key and independent operator.

## Rollback

The pilot is disabled by default and confined to this branch. Rollback is removal of
the gateway adapter call and verifier package before merge. No running process or user
configuration is changed by this task.

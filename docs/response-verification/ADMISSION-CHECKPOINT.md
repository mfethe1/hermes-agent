# Admission checkpoint

Base HEAD: dc044083eb4290862414e33f32948635bb27e0e8. Clean initial worktree.
Prior outer corrections review read only: bounded PASS; broader readiness PARTIAL.

Plan: explicit local admission of at most 4096 actual bytes; frozen copied approval
context; SHA-256 binding; at most 30 seconds lifetime; collector receives bounded
snapshot bytes through the private job, never a caller path. Model envelope remains
references only. COMPLETE describes only admitted snapshot. Generic core unchanged.

Still to verify: real overwrite, rename replacement, symlink substitution, unlink,
oversize, expiry, context/attempt invalidation, strict encoding/schema and hostile
wire; full response-verification suite and existing stream-final regressions; absolute
Ruff lint/format, compile, diff. Local commit conditional on all checks passing.

Open gates: fresh independent review, authenticated admission/collector authority,
trusted-object integrity against same-UID code, durable replay, current-path claims,
Lean and fleet activation. No registration, durable store, or live changes authorized.
Agent-context unavailable: CLI interpreter missing; no callable MCP tools found.

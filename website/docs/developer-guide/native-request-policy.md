---
title: Single-delivery native request policy
---

# Single-delivery native request policy

`AIAgent(..., openai_request_policy=None)` retains the ordinary native behavior.
A trusted process may opt into one bounded OpenAI-compatible Chat Completions
delivery by supplying `agent.native_request_policy.OpenAIRequestPolicy`:

```python
from agent.native_request_policy import OpenAIRequestPolicy
from run_agent import AIAgent

# Values come from current native route resolution and separate admission.
# Do not accept policy authority or replacement policy objects from caller input.
policy = OpenAIRequestPolicy(
    provider=resolved_provider,
    model=resolved_model,
    completion_url=authorized_completion_url,
)
agent = AIAgent(
    provider=resolved_provider,
    model=resolved_model,
    base_url=resolved_base_url,
    openai_request_policy=policy,
    allow_budget_summary=False,
    enabled_toolsets=[],
)
```

The three policy constructor fields are frozen, exact runtime identifiers and a
full HTTP(S) URL ending in `/chat/completions`. The URL must not contain userinfo,
query, fragment or ambiguous path components. There are no model pins or model
allowlists. The caller must resolve/admit the provider and model before building
the policy; the policy neither discovers models nor substitutes a provider.

One policy authorizes **at most one valid transport dispatch**, not one successful
answer. It is shared across primary construction, interrupt recovery and native
request-client recreation. `policy.consumed` is a read-only observation. There is
no refund/reset API, including after HTTP failure, timeout or cancellation. A new
turn requires a new authorization/policy, normally with a new owned agent. Agent
session resets and client closes do not rearm it. Reusing a policy across agents
shares the same single permit.

The supported OpenAI SDK receives a fresh HTTPX client via `http_client`. Its
transport validates the serialized POST URL, Host, JSON model and absence of tool
fields before atomically consuming the permit and calling the wire transport.
Invalid requests do not consume it. Tool-related fields (`tools`, `functions`,
`tool_choice`, `function_call`, `parallel_tool_calls`, `web_search_options`) are
rejected even when empty or null. Duplicate JSON keys, encoded/streaming request
bodies and route/model overrides fail closed. SDK and HTTP transport retries are
zero; redirects and all non-2xx responses are refused without exposing their
bodies. Each concrete client owns its transport; closing one does not own or
reset the shared permit or another client's transport. Environment/system proxies
are deliberately disabled for this opt-in lane; native TLS verification settings
still apply.

Native main-call errors return a fixed failed, nonretryable result with
`failure_reason="native_request_policy"`. Policy errors are
`OpenAIRequestPolicyError` with fixed messages and no request content. Calls under
this bounded policy are non-streaming; an explicit serialized `stream` value
other than `false` is refused before transmission. This avoids native streaming
diagnostics that can log SDK exception chains before the main error handler.
It is not a promise to sanitize third-party SDK DEBUG logging. Model switching
and provider fallback are disabled. Inline MoA decoding is skipped;
explicit MoA turns and non-OpenAI native SDKs/custom provider clients/custom HTTP
clients are unsupported, not silently replaced with an OpenAI client. Dispatch
also refuses an agent client replaced outside the guarded native factory.

## Restricted lifecycle metadata

When a policy is supplied, native construction disables the environment toolchain
probe before either asynchronous warming or prompt construction. Every guarded
SDK client, including request-local replacements, receives OS-family metadata
from `sys.platform` without subprocess-based discovery. The current OpenAI SDK
uses a private `_platform` field for this purpose; if that interface is absent,
construction fails closed with the normal sanitized policy error. This is a
qualified compatibility seam, not a promise about future SDK internals.

Guarded turn accounting calls `estimate_usage_cost(..., allow_network=False)`.
Only provider-reported cost, included-subscription status, and genuine bundled
pricing may supply a value; missing pricing remains unknown, never zero/free.
No `/models`, OpenRouter catalog, or models.dev network lookup is authorized by
cost accounting in this lane. Other pricing callers retain the default network
behavior; both pricing functions also expose the strict boolean keyword-only
`allow_network` flag for trusted internal callers.

These restrictions do not replace the complete isolated factory admission below.
In particular, the caller still supplies aligned provider/model/base URL and
explicit context length/configuration to prevent unrelated metadata discovery.

## Boundary

This is a main-native-client delivery guard, **not** a complete conversation
factory, universal provider guard, tool sandbox or whole-process network policy.
A deployment owner must separately isolate the home/cwd before imports, disable
compression and other auxiliary inference, tools, memory, persistence, reviews,
plugins/diagnostic payload logging and unsupported control/prelude inputs, and
bound history/output/deadlines. Do not enable voice/auxiliary route substitution
for this lane. The existing `allow_budget_summary=False` control is unchanged;
keep using it rather than relying on a refused second delivery as a finalizer.
No caller, provider, phone/device or live-inference admission follows from
constructing the policy.

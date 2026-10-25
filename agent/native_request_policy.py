"""Opt-in, process-owned, single-delivery policy for native Chat Completions.

This is not a provider resolver or a sandbox. The caller admits a current native
OpenAI-compatible route, then creates ONE policy per turn. All clients for that
turn share the policy, never their disposable HTTP clients. There is no reset or
refund API; creating another policy is a new authorization by the trusted owner.
"""

from __future__ import annotations

import json
import threading
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import httpx


class OpenAIRequestPolicyError(RuntimeError):
    """Local refusal; policy-generated messages never include request data."""


class _Permit:
    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.consumed = False

    def consume(self) -> None:
        with self.lock:
            if self.consumed:
                raise OpenAIRequestPolicyError("Native request permit already consumed")
            self.consumed = True


def _endpoint(value: str) -> httpx.URL:
    """Reject ambiguous URLs rather than repairing an authorization token."""
    try:
        if not isinstance(value, str) or not value or any(c.isspace() or ord(c) < 32 or ord(c) == 127 for c in value):
            raise ValueError
        parsed = urlsplit(value)
        if (
            parsed.scheme not in {"http", "https"} or not parsed.netloc
            or "@" in parsed.netloc or "?" in value or "#" in value
            or "\\" in value or "%" in parsed.path or "//" in parsed.path
            or any(p in {".", ".."} for p in parsed.path.split("/"))
        ):
            raise ValueError
        url = httpx.URL(value)
        if not url.host or url.username or url.password:
            raise ValueError
        return url
    except Exception:
        raise OpenAIRequestPolicyError("Invalid native completion URL") from None


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError
        result[key] = value
    return result


def _invalid_constant(value: str) -> None:
    raise ValueError


@dataclass(frozen=True, eq=False, repr=False)
class OpenAIRequestPolicy:
    """Frozen runtime identity plus a thread-safe, consumed-once delivery permit.

    ``provider`` and ``model`` are exact, already-resolved runtime identifiers.
    ``completion_url`` is the full authorized URL ending in /chat/completions,
    without credentials, query or fragment. HTTP is supported for an admitted
    local endpoint; remote-route security/admission remains the caller's job.
    """

    provider: str
    model: str
    completion_url: str
    _permit: _Permit = field(default_factory=_Permit, init=False, repr=False)
    _url: httpx.URL = field(init=False, repr=False)

    def __post_init__(self) -> None:
        for value in (self.provider, self.model):
            if (
                not isinstance(value, str) or not value or value != value.strip()
                or any(ord(c) < 32 or ord(c) == 127 for c in value)
            ):
                raise OpenAIRequestPolicyError("Invalid native runtime identity")
        url = _endpoint(self.completion_url)
        if not url.raw_path.endswith(b"/chat/completions"):
            raise OpenAIRequestPolicyError("Invalid native completion path")
        object.__setattr__(self, "_url", url)

    @property
    def consumed(self) -> bool:
        """Observation only; never use this property as an authorization check."""
        with self._permit.lock:
            return self._permit.consumed

    def validate_runtime(self, *, provider: str, model: str, api_mode: str) -> None:
        if provider != self.provider or model != self.model or api_mode != "chat_completions":
            raise OpenAIRequestPolicyError("Native request runtime mismatch")

    def validate_client(self, client: Any) -> None:
        # Route binders can replace agent.client without using the primary
        # factory. Even an identical route must not substitute an unguarded SDK.
        if getattr(client, "_hermes_native_request_policy", None) is not self:
            raise OpenAIRequestPolicyError("Native request requires a policy-bound client")

    def validate_client_kwargs(self, client_kwargs: dict) -> None:
        base = client_kwargs.get("base_url")
        if not isinstance(base, str):
            raise OpenAIRequestPolicyError("Native request requires an explicit base URL")
        url = _endpoint(base.rstrip("/") + "/chat/completions")
        if url != self._url or client_kwargs.get("default_query"):
            raise OpenAIRequestPolicyError("Native request route mismatch")
        if "http_client" in client_kwargs:
            # A foreign client may mount an unguarded route or retry internally.
            raise OpenAIRequestPolicyError("Custom HTTP clients are not supported by native request policy")

    def _validate_request(self, request: httpx.Request) -> None:
        if (
            request.method != "POST" or request.url != self._url
            or request.url.raw_path != self._url.raw_path
            or request.headers.get("host") != self._url.netloc.decode("ascii")
        ):
            raise OpenAIRequestPolicyError("Native request route mismatch")
        if (
            request.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json"
            or request.headers.get("content-encoding", "identity").lower() != "identity"
        ):
            raise OpenAIRequestPolicyError("Native request requires unencoded JSON")
        try:
            # Native SDK JSON is buffered. Do not consume a foreign streaming body.
            body = json.loads(
                request.content.decode("utf-8"), object_pairs_hook=_unique_object,
                parse_constant=_invalid_constant,
            )
        except Exception:
            raise OpenAIRequestPolicyError("Invalid native request JSON") from None
        if not isinstance(body, dict) or body.get("model") != self.model:
            raise OpenAIRequestPolicyError("Native request model mismatch")
        if "stream" in body and body["stream"] is not False:
            raise OpenAIRequestPolicyError("Streaming is not supported by native request policy")
        if any(key in body for key in (
            "tools", "functions", "tool_choice", "function_call", "parallel_tool_calls", "web_search_options",
        )):
            raise OpenAIRequestPolicyError("Tool declarations are forbidden by native request policy")

    def build_http_client(self, *, verify: Any = True) -> httpx.Client:
        """Fresh owned HTTPX resources; only the authorization state is shared.

        No environment/system proxies, mounts, redirects, authentication replay or
        transport retries. TLS verification is the native resolver's chosen value.
        The OpenAI SDK receives this via its supported ``http_client`` parameter.
        """
        inner = None
        try:
            inner = httpx.HTTPTransport(verify=verify, retries=0, trust_env=False)
            return httpx.Client(
                transport=_PolicyTransport(self, inner), follow_redirects=False, trust_env=False,
            )
        except Exception:
            if inner is not None:
                with suppress(Exception):
                    inner.close()
            raise OpenAIRequestPolicyError("Native guarded HTTP client creation failed") from None


class _PolicyTransport(httpx.BaseTransport):
    def __init__(self, policy: OpenAIRequestPolicy, inner: httpx.BaseTransport) -> None:
        self._policy = policy
        self._inner = inner

    @property
    def _pool(self):
        # Preserve native socket-abort discovery for this client's owned pool.
        # Only the consumed permit is shared; HTTP transports remain client-local.
        return getattr(self._inner, "_pool", None)

    def handle_request(self, request: httpx.Request) -> httpx.Response:
        self._policy._validate_request(request)
        # Last operation before handing the request to the wire transport. Never
        # undo this transition, including cancellation, timeout and HTTP failure.
        self._policy._permit.consume()
        try:
            response = self._inner.handle_request(request)
        except Exception:
            raise OpenAIRequestPolicyError("Native request transmission failed; permit remains consumed") from None
        if not 200 <= response.status_code < 300:
            # Never give the SDK a redirect target or provider error body to log,
            # classify for repair, or use for retry/fallback. Do not read the body.
            try:
                response.close()
            finally:
                raise OpenAIRequestPolicyError("Native request HTTP failure; permit remains consumed") from None
        return response

    def close(self) -> None:
        try:
            self._inner.close()
        except Exception:
            raise OpenAIRequestPolicyError("Native guarded HTTP client close failed") from None

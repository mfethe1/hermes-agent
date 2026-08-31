from __future__ import annotations

import pytest

from acp_adapter import server
from acp_adapter.server import HermesACPAgent


@pytest.mark.asyncio
async def test_initialize_attests_selected_hermes_profile(monkeypatch):
    monkeypatch.setattr(server, "_selected_profile_name", lambda: "jake")

    response = await HermesACPAgent().initialize()

    assert response.agent_info is not None
    assert response.agent_info.name == "hermes-agent"
    assert response.agent_info.field_meta == {
        "hermes": {
            "profile": "jake",
        }
    }


@pytest.mark.asyncio
async def test_initialize_uses_default_profile_when_no_selector(monkeypatch):
    monkeypatch.setattr(server, "_selected_profile_name", lambda: "default")

    response = await HermesACPAgent().initialize()

    assert response.agent_info is not None
    assert response.agent_info.field_meta == {
        "hermes": {
            "profile": "default",
        }
    }

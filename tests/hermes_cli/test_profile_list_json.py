from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from hermes_cli.profiles import ProfileInfo, profile_info_to_public_json


def test_profile_json_is_machine_stable_and_omits_local_paths():
    profile = ProfileInfo(
        name="jake",
        path=Path("/Users/example/.hermes/profiles/jake"),
        is_default=False,
        gateway_running=False,
        model="x-ai/grok-4.6",
        provider="openrouter",
        has_env=True,
        skill_count=124,
        alias_path=Path("/Users/example/.local/bin/jake"),
        alias_name="jake",
        description="Implementation agent",
        display_name="Jake",
    )

    row = profile_info_to_public_json(profile, active_profile="default")

    assert row == {
        "name": "jake",
        "display_name": "Jake",
        "description": "Implementation agent",
        "description_auto": False,
        "is_default": False,
        "active": False,
        "model": "x-ai/grok-4.6",
        "provider": "openrouter",
        "gateway_running": False,
        "alias": "jake",
        "distribution": None,
    }
    rendered = repr(row)
    assert "/Users/example" not in rendered
    assert "path" not in row
    assert "has_env" not in row
    assert "skill_count" not in row


def test_profile_json_marks_default_active_without_alias_or_distribution():
    profile = ProfileInfo(
        name="default",
        path=Path("/Users/example/.hermes"),
        is_default=True,
        gateway_running=True,
        model=None,
        provider=None,
    )

    row = profile_info_to_public_json(profile, active_profile="default")

    assert row["active"] is True
    assert row["alias"] is None
    assert row["distribution"] is None


def test_cmd_profile_list_json_emits_versioned_envelope(monkeypatch, capsys):
    from hermes_cli import main as main_mod
    from hermes_cli import profiles as profiles_mod

    profile = ProfileInfo(
        name="jake",
        path=Path("/private/profile/path"),
        is_default=False,
        gateway_running=False,
        model="x-ai/grok-4.6",
        provider="openrouter",
    )
    monkeypatch.setattr(profiles_mod, "list_profiles", lambda: [profile])
    monkeypatch.setattr(profiles_mod, "get_active_profile_name", lambda: "jake")

    main_mod.cmd_profile(SimpleNamespace(profile_action="list", json=True))

    payload = json.loads(capsys.readouterr().out)
    assert payload["schema"] == "hermes-profile-list/v1"
    assert payload["active_profile"] == "jake"
    assert payload["profiles"][0]["active"] is True
    assert payload["profiles"][0]["name"] == "jake"
    assert "/private/profile/path" not in json.dumps(payload)

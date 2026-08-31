from __future__ import annotations

import os

from acp_adapter import entry


def test_acp_entry_accepts_profile_selector():
    args = entry._parse_args(["--profile", "jake", "--check"])

    assert args.profile == "jake"
    assert args.check is True


def test_apply_profile_sets_profile_home_before_runtime_boot(monkeypatch):
    from hermes_cli import profiles as profiles_mod

    monkeypatch.delenv("HERMES_HOME", raising=False)
    monkeypatch.delenv("HERMES_PROFILE_NAME", raising=False)
    monkeypatch.setattr(
        profiles_mod,
        "resolve_profile_env",
        lambda name: f"/profiles/{name}",
    )

    entry._apply_profile("jake")

    assert os.environ["HERMES_HOME"] == "/profiles/jake"
    assert os.environ["HERMES_PROFILE_NAME"] == "jake"


def test_acp_check_applies_profile_before_import_check(monkeypatch):
    observed = {}

    def apply(name):
        observed["profile"] = name

    def check():
        observed["checked_after"] = observed.get("profile")

    monkeypatch.setattr(entry, "_apply_profile", apply)
    monkeypatch.setattr(entry, "_run_check", check)

    entry.main(["--profile", "jake", "--check"])

    assert observed == {"profile": "jake", "checked_after": "jake"}

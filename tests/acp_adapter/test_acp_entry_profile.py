from __future__ import annotations

import os

import pytest

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


def test_profile_busy_fails_before_acp_runtime_boot(monkeypatch):
    from acp_adapter import profile_lock

    monkeypatch.setattr(entry, "_apply_profile", lambda _name: None)
    monkeypatch.setattr(
        profile_lock,
        "acquire_profile_runtime_lease",
        lambda _name: (_ for _ in ()).throw(
            profile_lock.ProfileRuntimeBusy("profile already active")
        ),
    )
    monkeypatch.setattr(
        entry,
        "_setup_logging",
        lambda: pytest.fail("runtime boot must not start while profile is busy"),
    )

    with pytest.raises(SystemExit) as exc:
        entry.main(["--profile", "jake"])

    assert exc.value.code == 3

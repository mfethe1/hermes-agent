"""Regression: doctor repair paths must preserve malformed/undecodable user bytes.

OpenClaw v2026.10.3 acceptance class: ``hermes doctor --fix`` must never drop
OAuth/credential profiles, never rewrite memory/persona files it cannot decode,
never crash on invalid UTF-8, and must keep a byte-exact backup before any
state.db surgery. Each test seeds a fake HERMES_HOME (tmp_path — never the real
~/.hermes), runs the real doctor check with ``should_fix=True``, and asserts
byte-for-byte preservation (sha256) or a byte-exact backup.
"""

from __future__ import annotations

import hashlib
import sqlite3
from pathlib import Path

import pytest

import hermes_state_repair
from hermes_cli import doctor as doctor_mod
from hermes_cli import doctor_config, doctor_state
from hermes_cli.doctor_report import Finding

# Invalid UTF-8: \xa7\xff\xfe is not decodable as UTF-8 (strict or utf-8-sig).
BAD_SOUL = b"# persona\n\xa7\xff\xfe raw bytes\n"
BAD_MEMORY = b"entry one\n\xa7\xff\xfe raw"
BAD_USER = b"user fact\n\xc3\x28 broken"
BAD_ENV = b"OPENAI_API_KEY=sk-test\n# caf\xe9 note (cp1252)\n"
BAD_CONFIG = b"model:\n  provider: openrouter\n# \xa7\xff\xfe trailing garbage\n"
AUTH_BYTES = b'{"token": "oauth-refresh-token-do-not-drop", "expires": 9999999999}'


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture
def fake_home(tmp_path, monkeypatch):
    """A HERMES_HOME seeded with malformed user data; doctor pointed at it."""
    home = tmp_path / "hermes-home"
    home.mkdir()
    (home / "SOUL.md").write_bytes(BAD_SOUL)
    (home / "memories").mkdir()
    (home / "memories" / "MEMORY.md").write_bytes(BAD_MEMORY)
    (home / "memories" / "USER.md").write_bytes(BAD_USER)
    (home / ".env").write_bytes(BAD_ENV)
    (home / "config.yaml").write_bytes(BAD_CONFIG)
    (home / "auth.json").write_bytes(AUTH_BYTES)  # OAuth/credential profile stand-in
    monkeypatch.setattr(doctor_mod, "HERMES_HOME", home)
    monkeypatch.setattr(doctor_mod, "_DHH", str(home))
    return home


def test_fix_preserves_undecodable_soul_md(fake_home):
    """SOUL.md with invalid UTF-8: doctor --fix must not crash, overwrite, or lossy-decode it."""
    finding = doctor_state._check_directory_structure(True)  # decorated: (should_fix) -> Finding
    assert isinstance(finding, Finding), "doctor check crashed instead of returning a Finding"
    assert (fake_home / "SOUL.md").read_bytes() == BAD_SOUL


def test_fix_preserves_undecodable_memory_files(fake_home):
    """MEMORY.md/USER.md with invalid UTF-8: same strict-refusal rule as
    tools/memory_tool_store._read_raw_checked — surface unreadable, never rewrite."""
    finding = doctor_state._check_directory_structure(True)
    assert isinstance(finding, Finding), "doctor check crashed instead of returning a Finding"
    assert (fake_home / "memories" / "MEMORY.md").read_bytes() == BAD_MEMORY
    assert (fake_home / "memories" / "USER.md").read_bytes() == BAD_USER


def test_fix_preserves_non_utf8_env_file(fake_home):
    """.env with cp1252 bytes: read-only for doctor (latin-1 fallback), never rewritten."""
    doctor_config._check_env_file(True)
    assert (fake_home / ".env").read_bytes() == BAD_ENV


def test_fix_preserves_undecodable_config_yaml(fake_home):
    """config.yaml with invalid UTF-8: validation degrades to a warning; the file is untouched."""
    doctor_config._check_config_file(True)
    assert (fake_home / "config.yaml").read_bytes() == BAD_CONFIG


def test_fix_preserves_auth_profile_bytes(fake_home):
    """No doctor repair path writes credential/auth files; bytes survive a full --fix sweep
    of the checks that touch HERMES_HOME state."""
    doctor_state._check_directory_structure(True)
    doctor_config._check_env_file(True)
    doctor_config._check_config_file(True)
    assert (fake_home / "auth.json").read_bytes() == AUTH_BYTES


def _make_db(path: Path) -> None:
    conn = sqlite3.connect(str(path), isolation_level=None)
    conn.execute("CREATE TABLE sessions (name TEXT)")
    conn.execute("INSERT INTO sessions VALUES ('seed')")
    conn.commit()
    conn.close()


def test_state_db_repair_keeps_byte_exact_backup(tmp_path, monkeypatch):
    """repair_state_db_schema (the doctor --fix state.db path) must snapshot the raw DB bytes
    before surgery and leave the repaired DB still holding the user's rows."""
    db = tmp_path / "state.db"
    _make_db(db)
    original_sha = _sha256(db)
    # Force the repair path on a well-formed DB: repair strategies then run for real.
    monkeypatch.setattr(hermes_state_repair, "_db_opens_cleanly", lambda path: "forced-unhealthy")
    monkeypatch.setattr(hermes_state_repair, "_repair_scratch_space_error", lambda path: None)
    # Holder probing inspects processes/logs outside the tmp home; pin it so the test is
    # deterministic and never consults real machine state.
    monkeypatch.setattr(hermes_state_repair, "_live_writer_holds_db", lambda path: False)

    report = hermes_state_repair.repair_state_db_schema(db)

    backup_path = report.get("backup_path")
    assert backup_path is not None, f"repair produced no backup: {report}"
    assert Path(backup_path).exists(), f"backup missing: {report}"
    assert _sha256(Path(backup_path)) == original_sha, "backup is not byte-identical to the original DB"
    conn = sqlite3.connect(str(db))
    try:
        rows = conn.execute("SELECT name FROM sessions").fetchall()
    finally:
        conn.close()
    assert rows == [("seed",)], "user rows did not survive repair"

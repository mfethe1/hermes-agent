"""Record process spawns instead of running them, below every test-side subprocess guard."""
import os
import subprocess


def record_spawns(monkeypatch) -> list:
    """Turn every process launch into a recorded ``FileNotFoundError``: its argv lands in the
    returned list and nothing executes. Patched where the launch really happens
    (``Popen._execute_child`` and the ``os`` exec/spawn calls), so it holds beneath the
    live-system guard's wrappers, without the guard, and for a binary named by absolute path."""
    spawned: list = []

    def refuse(argv):
        spawned.append([os.fsdecode(part) for part in argv])
        raise FileNotFoundError(2, "spawn recorded by the test, not executed", os.fsdecode(argv[0]))

    base = next(cls for cls in subprocess.Popen.__mro__ if "_execute_child" in vars(cls))
    monkeypatch.setattr(base, "_execute_child", lambda self, args, *a, **k: refuse(list(args)))
    for name in ("execv", "execve", "execvp", "execvpe", "posix_spawn", "posix_spawnp"):
        if hasattr(os, name):
            monkeypatch.setattr(os, name, lambda path, argv=(), *a, **k: refuse(list(argv) or [path]))
    return spawned

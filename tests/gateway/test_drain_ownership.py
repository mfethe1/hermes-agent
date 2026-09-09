"""Real filesystem and process arbitration; no live profile or services."""
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
from types import SimpleNamespace

import pytest

from gateway import drain_control as dc


def test_owned_lifecycle_and_failures(tmp_path, monkeypatch):
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    first = dc.create_drain_request(principal='controller', suppress_notification=True)
    token = first['request_id']
    assert dc.create_drain_request() is None
    refreshed = dc.refresh_drain_request(request_id=token)
    assert refreshed['request_id'] == token
    assert refreshed['requested_at'] >= first['requested_at']
    assert dc.drain_notification_suppressed()
    replacement = dc.write_drain_request(principal='operator')
    assert replacement['request_id'] != token
    assert dc.refresh_drain_request(request_id=token) is None
    assert not dc.clear_drain_request(request_id=token)
    assert dc.read_drain_request() == replacement
    for invalid in ('', None, 12):
        with pytest.raises(ValueError):
            dc.clear_drain_request(request_id=invalid)
    # Publication failure cannot corrupt or erase the previous marker.
    with monkeypatch.context() as m:
        def interrupted(*args):
            raise KeyboardInterrupt()
        m.setattr(dc.os, 'replace', interrupted)
        with pytest.raises(KeyboardInterrupt):
            dc.write_drain_request()
    assert dc.read_drain_request() == replacement
    assert not list(tmp_path.glob('.drain-*.tmp'))
    assert dc.clear_drain_request(request_id=replacement['request_id'])
    assert not dc.clear_drain_request(request_id=token)
    path = dc.drain_request_path()
    for raw in (b'{broken', b'\xff', b'[]', json.dumps({'request_id': token}).encode(), json.dumps({'requested_at': '2000-01-01T00:00:00Z', 'request_id': 'old'}).encode()):
        path.write_bytes(raw)
        assert dc.create_drain_request() is None
        assert dc.refresh_drain_request(request_id=token) is None
        assert not dc.clear_drain_request(request_id=token)
        assert path.read_bytes() == raw
        assert dc.clear_drain_request()  # explicit operator override
    # Exercise the actual dashboard handler with real marker I/O: operator
    # replacement/cancellation deliberately supersede controller ownership.
    from hermes_cli.web_routers.actions import gateway_drain
    class Request:
        state = SimpleNamespace(token_principal=None)
        def __init__(self, action):
            self.action = action
        async def json(self):
            return {'action': self.action}
    owned = dc.create_drain_request()
    response = asyncio.run(gateway_drain(Request('drain')))
    assert response['ok'] and dc.drain_requested()
    assert not dc.clear_drain_request(request_id=owned['request_id'])
    response = asyncio.run(gateway_drain(Request('cancel')))
    assert response['ok'] and response['was_draining'] and not dc.drain_requested()
    path.write_text('{}')
    with monkeypatch.context() as m:
        def unreadable(*args, **kwargs):
            raise PermissionError('test read failure')
        m.setattr(Path, 'read_text', unreadable)
        assert dc.drain_requested()
        assert dc.create_drain_request() is None
        assert not dc.clear_drain_request(request_id=token)
    path.unlink()
    path.symlink_to(tmp_path / 'missing')
    with pytest.raises(OSError):
        dc.write_drain_request()
    assert path.is_symlink()
    path.unlink()
    lock = path.with_name(path.name + '.lock')
    lock.unlink()  # fixture only: no concurrent users
    lock.mkdir()
    with pytest.raises(OSError):
        dc.write_drain_request()
    assert not path.exists()


def test_subprocess_mutations_share_lock_and_interrupted_writer(tmp_path, monkeypatch):
    monkeypatch.setenv('HERMES_HOME', str(tmp_path))
    env = dict(os.environ, PYTHONPATH=str(Path.cwd()), PYTHONDONTWRITEBYTECODE='1')
    first = dc.create_drain_request()
    token = first['request_id']
    # Pause a real publisher just before replacement while it owns the lock.
    script = '''
from gateway import drain_control as dc
import sys
original = dc.os.replace
def paused(*args):
    print('LOCKED', flush=True)
    sys.stdin.readline()
    original(*args)
dc.os.replace = paused
dc.write_drain_request(principal='replacement')
'''
    def start_writer():
        p = subprocess.Popen([sys.executable, '-c', script], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
        assert p.stdout.readline().strip() == 'LOCKED'
        return p
    writer = start_writer()
    try:
        # All public mutation paths contend against another OS process.
        operations = ['dc.write_drain_request()', 'dc.create_drain_request()',
                      f'dc.refresh_drain_request(request_id={token!r})',
                      f'dc.clear_drain_request(request_id={token!r})', 'dc.clear_drain_request()']
        for operation in operations:
            code = 'from gateway import drain_control as dc\ntry:\n    '+operation+'\nexcept OSError:\n    print("CONTENDED")\nelse:\n    raise AssertionError("mutation bypassed lock")'
            result = subprocess.run([sys.executable, '-c', code], capture_output=True, text=True, env=env, timeout=10)
            assert result.returncode == 0, result.stderr
            assert result.stdout.strip() == 'CONTENDED'
            assert dc.read_drain_request() == first
        writer.communicate('\n', timeout=10)
        assert writer.returncode == 0
    finally:
        if writer.poll() is None:
            writer.kill()
            writer.communicate(timeout=10)
    replacement = dc.read_drain_request()
    assert replacement['request_id'] != token
    assert not dc.clear_drain_request(request_id=token)
    # Pause conditional clear after its owner read. A replacement cannot sneak
    # between that read and unlink because it must acquire the same lock.
    clear_script = """
from gateway import drain_control as dc
import sys
original = dc.read_drain_request
def paused(**kwargs):
    body = original(**kwargs)
    print('CHECKED', flush=True)
    sys.stdin.readline()
    return body
dc.read_drain_request = paused
dc.clear_drain_request(request_id=sys.argv[1])
"""
    clearer = subprocess.Popen([sys.executable, '-c', clear_script, replacement['request_id']], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env)
    try:
        assert clearer.stdout.readline().strip() == 'CHECKED'
        with pytest.raises(OSError):
            dc.write_drain_request(principal='racing-replacement')
        clearer.communicate('\n', timeout=10)
        assert clearer.returncode == 0
    finally:
        if clearer.poll() is None:
            clearer.kill()
            clearer.communicate(timeout=10)
    replacement = dc.write_drain_request(principal='after-clear')
    assert not dc.clear_drain_request(request_id=token)
    # Abrupt death while publishing releases the persistent lock, preserving old bytes.
    writer = start_writer()
    writer.kill()
    writer.communicate(timeout=10)
    assert dc.read_drain_request() == replacement
    assert dc.refresh_drain_request(request_id=replacement['request_id'])
    assert dc.clear_drain_request(request_id=replacement['request_id'])
    # Simultaneous create attempts have exactly one owner (losers refuse or contend).
    code = '''
from gateway import drain_control as dc
import sys
sys.stdin.readline()
try:
    body = dc.create_drain_request()
    print('WON' if body else 'REFUSED')
except OSError:
    print('CONTENDED')
'''
    children = [subprocess.Popen([sys.executable, '-c', code], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, env=env) for _ in range(6)]
    try:
        for p in children:
            p.stdin.write('\n')
            p.stdin.flush()
        outcomes = [p.communicate(timeout=10) for p in children]
        assert all(p.returncode == 0 for p in children), outcomes
        assert sum(out.strip() == 'WON' for out, err in outcomes) == 1
        assert dc.drain_requested()
    finally:
        for p in children:
            if p.poll() is None:
                p.kill()
                p.communicate(timeout=10)

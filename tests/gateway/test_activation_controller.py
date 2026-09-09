"""Real marker/journal/process I/O; supervisor/work authority is simulated."""
import json
import select
import signal
import subprocess
import sys
import time

import pytest

from gateway import drain_control
from gateway.activation_controller import ActivationAdapter, Blocked, main
from gateway.safe_stop import StopObservation

pytestmark = pytest.mark.macos_only


@pytest.mark.parametrize('lost_ack', [False, 'before', 'after'])
def test_durable_stop_recovery_never_replays(tmp_path, lost_ack):
    home = tmp_path / 'home'
    home.mkdir()
    receipt = tmp_path / 'receipt.json'
    child = subprocess.Popen(
        [sys.executable, '-u', '-c',
         "import time; print('ready', flush=True); time.sleep(60)"], stdout=subprocess.PIPE,
    )
    try:
        assert select.select([child.stdout], [], [], 5)[0]
        assert child.stdout.readline() == b'ready\n'

        class Boundary:
            requests = 0

            def observe(self, *, deadline):
                marker = drain_control.read_drain_request(home=home)
                exited = child.poll() is not None
                return StopObservation(marker['request_id'], None if exited else str(child.pid),
                                       time.monotonic(), True, True, 0, 0, 0, 0, exited)

            def request_term(self, *, owner, identity, deadline, max_age):
                saved = json.loads(receipt.read_text())
                assert saved['phase'] == 'stop_entered' and saved['owner'] == owner
                assert saved['identity'] == identity == str(child.pid)
                self.requests += 1
                if lost_ack == 'before':
                    raise OSError('delivery unknown; no signal in this fixture')
                child.send_signal(signal.SIGTERM)
                if lost_ack:
                    raise OSError('ack lost after delivery')
                return True

        boundary = Boundary()
        adapter = ActivationAdapter(home=home, receipt=receipt, boundary=boundary)
        result = adapter.stop(identity=str(child.pid), timeout=3)
        assert result['result']['state'] == ('unknown' if lost_ack else 'stopped')
        if lost_ack != 'before':
            child.wait(timeout=5)
        else:
            assert child.poll() is None
        # New controller instance loads the durable attempt and observes only.
        recovered = ActivationAdapter(home=home, receipt=receipt, boundary=boundary)
        result = recovered.stop(identity='caller must not replace journal identity', timeout=3)
        assert result['reconciliation']['state'] == ('unknown' if lost_ack == 'before' else 'stopped')
        assert result['install_enabled'] is False and boundary.requests == 1
        owner = result['owner']
        recovered.refresh(owner)
        assert recovered.marker_owner() == owner
        # Operator replacement wins; refresh/clear never remove the foreign owner.
        foreign = drain_control.write_drain_request(home=home)
        with pytest.raises(Blocked):
            recovered.refresh(owner)
        with pytest.raises(Blocked):
            recovered.clear(owner)
        assert recovered.reconcile()['reconciliation']['state'] == 'unknown'
        assert recovered.marker_owner() == foreign['request_id']
        assert boundary.requests == 1
    finally:
        if child.poll() is None:
            child.kill()  # owned disposable test child only
        child.wait(timeout=5)
        child.stdout.close()


@pytest.mark.parametrize('mode', ['--preflight', '--dry-run', '--apply'])
def test_native_entrypoint_refuses_without_mutation(tmp_path, capsys, mode):
    plist = tmp_path / 'missing.plist'
    before = list(tmp_path.iterdir())
    assert main([mode, '--plist', str(plist)]) == 2
    result = json.loads(capsys.readouterr().out)
    assert result['ready'] is False and result['install_enabled'] is False
    assert any('atomic identity' in x for x in result['missing_capabilities'])
    assert list(tmp_path.iterdir()) == before


def test_unknown_marker_ack_and_foreign_marker_do_not_replay(tmp_path):
    home = tmp_path / 'home'
    home.mkdir()
    receipt = tmp_path / 'receipt.json'
    foreign = drain_control.write_drain_request(home=home)
    adapter = ActivationAdapter(home=home, receipt=receipt, boundary=None)
    with pytest.raises(Blocked):
        adapter.stop(identity='owned')
    assert json.loads(receipt.read_text())['phase'] == 'creating_marker'
    result = adapter.stop(identity='owned')
    assert result['reconciliation']['state'] == 'unknown'
    assert adapter.marker_owner() == foreign['request_id']
    assert result['install_enabled'] is False

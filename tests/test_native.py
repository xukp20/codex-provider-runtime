"""Black-box acceptance against the native daemon and a local fake Responses API.

Run explicitly with CPR_TEST_BINARY=/path/to/codex pytest -q tests/test_native.py.
No test uses the caller's CODEX_HOME, App socket, or real provider credentials.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time

import psutil
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'experiments'))
from mock_responses import MockResponses
from codex_provider_runtime.runtime import private_socket_directory


BINARY = os.environ.get('CPR_TEST_BINARY')
pytestmark = pytest.mark.skipif(not BINARY, reason='Set CPR_TEST_BINARY for isolated native acceptance')


class NativeFixture:
    def __init__(self, root, mock):
        self.root, self.mock = root, mock
        self.app = None
        self.log = None
        # Native Windows AF_UNIX rejects the inherited Temp directory ACL.
        # Only this disposable fixture directory is made private.
        private_socket_directory(root)
        self.home = root / 'home'
        self.home.mkdir()
        self.sock = root / 'app.sock'
        self.env = {**os.environ, 'CODEX_HOME': str(self.home),
                    'CPR_RUNTIME_DIR': str(root),
                    'PYTHONPATH': str(Path(__file__).resolve().parents[1] / 'src')}
        self.env.pop('CODEX_APP_SERVER_SOCKET', None)
        self.env.pop('CPR_FORCE_PROXY', None)
        for name in ('CODEX_SQLITE_HOME', 'CODEX_API_KEY', 'CODEX_ACCESS_TOKEN',
                     'OPENAI_API_KEY', 'OPENAI_BASE_URL'):
            self.env.pop(name, None)
        self.argv = [sys.executable, '-m', 'codex_provider_runtime.cli', '--home', str(self.home),
                     '--binary', BINARY, '--app-socket', str(self.sock)]
        self.home.joinpath('config.toml').write_text(f'''model="gpt-6.1-sol"
model_provider="fixture-a"
[features]
code_mode_host=true
multi_agent=true
[model_providers.fixture-a]
name="Fixture A"
base_url="{mock.url}/a/v1"
wire_api="responses"
experimental_bearer_token="local-fixture-only-a"
[model_providers.fixture-b]
name="Fixture B"
base_url="{mock.url}/b/v1"
wire_api="responses"
experimental_bearer_token="local-fixture-only-b"
''')
        self.log = root.joinpath('app.log').open('wb')
        process_options = ({'creationflags': subprocess.CREATE_NO_WINDOW |
                            subprocess.CREATE_NEW_PROCESS_GROUP} if os.name == 'nt'
                           else {'start_new_session': True})
        self.app = subprocess.Popen([BINARY, 'app-server', '--listen', 'unix://' + str(self.sock)],
                                    env=self.env, stdin=subprocess.DEVNULL,
                                    stdout=self.log, stderr=self.log, **process_options)
        deadline = time.monotonic() + 20
        while time.monotonic() < deadline:
            assert self.app.poll() is None, root.joinpath('app.log').read_text()
            if self.sock.exists():
                row = self.call('doctor')
                if any(s.get('reachable') and s.get('kind') == 'app' for s in row['servers']):
                    return
            time.sleep(.1)
        raise AssertionError('Isolated native App fixture did not become ready')

    def call(self, *args, expected=0, external=False):
        argv = self.argv + (['--external', '--sender', 'pytest'] if external else []) + list(args)
        completed = subprocess.run(argv, env=self.env, text=True, capture_output=True, timeout=35)
        assert completed.returncode == expected, (argv, completed.returncode, completed.stdout, completed.stderr)
        raw = completed.stdout or completed.stderr
        return json.loads(raw)

    def create(self, title, provider='fixture-b', **options):
        argv = ['create', '--provider', provider, '--title', title, '--cwd', str(self.root)]
        for key, value in options.items():
            argv.extend(['--' + key.replace('_', '-'), str(value)])
        return self.call(*argv)

    def cleanup(self):
        # Kill only processes launched into this fixture's private records/watch files.
        for path in self.root.glob('cpr-*/*.json'):
            record = json.loads(path.read_text())
            try:
                process = psutil.Process(record['pid'])
                if abs(process.create_time() - record['created']) < .01 and record['socket'] in ' '.join(process.cmdline()):
                    process.terminate()
                    process.wait(10)
            except (KeyError, psutil.Error):
                pass
        for path in self.home.glob('provider-runtime/watches/*.json'):
            job = json.loads(path.read_text())
            try:
                process = psutil.Process(job['pid'])
                if str(path) in process.cmdline() and '_watch' in process.cmdline():
                    process.terminate()
                    process.wait(10)
            except (KeyError, psutil.Error):
                pass
        if self.app is not None and self.app.poll() is None:
            self.app.terminate()
            self.app.wait(10)
        if self.log is not None:
            self.log.close()


@pytest.fixture
def native():
    with tempfile.TemporaryDirectory(prefix='cpr-native-') as directory, MockResponses() as mock:
        fixture = NativeFixture.__new__(NativeFixture)
        try:
            fixture.__init__(Path(directory), mock)
            yield fixture
        finally:
            fixture.cleanup()


def test_native_creation_routing_names_and_receipts(native):
    before = len(native.mock.requests)
    source = native.create('Native exact', model='gpt-6-astra', effort='medium', context_window=100000)
    assert len(native.mock.requests) == before, 'Empty creation performed inference'
    assert source['model'] == 'gpt-6-astra' and source['effort'] == 'medium'
    assert source['context'] == {'model_context_window': 100000}
    found = native.call('find', '--name', 'Native exact')['thread']
    assert found['id'] == source['threadId'] and found['modelProvider'] == 'fixture-b'
    child = native.call('fork', '--thread', source['threadId'], '--provider', 'fixture-a', '--title', 'Native fork')
    assert child['provider'] == 'fixture-a'
    assert len(native.mock.requests) == before, 'Empty fork performed inference'
    second = native.create('Native exact second')
    unique = native.call('find', '--name', 'EXACT SECOND')['thread']
    assert unique['id'] == second['threadId']
    unknown = native.call('server', 'ensure', '--provider', 'missing-provider', expected=1)
    assert 'Unknown native provider ID' in unknown['error']
    rejected = native.call('send', '--name', 'Native', '--message', 'SHOULD_NOT_SEND', expected=1)
    assert 'exactly one match' in rejected['error']
    assert len(native.mock.requests) == before
    exact = native.call('find', '--name', 'Native exact')['thread']
    assert exact['id'] == source['threadId']
    sent = native.call('send', '--name', 'Native exact', '--message', 'SHORT', '--wait', '--timeout', '10', '--request-id', 'native-repeat')
    assert sent['status'] == 'completed'
    assert any(r['path'] == '/b/v1/responses' and r['model'] == 'gpt-6-astra' and r['effort'] == 'medium' for r in native.mock.requests), native.mock.requests
    before_populated_fork = len(native.mock.requests)
    populated = native.call('fork', '--thread', source['threadId'], '--provider', 'fixture-a', '--title', 'Populated cross fork')
    assert len(native.mock.requests) == before_populated_fork, 'Populated fork performed inference'
    assert populated['provider'] == 'fixture-a'
    assert populated['model'] == 'gpt-6-astra' and populated['effort'] == 'medium'
    fork_done = native.call('send', '--thread', populated['threadId'], '--message', 'FORK SHORT', '--wait', '--timeout', '10')
    assert fork_done['status'] == 'completed'
    assert native.mock.requests[-1]['path'] == '/a/v1/responses'
    assert native.mock.requests[-1]['model'] == 'gpt-6-astra' and native.mock.requests[-1]['effort'] == 'medium'
    after = len(native.mock.requests)
    repeated = native.call('send', '--name', 'Native exact', '--message', 'SHORT', '--wait', '--timeout', '10', '--request-id', 'native-repeat')
    assert repeated['reusedReceipt'] is True and repeated['turnId'] == sent['turnId']
    assert len(native.mock.requests) == after, 'Receipt retry performed duplicate inference'
    thread = native.call('read', '--thread', source['threadId'])['thread']
    assert any(t['id'] == sent['turnId'] for t in thread['turns'])
    conflict = native.call('send', '--thread', source['threadId'], '--provider', 'fixture-a', '--message', 'SHOULD_NOT_SEND', expected=1)
    assert 'Provider conflict' in conflict['error'] and len(native.mock.requests) == after
    native.call('server', 'stop', '--provider', 'fixture-b')
    resumed = native.call('resume', '--thread', source['threadId'])
    assert resumed['context'] == {'model_context_window': 100000}
    assert resumed['model'] == 'gpt-6-astra' and resumed['effort'] == 'medium'
    again = native.call('send', '--thread', source['threadId'], '--message', 'AFTER RESTART', '--wait', '--timeout', '10')
    assert again['status'] == 'completed'
    rollout = next(native.home.glob('sessions/**/*' + source['threadId'] + '.jsonl'))
    context_windows = [v['payload']['model_context_window'] for line in rollout.read_text().splitlines()
                       for v in [json.loads(line)] if v.get('type') == 'event_msg' and v['payload'].get('type') == 'task_started']
    assert context_windows == [95000, 95000]
    denied = native.call('server', 'stop', '--provider', 'fixture-a', expected=1)
    assert 'never stopped' in denied['error'] and native.app.poll() is None


def test_native_disconnect_interrupt_and_auto_ensure_external(native):
    thread = native.create('Slow native')
    before = len(native.mock.requests)
    missing_sender = native.call('--external', 'send', '--thread', thread['threadId'],
                                 '--message', 'SHOULD_NOT_SEND', expected=1)
    assert 'sender attribution' in missing_sender['error']
    assert len(native.mock.requests) == before
    sent = native.call('send', '--thread', thread['threadId'], '--message', 'SLOW_E1')
    # The send CLI has exited, leaving no subscriber; the owned native daemon continues.
    done = native.call('wait', '--thread', thread['threadId'], '--turn', sent['turnId'], '--timeout', '10')
    assert done['status'] == 'completed'
    second = native.call('send', '--thread', thread['threadId'], '--message', 'SLOW_E1 again')
    refused = native.call('server', 'stop', '--provider', 'fixture-b', expected=1)
    assert 'active tasks' in refused['error']
    native.call('interrupt', '--thread', thread['threadId'], '--turn', second['turnId'])
    interrupted = native.call('wait', '--thread', thread['threadId'], '--turn', second['turnId'], '--timeout', '10', expected=4)
    assert interrupted['status'] == 'interrupted'
    native.call('server', 'stop', '--provider', 'fixture-b')
    started = native.call('send', '--thread', thread['threadId'], '--message', 'SHORT EXTERNAL',
                          '--request-id', 'native-external', '--wait', '--timeout', '10', external=True)
    assert started['status'] == 'completed' and started['provider'] == 'fixture-b'
    assert '[From pytest, an external agent, via codex-provider-runtime.]' in json.dumps(native.mock.last_body)
    before_conflict = len(native.mock.requests)
    sender_conflict = native.call('--external', '--sender', 'different sender', 'send',
                                  '--thread', thread['threadId'], '--message', 'SHORT EXTERNAL',
                                  '--request-id', 'native-external', expected=1)
    assert 'different operation' in sender_conflict['error']
    assert len(native.mock.requests) == before_conflict, 'Sender conflict performed inference'
    assert native.app.poll() is None


def test_native_one_shot_reply_wakes_idle_destination(native):
    destination = native.create('Reply destination', provider='fixture-a')
    source = native.create('Reply source')
    sent = native.call('send', '--thread', source['threadId'], '--message', 'SHORT', '--reply-to', destination['threadId'], '--request-id', 'native-watch')
    path = Path(sent['watchFile'])
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline:
        state = json.loads(path.read_text())
        if state['state'] in {'completed', 'failed', 'waiting'}:
            break
        time.sleep(.1)
    assert state['state'] == 'completed', state
    assert state['result']['replyVerified'] and state['result']['newTurnStarted']
    replied_turn = state['result']['turnId']
    done = native.call('wait', '--thread', destination['threadId'], '--turn', replied_turn, '--timeout', '10')
    assert done['status'] == 'completed'
    before = len(native.mock.requests)
    again = native.call('send', '--thread', source['threadId'], '--message', 'SHORT', '--reply-to', destination['threadId'], '--request-id', 'native-watch')
    assert again['watchId'] == sent['watchId']
    assert len(native.mock.requests) == before


def test_native_empty_configuration_survives_owned_restart(native):
    created = native.create('Empty cold configuration', model='gpt-6-astra', effort='medium', context_window=100000)
    assert not native.mock.requests
    native.call('server', 'stop', '--provider', 'fixture-b')
    found = native.call('find', '--name', 'Empty cold configuration')['thread']
    assert found['id'] == created['threadId'] and found['modelProvider'] == 'fixture-b'
    assert not native.mock.requests, 'Offline empty name lookup performed inference'
    sent = native.call('send', '--name', 'Empty cold configuration', '--message', 'AFTER EMPTY RESTART', '--wait', '--timeout', '10')
    assert sent['status'] == 'completed' and sent['threadId'] == created['threadId']
    assert len(native.mock.requests) == 1
    assert native.mock.requests[-1]['model'] == 'gpt-6-astra'
    assert native.mock.requests[-1]['effort'] == 'medium'
    resumed = native.call('resume', '--thread', created['threadId'])
    assert resumed['model'] == 'gpt-6-astra' and resumed['effort'] == 'medium', resumed
    assert resumed['context'] == {'model_context_window': 100000}
    assert len(native.mock.requests) == 1, 'Explicit idle resume performed inference'

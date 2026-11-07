"""Native Stage 0 experiment against an isolated home and local fixture only."""
import argparse
import asyncio
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

sys.path.insert(0, str(Path(__file__).parent))
from mock_responses import MockResponses
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'src'))
from codex_provider_runtime.transport import Rpc


def spawn(binary, home, sock, log):
    env = {**os.environ, 'CODEX_HOME': str(home)}
    return subprocess.Popen([binary, '-c', 'model_provider="mock-a"', 'app-server', '--listen', 'unix://' + str(sock)],
        env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=log, start_new_session=True)


async def ready(sock, process, binary):
    for _ in range(120):
        if process.poll() is not None:
            raise RuntimeError('owned server exited: ' + str(process.returncode))
        if sock.exists():
            try:
                async with Rpc(sock, binary, timeout=1):
                    return
            except Exception:
                pass
        await asyncio.sleep(.1)
    raise TimeoutError('native socket not ready')


async def terminal(sock, tid, turn, binary):
    async with Rpc(sock, binary) as c:
        for _ in range(100):
            r = await c.request('thread/turns/list', {'threadId': tid, 'limit': 10, 'itemsView': 'full'})
            found = next((t for t in r['data'] if t['id'] == turn), None)
            if found and found['status'] != 'inProgress':
                return found
            await asyncio.sleep(.15)
    raise TimeoutError('turn did not finish')


async def main(args):
    result = {'binary': args.binary, 'protected_server_pid': 32066, 'real_inference': False}
    with tempfile.TemporaryDirectory(prefix='cpr-e1-') as d, MockResponses() as mock:
        root = Path(d); home = root/'home';home.mkdir();sock = root/'a.sock'
        (home/'cross.toml').write_text('name="cross"\ndescription="Cross fixture role"\nmodel="gpt-6.1-sol"\nmodel_provider="mock-b"\ndeveloper_instructions="Bounded fixture child."\n')
        (home/'config.toml').write_text(f'''model="gpt-6.1-sol"
model_provider="mock-a"
[features]
multi_agent=true
code_mode_host=true
[agents.cross]
description="Cross fixture role"
config_file="{home/'cross.toml'}"
[model_providers.mock-a]
name="Mock A"
base_url="{mock.url}/a/v1"
wire_api="responses"
experimental_bearer_token="fake-a"
[model_providers.mock-b]
name="Mock B"
base_url="{mock.url}/b/v1"
wire_api="responses"
experimental_bearer_token="fake-b"
''')
        log = open(root/'server.log', 'wb');process = spawn(args.binary, home, sock, log)
        try:
            await ready(sock, process, args.binary)
            async with Rpc(sock, args.binary) as c:
                r = await c.request('thread/start', {'cwd': d, 'model': 'gpt-6.1-sol',
                    'approvalPolicy': 'never', 'sandbox': 'danger-full-access',
                    'config': {'model_reasoning_effort': 'high', 'model_context_window': 100000}})
                tid = r['thread']['id']
                r = await c.request('turn/start', {'threadId': tid, 'input': [{'type': 'text', 'text': 'SLOW_E1'}]})
                turn = r['turn']['id']
            await asyncio.sleep(4)
            done = await terminal(sock, tid, turn, args.binary)
            result['E1'] = {'status': done['status'], 'messages': [x.get('text') for x in done.get('items', []) if x.get('type') == 'agentMessage'],
                'server_alive_after_disconnect': process.poll() is None}
            # Tool definitions are primary evidence for direct model/effort fields.
            tools = [x for x in mock.tools if 'spawn_agent' in json.dumps(x)]
            result['body_info'] = {k:v for k,v in mock.last_body.items() if k not in ['input','instructions']}
            result['spawn_schema'] = tools
            result['tool_names'] = [x.get('name', x.get('type')) for x in mock.tools]
            tool = next((x for top in mock.tools for x in (top.get('tools', []) if top.get('type') == 'namespace' else [top]) if x.get('name') == 'spawn_agent'), None)
            assert tool, 'native spawn schema absent'
            if tool:
                for label, arguments in [
                    ('direct', {'message': 'E4_CHILD_DIRECT', 'model': 'gpt-6.1-sol', 'reasoning_effort': 'high', 'task_name': 'direct', 'fork_turns': 'none'}),
                    ('different_model', {'message': 'E4_CHILD_DIFFERENT_MODEL', 'model': 'gpt-6-astra', 'reasoning_effort': 'medium', 'task_name': 'different_model', 'fork_turns': 'none'}),
                    ('role', {'message': 'E4_CHILD_ROLE', 'agent_type': 'cross', 'task_name': 'cross', 'fork_turns': 'none'}),
                ]:
                    before = len(mock.requests)
                    mock.next_call = {'name': tool['name'], 'namespace': 'collaboration', 'arguments': arguments}
                    async with Rpc(sock, args.binary) as c:
                        r = await c.request('thread/start', {'cwd': d, 'approvalPolicy': 'never', 'sandbox': 'danger-full-access',
                            'config': {'model_context_window': 123456, 'model_auto_compact_token_limit': 110000}})
                        parent = r['thread']['id']
                        r = await c.request('turn/start', {'threadId': parent, 'input': [{'type': 'text', 'text': 'E4_PARENT_' + label}] })
                        parent_turn = r['turn']['id']
                    await asyncio.sleep(3)
                    r = await terminal(sock, parent, parent_turn, args.binary)
                    result['E4_' + label] = {'status': r['status'], 'requests': mock.requests[before:],
                        'tool_items': [x for x in r.get('items', []) if x.get('type') in ['collabAgentSpawnTool', 'agentMessage']]}
                    actual_children = [x for x in mock.requests[before:] if x.get('agent_name') != '/root']
                    assert actual_children, 'spawn produced no child inference'
                    assert all(x['path'] == '/a/v1/responses' for x in actual_children), actual_children
                    if label == 'direct':
                        assert any(x['model'] == 'gpt-6.1-sol' and x['effort'] == 'high' for x in actual_children), actual_children
                    if label == 'different_model':
                        assert any(x['model'] == 'gpt-6-astra' and x['effort'] == 'medium' for x in actual_children), actual_children
            # Restart only our owned process; inspect persistence without overlays.
            process.terminate();process.wait(timeout=15)
            process = spawn(args.binary, home, sock, log)
            await ready(sock, process, args.binary)
            async with Rpc(sock, args.binary) as c:
                r = await c.request('thread/resume', {'threadId': tid, 'excludeTurns': True})
                result['resume'] = {k:r.get(k) for k in ['model','modelProvider','reasoningEffort']}
                r = await c.request('turn/start', {'threadId': tid, 'input': [{'type': 'text','text': 'RESUME_FAST'}]})
                newturn=r['turn']['id']
            await terminal(sock,tid,newturn,args.binary)
            contexts=[]
            sessions=[]
            for p in home.glob('sessions/**/*.jsonl'):
                session_summary = {'file': p.name, 'contexts': []}
                for line in p.read_text().splitlines():
                    v=json.loads(line);payload=v.get('payload',{})
                    if v.get('type') == 'session_meta':
                        session_summary.update({k:payload.get(k) for k in ['id', 'model_provider', 'source']})
                    if v.get('type') == 'turn_context':
                        session_summary.update({k:payload.get(k) for k in ['model', 'effort', 'model_context_window', 'model_auto_compact_token_limit'] if k in payload})
                    if v.get('type')=='event_msg' and payload.get('type')=='task_started':
                        context = {'turn_id':payload.get('turn_id'),'model_context_window':payload.get('model_context_window')}
                        contexts.append(context)
                        session_summary['contexts'].append(context)
                sessions.append(session_summary)
            result['task_context_windows']=contexts
            result['sessions']=sessions
            e1_session = next(x for x in sessions if x.get('id') == tid)
            child_thread_ids = {x['thread_id'] for x in mock.requests if x.get('agent_name') != '/root'}
            child_sessions = [x for x in sessions if x.get('id') in child_thread_ids]
            assert len(child_sessions) == 3, child_sessions
            assert all(x['model_provider'] == 'mock-a' for x in child_sessions), child_sessions
            assert all(x['contexts'][0]['model_context_window'] == 117283 for x in child_sessions), child_sessions
            result['conclusions'] = {
                'disconnect_survives': result['E1']['status'] == 'completed' and result['E1']['server_alive_after_disconnect'],
                'model_and_effort_native_spawn': bool(result.get('E4_direct')) and bool(result.get('E4_different_model')),
                'role_provider_inherits_parent': bool(result.get('E4_role')),
                'fresh_children_inherit_context_window_in_this_config': all(x['contexts'][0]['model_context_window'] == 117283 for x in child_sessions),
                'context_window_cold_resume_lost': len({x['model_context_window'] for x in e1_session['contexts']}) > 1,
            }
            assert all(result['conclusions'].values()), result['conclusions']
            result['source_references'] = [
                'https://github.com/openai/codex/blob/rust-v0.167.0/codex-rs/core/src/agent/role.rs',
                'https://github.com/openai/codex/blob/rust-v0.167.0/codex-rs/core/src/agent/child_config.rs',
                'https://github.com/openai/codex/blob/rust-v0.167.0/codex-rs/core/src/tools/handlers/multi_agents_spec.rs',
                'https://github.com/openai/codex/blob/rust-v0.167.0/codex-rs/core/src/client.rs',
            ]
            result['requests']=mock.requests
        except Exception as e:
            result['error']=repr(e)
        finally:
            if process.poll() is None:
                process.terminate();process.wait(timeout=15)
            log.close()
            result['log_tail']=(root/'server.log').read_text(errors='replace')[-7000:]
    Path(args.output).write_text(json.dumps(result,ensure_ascii=False,indent=2)+'\n')
    print(json.dumps({k:v for k,v in result.items() if k not in ['spawn_schema','log_tail','E4_direct','E4_role','E4_different_model','body_info','sessions']},ensure_ascii=False))
    if 'error' in result:raise SystemExit(1)

if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('--binary',required=True);p.add_argument('--output',required=True)
    asyncio.run(main(p.parse_args()))

"""Local Responses fixture; never forwards requests to a real provider."""
import json
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class MockResponses:
    def __init__(self):
        self.requests = []
        self.tools = []
        self.next_call = None
        self.last_body = {}
        fixture = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = 'HTTP/1.1'

            def log_message(self, *_):
                pass

            def do_GET(self):
                raw = json.dumps({'object': 'list', 'data': []}).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers.get('Content-Length', 0))))
                fixture.last_body = body
                fixture.tools = body.get('tools', []) or [tool for item in body.get('input', []) if item.get('type') == 'additional_tools' for tool in item.get('tools', [])]
                content = json.dumps(body.get('input', []), ensure_ascii=False)
                metadata = body.get('client_metadata', {})
                turn_metadata = json.loads(metadata.get('x-codex-turn-metadata', '{}'))
                fixture.requests.append({'path': self.path, 'model': body.get('model'),
                    'thread_id': metadata.get('thread_id'), 'agent_name': turn_metadata.get('agent_name'),
                    'effort': body.get('reasoning', {}).get('effort'),
                    'child': 'E4_CHILD' in content, 'slow': 'SLOW_E1' in content})
                if 'SLOW_E1' in content:
                    time.sleep(3)
                call = fixture.next_call
                fixture.next_call = None
                rid = 'resp_' + uuid.uuid4().hex
                if call:
                    item = {'type': 'function_call', 'id': 'fc_' + uuid.uuid4().hex,
                        'call_id': 'call_' + uuid.uuid4().hex,
                        'name': call['name'], 'arguments': json.dumps(call['arguments']), 'status': 'completed'}
                    if call.get('namespace'): item['namespace'] = call['namespace']
                else:
                    item = {'type': 'message', 'id': 'msg_' + uuid.uuid4().hex,
                        'role': 'assistant', 'status': 'completed',
                        'content': [{'type': 'output_text', 'text': 'MOCK_OK', 'annotations': []}]}
                response = {'id': rid, 'object': 'response', 'status': 'completed',
                    'model': body.get('model'), 'output': [item],
                    'usage': {'input_tokens': 10, 'output_tokens': 2, 'total_tokens': 12}}
                events = [
                    {'type': 'response.created', 'response': {**response, 'status': 'in_progress', 'output': []}},
                    {'type': 'response.output_item.added', 'output_index': 0, 'item': item},
                ]
                if call:
                    events.append({'type': 'response.function_call_arguments.done', 'item_id': item['id'],
                        'output_index': 0, 'arguments': item['arguments']})
                else:
                    events.append({'type': 'response.output_text.delta', 'item_id': item['id'],
                        'output_index': 0, 'content_index': 0, 'delta': 'MOCK_OK'})
                events.extend([
                    {'type': 'response.output_item.done', 'output_index': 0, 'item': item},
                    {'type': 'response.completed', 'response': response},
                ])
                raw = ''.join('event: ' + e['type'] + '\ndata: ' + json.dumps(e) + '\n\n' for e in events).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                try:
                    self.wfile.write(raw)
                    self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass

        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    @property
    def url(self):
        return 'http://127.0.0.1:' + str(self.server.server_port)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.server.shutdown()
        self.server.server_close()

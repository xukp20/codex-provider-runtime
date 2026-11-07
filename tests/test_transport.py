"""Fake RPC fixtures only; the optional native proxy merely tunnels fixture bytes."""
import asyncio
import importlib.util
import json
import os
from pathlib import Path
import socket
import tempfile
import unittest
from unittest.mock import patch

from websockets.legacy.server import unix_serve

from codex_provider_runtime.transport import Rpc, RpcError, TransportError


BINARY = Path(os.environ.get('CPR_TEST_BINARY') or
              '/root/.cache/codex-provider-runtime/toolchains/0.167.0/codex')
UNIX_FIXTURE = os.name != 'nt' and hasattr(socket, 'AF_UNIX')


@unittest.skipUnless(UNIX_FIXTURE, 'Python Unix server fixture unavailable; native integration covers Windows proxy')
class RpcTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix='cpr-transport-')
        self.addCleanup(self.temporary.cleanup)
        self.path = Path(self.temporary.name) / 'fixture.sock'
        self.incoming = asyncio.Queue()
        self.initialized = asyncio.Event()
        self.connection = None
        self.init = None
        self.server = await unix_serve(self.handle, str(self.path))
        self.force = patch.dict(os.environ, {'CPR_FORCE_PROXY': '0'})
        self.force.start()
        self.addCleanup(self.force.stop)

    async def asyncTearDown(self):
        self.server.close()
        await self.server.wait_closed()

    async def handle(self, connection):
        self.connection = connection
        try:
            async for raw in connection:
                message = json.loads(raw)
                self.assertNotIn('jsonrpc', message)
                if message.get('method') == 'initialize':
                    self.init = message
                    await connection.send(json.dumps({'id': message['id'], 'result': {'userAgent': 'fixture'}}))
                elif message.get('method') == 'initialized':
                    self.initialized.set()
                else:
                    self.incoming.put_nowait(message)
        except Exception:
            if not connection.closed:
                raise

    def rpc(self, timeout=1):
        return Rpc(self.path, str(BINARY), timeout=timeout)

    async def send(self, message):
        await self.connection.send(json.dumps(message))

    async def test_initialize_and_concurrent_routing_without_reverse_approval(self):
        async with self.rpc() as rpc:
            await asyncio.wait_for(self.initialized.wait(), 1)
            self.assertTrue(self.init['params']['capabilities']['experimentalApi'])
            first = asyncio.create_task(rpc.request('first', {'n': 1}))
            second = asyncio.create_task(rpc.request('second', {'n': 2}))
            messages = [await self.incoming.get(), await self.incoming.get()]
            # A native request may reuse an outbound ID; method distinguishes it.
            reverse = {'id': messages[0]['id'], 'method': 'item/commandExecution/requestApproval', 'params': {'command': 'fixture'}}
            await self.send(reverse)
            await self.send({'method': 'thread/started', 'params': {'threadId': 'fixture'}})
            for message in reversed(messages):
                await self.send({'id': message['id'], 'result': {'method': message['method']}})
            self.assertEqual(await first, {'method': 'first'})
            self.assertEqual(await second, {'method': 'second'})
            self.assertEqual(await rpc.notifications.get(), {'method': 'thread/started', 'params': {'threadId': 'fixture'}})
            self.assertEqual(rpc.pending_requests[reverse['id']], reverse)
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(self.incoming.get(), .03)
            waiter = await self.connection.ping(b'fixture-ping')
            await asyncio.wait_for(waiter, 1)

    async def test_rpc_error_keeps_native_error_and_method(self):
        async with self.rpc() as rpc:
            request = asyncio.create_task(rpc.request('bad/method', {}))
            message = await self.incoming.get()
            error = {'code': -32601, 'message': 'Unknown fixture method', 'data': {'fixture': True}}
            await self.send({'id': message['id'], 'error': error})
            with self.assertRaises(RpcError) as caught:
                await request
            self.assertEqual(caught.exception.method, 'bad/method')
            self.assertEqual(caught.exception.error, error)

    async def test_timeout_and_late_reply_do_not_misroute_next_response(self):
        async with self.rpc() as rpc:
            request = asyncio.create_task(rpc.request('slow', {}, timeout=.03))
            timed_out = await self.incoming.get()
            with self.assertRaisesRegex(TransportError, 'timed out'):
                await request
            self.assertFalse(rpc._waiting)
            await self.send({'id': timed_out['id'], 'result': {'late': True}})
            next_request = asyncio.create_task(rpc.request('next', {}))
            next_message = await self.incoming.get()
            await self.send({'id': next_message['id'], 'result': {'next': True}})
            self.assertEqual(await next_request, {'next': True})

    async def test_disconnect_fails_all_waiters_and_future_requests(self):
        async with self.rpc() as rpc:
            tasks = [asyncio.create_task(rpc.request('wait', {})) for _ in range(2)]
            for _ in tasks:
                await self.incoming.get()
            await self.connection.close()
            results = await asyncio.gather(*tasks, return_exceptions=True)
            self.assertTrue(all(isinstance(result, TransportError) for result in results))
            with self.assertRaises(TransportError):
                await rpc.request('after-close', {})
        self.assertFalse(rpc._waiting)

    async def test_context_close_fails_pending_request(self):
        async with self.rpc() as rpc:
            task = asyncio.create_task(rpc.request('wait', {}))
            await self.incoming.get()
        with self.assertRaises(TransportError):
            await task
        await rpc.close()  # Idempotent; the fixture server still accepts clients.
        async with self.rpc():
            pass

    async def test_missing_socket_reports_transport_error(self):
        with self.assertRaises(TransportError):
            async with Rpc(self.path.parent / 'absent.sock', str(BINARY), timeout=.1):
                pass


@unittest.skipUnless(UNIX_FIXTURE and BINARY.is_file() and importlib.util.find_spec('wsproto'),
                     'Python Unix server fixture and native proxy binary required')
class ProxyRpcTests(RpcTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.proxy_environment = patch.dict(os.environ, {'CPR_FORCE_PROXY': '1'})
        self.proxy_environment.start()
        self.addCleanup(self.proxy_environment.stop)

    async def test_proxy_http_upgrade_fragmentation_ping_and_owned_shutdown(self):
        # A proxy subprocess is the only native process started by this test.
        async with self.rpc() as rpc:
            process = rpc._connection.process
            await asyncio.wait_for(self.initialized.wait(), 1)
            self.assertEqual(self.connection.path, '/rpc')
            await self.connection.send(['{"method":"fragmented",', '"params":{"ok":true}}'])
            self.assertEqual((await rpc.notifications.get())['params'], {'ok': True})
            pong = await self.connection.ping(b'proxy-ping')
            await asyncio.wait_for(pong, 1)
            self.assertIsNone(process.returncode)
        self.assertIsNotNone(process.returncode)
        # Closing the owned tunnel must leave the listening server available.
        with patch.dict(os.environ, {'CPR_FORCE_PROXY': '0'}):
            async with self.rpc():
                pass


if __name__ == '__main__':
    unittest.main()

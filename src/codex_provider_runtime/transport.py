"""An initialized JSON-RPC connection to an existing native App Server.

Unix sockets carry WebSockets, including through ``app-server proxy`` pipes.
This client never launches or shuts down an App Server and never answers native
server requests automatically.
"""
from __future__ import annotations

import asyncio
from collections import deque
from contextlib import suppress
import json
import os
from pathlib import Path
import sys
from typing import Any


class TransportError(RuntimeError):
    """The native connection was lost or an operation timed out."""


class RpcError(RuntimeError):
    def __init__(self, method: str, error_dict: dict):
        self.method = method
        self.error = error_dict
        super().__init__(f"{method}: {error_dict.get('message', 'RPC error')}")


class _ProxyWebSocket:
    """WebSocket framing on the raw byte tunnel owned by a proxy subprocess."""

    def __init__(self):
        from wsproto import ConnectionType, WSConnection

        self.process: asyncio.subprocess.Process | None = None
        self.connection = WSConnection(ConnectionType.CLIENT)
        self.events = deque()
        self.fragments: list[str] = []
        self.write_lock = asyncio.Lock()

    async def _send_event(self, event):
        async with self.write_lock:
            data = self.connection.send(event)
            self.process.stdin.write(data)
            await self.process.stdin.drain()

    async def _next_event(self):
        while not self.events:
            data = await self.process.stdout.read(65536)
            if not data:
                raise TransportError("Native proxy connection closed")
            self.connection.receive_data(data)
            self.events.extend(self.connection.events())
        return self.events.popleft()

    async def handshake(self):
        from wsproto.events import AcceptConnection, Request

        await self._send_event(Request(host="localhost", target="/rpc"))
        event = await self._next_event()
        if not isinstance(event, AcceptConnection):
            raise TransportError("Native proxy WebSocket upgrade rejected")

    async def send(self, text: str):
        from wsproto.events import TextMessage

        await self._send_event(TextMessage(data=text))

    async def recv(self) -> str:
        from wsproto.events import CloseConnection, Ping, Pong, TextMessage

        while True:
            event = await self._next_event()
            if isinstance(event, TextMessage):
                self.fragments.append(event.data)
                if event.message_finished:
                    text = "".join(self.fragments)
                    self.fragments.clear()
                    return text
            elif isinstance(event, Ping):
                await self._send_event(event.response())
            elif isinstance(event, CloseConnection):
                with suppress(Exception):
                    await self._send_event(event.response())
                raise TransportError("Native proxy WebSocket closed")
            elif not isinstance(event, Pong):
                raise TransportError("Expected native WebSocket text message")

    async def close(self):
        if self.process is None:
            return
        from wsproto.events import CloseConnection

        with suppress(Exception):
            await asyncio.wait_for(self._send_event(CloseConnection(code=1000)), 1)
        if self.process.stdin:
            self.process.stdin.close()
        try:
            await asyncio.wait_for(self.process.wait(), 1)
        except asyncio.TimeoutError:
            with suppress(ProcessLookupError):
                self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), 1)
            except asyncio.TimeoutError:
                with suppress(ProcessLookupError):
                    self.process.kill()
                await self.process.wait()


class Rpc:
    """One connection, one reader, and independently routed concurrent requests.

    ``pending_requests`` retains raw native reverse requests keyed by their IDs.
    The caller must decide how to handle these; no approval or error response is
    sent implicitly. ``notifications`` contains raw messages without an ID.
    """

    def __init__(self, socket_path: str | Path, binary: str, timeout: float = 15):
        self.socket_path = str(socket_path)
        self.binary = binary
        self.timeout = timeout
        self.notifications: asyncio.Queue[dict] = asyncio.Queue()
        self.pending_requests: dict[Any, dict] = {}
        self._waiting: dict[Any, asyncio.Future] = {}
        self._next_id = 0
        self._connection = None
        self._reader = None
        self._failure: TransportError | None = None
        self._closed = False

    async def __aenter__(self):
        if self._connection is not None or self._closed:
            raise TransportError("RPC connection cannot be reopened")
        try:
            if sys.platform == "win32" or os.environ.get("CPR_FORCE_PROXY") == "1":
                # The proxy translates no protocol: stdout/stdin are raw socket bytes.
                self._connection = _ProxyWebSocket()
                self._connection.process = await asyncio.wait_for(asyncio.create_subprocess_exec(
                    self.binary, "app-server", "proxy", "--sock", self.socket_path,
                    stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                ), self.timeout)
                await asyncio.wait_for(self._connection.handshake(), self.timeout)
            else:
                from websockets.legacy.client import unix_connect

                self._connection = await unix_connect(
                    self.socket_path, uri="ws://localhost/rpc", open_timeout=self.timeout,
                    close_timeout=1, max_size=None,
                )
            self._reader = asyncio.create_task(self._read(), name="native-rpc-reader")
            await self.request("initialize", {
                "clientInfo": {"name": "codex_provider_runtime", "title": "Codex Provider Runtime", "version": "0.1.0"},
                "capabilities": {"experimentalApi": True},
            })
            await self._send({"method": "initialized", "params": {}})
            return self
        except BaseException as error:
            await self.close()
            if isinstance(error, (asyncio.CancelledError, RpcError, TransportError)):
                raise
            raise TransportError("Unable to initialize native RPC connection") from error

    async def __aexit__(self, exc_type, exc, traceback):
        await self.close()

    async def _send(self, message: dict):
        if self._closed or self._failure is not None or self._connection is None:
            raise self._failure or TransportError("RPC connection is closed")
        try:
            await self._connection.send(json.dumps(message, separators=(",", ":")))
        except Exception as error:
            failure = TransportError("Native RPC send failed")
            self._fail(failure)
            raise failure from error

    async def request(self, method: str, params: dict, timeout: float | None = None) -> dict:
        self._next_id += 1
        request_id = self._next_id
        future = asyncio.get_running_loop().create_future()
        self._waiting[request_id] = future
        try:
            async def exchange():
                await self._send({"id": request_id, "method": method, "params": params})
                return await future

            response = await asyncio.wait_for(exchange(), self.timeout if timeout is None else timeout)
            if "error" in response:
                raise RpcError(method, response["error"])
            result = response.get("result", {})
            if not isinstance(result, dict):
                raise TransportError(f"Native RPC {method} returned a non-object result")
            return result
        except asyncio.TimeoutError as error:
            raise TransportError(f"Native RPC {method} timed out") from error
        finally:
            self._waiting.pop(request_id, None)
            # A send failure can also fail this future before exchange awaits it.
            if future.done() and not future.cancelled():
                future.exception()
            elif not future.done():
                future.cancel()

    def _fail(self, failure: TransportError):
        self._failure = self._failure or failure
        for future in self._waiting.values():
            if not future.done():
                future.set_exception(self._failure)

    async def _read(self):
        try:
            while True:
                message = json.loads(await self._connection.recv())
                if not isinstance(message, dict):
                    raise TransportError("Native RPC message is not an object")
                if "method" in message:
                    if "id" in message:
                        self.pending_requests[message["id"]] = message
                    else:
                        self.notifications.put_nowait(message)
                elif "id" in message:
                    future = self._waiting.get(message["id"])
                    if future is not None and not future.done():
                        future.set_result(message)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            failure = error if isinstance(error, TransportError) else TransportError("Native RPC connection lost")
            self._fail(failure)

    async def close(self):
        if self._closed:
            return
        self._closed = True
        self._fail(TransportError("RPC connection is closed"))
        if self._reader is not None:
            self._reader.cancel()
            with suppress(asyncio.CancelledError):
                await self._reader
        if self._connection is not None:
            with suppress(Exception):
                await self._connection.close()

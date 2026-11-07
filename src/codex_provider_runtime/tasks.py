"""Provider routing and native task operations shared by every sender."""
from __future__ import annotations

from contextlib import asynccontextmanager
import asyncio
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from .runtime import Endpoint, Runtime, RuntimeFailure
from .state import FileLock, atomic_json, key, locked, read_json
from .transport import RpcError, TransportError


TERMINAL = {"completed", "failed", "interrupted"}
SOURCE_KINDS = ["cli", "vscode", "exec", "appServer", "subAgent", "subAgentReview", "subAgentCompact", "subAgentThreadSpawn", "subAgentOther", "unknown"]


def text_input(text):
    return [{"type": "text", "text": text}]


def name_of(thread):
    return thread.get("name") or ""


def select_name(rows, name):
    exact = [x for x in rows if name_of(x) == name]
    candidates = exact or [x for x in rows if name.casefold() in name_of(x).casefold()]
    if len(candidates) != 1:
        values = [{"id": x["id"], "name": name_of(x), "provider": x.get("modelProvider")} for x in candidates]
        raise RuntimeFailure("Name lookup needs exactly one match: "+json.dumps(values, ensure_ascii=False))
    return candidates[0]


def active_turn(thread):
    return next((x for x in reversed(thread.get("turns", [])) if x.get("status") == "inProgress"), None)


class Tasks:
    def __init__(self, runtime: Runtime):
        self.runtime = runtime

    async def catalogs(self):
        primary = await self.runtime.catalog()
        endpoints = [primary]
        for path in self.runtime.run.glob("*.json"):
            record = read_json(path)
            ep = Endpoint(**{k: record.get(k) for k in Endpoint.__dataclass_fields__})
            if ep.socket != primary.socket:
                endpoints.append(ep)
        return endpoints

    async def loaded_ids(self, c):
        rows, cursor = [], None
        while True:
            page = await c.request("thread/loaded/list", {"cursor": cursor, "limit": 100})
            rows.extend(page.get("data", []))
            cursor = page.get("nextCursor")
            if not cursor:
                return rows

    async def list(self, provider=None, archived=False):
        endpoints = await self.catalogs()
        ep = endpoints[0]
        rows, cursor = [], None
        async with ep.rpc() as c:
            while True:
                r = await c.request("thread/list", {"modelProviders": [provider] if provider else [],
                    "archived": archived, "limit": 100, "cursor": cursor, "sourceKinds": SOURCE_KINDS, "sortKey": "updated_at"})
                rows.extend(r["data"])
                cursor = r.get("nextCursor")
                if not cursor:
                    break
        if not archived:
            # Native catalogs omit a new empty thread until its first user turn.
            # The owning server can enumerate it without starting inference.
            by_id = {x["id"]: x for x in rows}
            for endpoint in endpoints:
                try:
                    async with endpoint.rpc(timeout=3) as c:
                        for thread_id in await self.loaded_ids(c):
                            row = (await c.request("thread/read", {"threadId": thread_id, "includeTurns": False}))["thread"]
                            if not provider or row.get("modelProvider") == provider:
                                by_id[thread_id] = row
                except (TransportError, RpcError, OSError):
                    continue
            # Empty threads remain absent from native lists after their server exits.
            # Keep only their IDs locally; read title/provider from native metadata.
            async with ep.rpc() as c:
                for path in (self.runtime.state/"initial").glob("*.json"):
                    initial = read_json(path)
                    thread_id = initial.get("threadId")
                    if not thread_id or thread_id in by_id:
                        continue
                    try:
                        row = (await c.request("thread/read", {"threadId": thread_id, "includeTurns": False}))["thread"]
                    except RpcError:
                        continue
                    native_path = row.get("path")
                    if (native_path and Path(native_path).resolve().is_relative_to(self.runtime.home/"sessions")
                            and (not provider or row.get("modelProvider") == provider)):
                        by_id[thread_id] = row
            rows = sorted(by_id.values(), key=lambda x:x.get("updatedAt", 0), reverse=True)
        return rows

    async def resolve(self, thread=None, name=None, provider=None):
        if name:
            row = select_name(await self.list(), name)
        elif thread:
            error = None
            for ep in await self.catalogs():
                try:
                    async with ep.rpc() as c:
                        row = (await c.request("thread/read", {"threadId": thread, "includeTurns": False}))["thread"]
                    break
                except (TransportError, RpcError, OSError) as caught:
                    error = caught
            else:
                raise RuntimeFailure(f"Cannot read thread {thread}: {error}")
        else:
            raise RuntimeFailure("Specify --thread ID or --name TITLE")
        actual = row.get("modelProvider")
        if not actual:
            raise RuntimeFailure("Thread metadata has no provider ID; refusing to guess")
        if provider and provider != actual:
            raise RuntimeFailure(f"Provider conflict: thread belongs to {actual}, requested {provider}")
        return row

    @asynccontextmanager
    async def connection(self, row, *, start=False):
        ep = await self.runtime.endpoint(row["modelProvider"], start=start)
        async with ep.rpc() as c:
            yield ep, c

    async def read(self, row):
        async with self.connection(row) as (_, c):
            return (await c.request("thread/read", {"threadId": row["id"], "includeTurns": True}))["thread"]

    async def turns(self, c, thread_id):
        rows, cursor = [], None
        while True:
            response = await c.request("thread/turns/list", {"threadId": thread_id, "limit": 100,
                "itemsView": "full", "cursor": cursor})
            rows.extend(response["data"])
            cursor = response.get("nextCursor")
            if not cursor:
                return rows

    def binding_path(self, thread):
        return self.runtime.state/"contexts"/(key(thread)+".json")

    def context(self, thread):
        return read_json(self.binding_path(thread), {})

    def initial_path(self, thread):
        return self.runtime.state/"initial"/(key(thread)+".json")

    async def settings(self, c, thread_id):
        current = (await c.request("thread/read", {"threadId": thread_id, "includeTurns": False}))["thread"]
        if current.get("model") is not None:
            return current
        # Before the first user turn, native storage has no model/effort record.
        page = await c.request("thread/turns/list", {"threadId": thread_id, "limit": 1})
        if not page.get("data"):
            initial = read_json(self.initial_path(thread_id), {})
            return {**current, "model": initial.get("model"), "reasoningEffort": initial.get("effort")}
        return current

    def save_context(self, thread, config):
        if config:
            atomic_json(self.binding_path(thread), config)

    async def _resume(self, c, row, *, model=None, effort=None, context=None):
        config = self.context(row["id"])
        loaded = await self.loaded_ids(c)
        current = await self.settings(c, row["id"])
        if row["id"] in loaded and not model and not effort and not context:
            # Native resume can reset an empty loaded thread's model to defaults.
            # An existing in-memory session already holds its launch configuration.
            return {"thread": current, "model": current.get("model"),
                "reasoningEffort": current.get("reasoningEffort"), "alreadyLoaded": True}
        if context:
            if row["id"] in loaded and any(config.get(k) != v for k,v in context.items()):
                raise RuntimeFailure("A loaded thread cannot reliably change its context limit; fork with the new limit")
            config = {**config, **context}
        params = {"threadId": row["id"], "excludeTurns": True,
            "approvalPolicy": "never", "sandbox": "danger-full-access"}
        model = model or current.get("model")
        effort = effort or current.get("reasoningEffort")
        if model:
            params["model"] = model
        if effort:
            config = {**config, "model_reasoning_effort": effort}
        if config:
            params["config"] = config
        result = await c.request("thread/resume", params)
        self.save_context(row["id"], {k:v for k,v in config.items() if k in {"model_context_window", "model_auto_compact_token_limit"}})
        return result

    async def resume(self, row, *, model=None, effort=None, context=None):
        async with self.connection(row, start=True) as (ep, c):
            result = await self._resume(c, row, model=model, effort=effort, context=context)
            return {"threadId": row["id"], "provider": ep.provider,
                "model": result.get("model"), "effort": result.get("reasoningEffort"),
                "context": self.context(row["id"]), "resumed": True}

    async def create(self, *, provider=None, source=None, title=None, cwd=None, model=None, effort=None, context=None):
        provider = provider or (source.get("modelProvider") if source else self.runtime.default_provider)
        ep = await self.runtime.endpoint(provider)
        config = dict(self.context(source["id"]) if source else {})
        config.update(context or {})
        if effort:
            config["model_reasoning_effort"] = effort
        params = {"approvalPolicy": "never", "sandbox": "danger-full-access", "modelProvider": provider}
        if config:
            params["config"] = config
        if model:
            params["model"] = model
        if cwd:
            params["cwd"] = str(Path(cwd).absolute())
        if source:
            # Fork at a completed turn boundary; do not fork a moving history.
            source_ep = await self.runtime.endpoint(source["modelProvider"], start=False)
            async with source_ep.rpc() as sc:
                current = (await sc.request("thread/read", {"threadId": source["id"], "includeTurns": True}))["thread"]
                if active_turn(current):
                    raise RuntimeFailure("Fork requires a completed turn boundary; wait or interrupt first")
                settings = await self.settings(sc, source["id"])
                if not model and settings.get("model"):
                    params["model"] = settings["model"]
                if not effort and settings.get("reasoningEffort"):
                    config["model_reasoning_effort"] = settings["reasoningEffort"]
                    params["config"] = config
            params.update({"threadId": source["id"], "excludeTurns": True})
        async with ep.rpc() as c:
            result = await c.request("thread/fork" if source else "thread/start", params)
            row = result["thread"]
            if title:
                await c.request("thread/name/set", {"threadId": row["id"], "name": title})
        self.save_context(row["id"], {k:v for k,v in config.items() if k in {"model_context_window", "model_auto_compact_token_limit"}})
        atomic_json(self.initial_path(row["id"]), {"threadId": row["id"], "model": result.get("model"), "effort": result.get("reasoningEffort")})
        return {"threadId": row["id"], "provider": ep.provider, "model": result.get("model"),
            "effort": result.get("reasoningEffort"), "context": self.context(row["id"]), "created": True}

    async def send(self, row, message, *, mode="followup", model=None, effort=None, message_id=None):
        message_id = message_id or str(uuid.uuid4())
        async with self.connection(row, start=True) as (ep, c):
            await self._resume(c, row)
            current = (await c.request("thread/read", {"threadId": row["id"], "includeTurns": True}))["thread"]
            active = active_turn(current)
            delivery = "steer" if mode == "followup" and active else mode
            if delivery == "followup":
                delivery = "start"
            if delivery == "queue":
                return await self._queue(ep, row, message, model, effort)
            if delivery == "steer":
                if not active:
                    raise RuntimeFailure("Steer requires an active turn")
                if model or effort:
                    raise RuntimeFailure("Steer cannot change model or effort; wait for idle and start")
                response = await c.request("turn/steer", {"threadId": row["id"],
                    "expectedTurnId": active["id"], "input": text_input(message), "clientUserMessageId": message_id})
                turn_id = response["turnId"]
            elif delivery == "start":
                if active:
                    raise RuntimeFailure("Start requires an idle thread; use followup, steer or queue")
                params = {"threadId": row["id"], "input": text_input(message), "clientUserMessageId": message_id}
                if model:
                    params["model"] = model
                if effort:
                    params["effort"] = effort
                response = await c.request("turn/start", params)
                turn_id = response["turn"]["id"]
            else:
                raise RuntimeFailure("Unknown delivery mode")
            return {"threadId": row["id"], "provider": ep.provider, "turnId": turn_id,
                "delivery": delivery, "clientUserMessageId": message_id, "accepted": True}

    async def _queue(self, ep, row, message, model, effort):
        if model or effort:
            raise RuntimeFailure("Native queue uses the thread model; change it in an idle start/resume first")
        command = [ep.binary, "queue", "--remote", "unix://"+ep.socket, "--thread", row["id"], "--message", message]
        process = await asyncio.create_subprocess_exec(*command, env={**os.environ, "CODEX_HOME": str(self.runtime.home)},
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
        try:
            stdout, stderr = await asyncio.wait_for(process.communicate(), 30)
        except asyncio.TimeoutError:
            process.kill()
            await process.wait()
            raise TransportError("Queue delivery timed out; inspect the native queue before retrying")
        if process.returncode:
            raise RuntimeFailure("Native queue rejected delivery: "+stderr.decode(errors="replace")[-1200:])
        return {"threadId": row["id"], "provider": ep.provider, "delivery": "queue", "accepted": True,
            "queueReceipt": stdout.decode(errors="replace").strip(), "turnId": None}

    async def wait(self, row, turn_id=None, timeout=3600):
        deadline = time.monotonic()+timeout if timeout else None
        reconnect_error = None
        while deadline is None or time.monotonic() < deadline:
            try:
                async with self.connection(row) as (_, c):
                    while deadline is None or time.monotonic() < deadline:
                        turns = await self.turns(c, row["id"])
                        if not turn_id:
                            # Native pages are newest-first.
                            if not turns:
                                return {"threadId": row["id"], "status": "idle", "turnId": None}
                            turn_id = turns[0]["id"]
                        turn = next((t for t in turns if t["id"] == turn_id), None)
                        if turn and turn.get("status") in TERMINAL:
                            return {"threadId": row["id"], "provider": row["modelProvider"], "turnId": turn_id,
                                "status": turn["status"], "turn": turn}
                        if c.pending_requests:
                            return {"threadId": row["id"], "turnId": turn_id, "status": "waitingForInput",
                                "requests": [{"id": v.get("id"), "method": v.get("method")} for v in c.pending_requests.values()]}
                        try:
                            await asyncio.wait_for(c.notifications.get(), timeout=.5)
                        except asyncio.TimeoutError:
                            pass
            except (TransportError, OSError) as error:
                reconnect_error = str(error)
                await asyncio.sleep(.5)
        return {"threadId": row["id"], "turnId": turn_id, "status": "timeout", "lastConnectionError": reconnect_error}

    async def interrupt(self, row, turn_id=None):
        async with self.connection(row) as (_, c):
            current = (await c.request("thread/read", {"threadId": row["id"], "includeTurns": True}))["thread"]
            active = active_turn(current)
            if not active or (turn_id and turn_id != active["id"]):
                raise RuntimeFailure("No matching active turn to interrupt")
            await c.request("turn/interrupt", {"threadId": row["id"], "turnId": active["id"]})
            return {"threadId": row["id"], "turnId": active["id"], "interruptRequested": True}

    async def mutate(self, request_id, description, operation):
        request_id = request_id or str(uuid.uuid4())
        path = self.runtime.state/"receipts"/(key(request_id)+".json")
        fingerprint = hashlib.sha256(json.dumps(description, sort_keys=True, ensure_ascii=False).encode()).hexdigest()
        async with locked(path.with_suffix(".lock"), timeout=60):
            previous = read_json(path)
            if previous:
                if previous["fingerprint"] != fingerprint:
                    raise RuntimeFailure("Request ID already belongs to a different operation")
                if previous["state"] == "succeeded":
                    return {**previous["result"], "requestId": request_id, "reusedReceipt": True}
                raise RuntimeFailure(f"Request {request_id} is {previous['state']}; inspect thread/history and receipt {path}, do not blindly resend")
            record = {"requestId": request_id, "fingerprint": fingerprint, "state": "pending", "started": time.time()}
            atomic_json(path, record)
            try:
                result = await operation()
            except BaseException as error:
                record.update(state="uncertain" if isinstance(error, (TransportError, asyncio.CancelledError)) else "failed", error=str(error))
                atomic_json(path, record)
                raise
            result = {**result, "requestId": request_id}
            atomic_json(path, {**record, "state": "succeeded", "result": result})
            return result

    async def reply(self, destination, source, turn_id, *, request_id=None):
        result = await self.wait(source, turn_id, timeout=1)
        if result["status"] not in TERMINAL:
            raise RuntimeFailure("Source turn has not reached a terminal state")
        messages = [x.get("text", "") for x in result["turn"].get("items", []) if x.get("type") == "agentMessage"]
        text = f"[Codex task result: provider={source['modelProvider']}, thread={source['id']}, turn={turn_id}, status={result['status']}]\n"+"\n".join(messages)
        receipt = await self.mutate(request_id, {"command": "reply", "destination": destination["id"], "source": source["id"], "turn": turn_id},
            lambda: self.send(destination, text))
        # Confirm the native turn containing the injected message can be read back.
        async with self.connection(destination) as (_, c):
            verified = False
            for _ in range(10):
                turns = await self.turns(c, destination["id"])
                found = next((x for x in turns if x["id"] == receipt["turnId"]), None)
                if found:
                    contents = [part.get("text", "") for item in found.get("items", [])
                        if item.get("type") == "userMessage" for part in item.get("content", [])]
                    verified = text in contents
                if verified:
                    break
                await asyncio.sleep(.1)
        return {**receipt, "sourceStatus": result["status"], "replyVerified": verified,
            "newTurnStarted": receipt["delivery"] == "start" and verified}

    def launch_watcher(self, source, turn_id, destination, request_id, timeout=0):
        job_id = key(request_id+destination["id"])
        path = self.runtime.state/"watches"/(job_id+".json")
        with FileLock(path.with_suffix(".launch.lock")):
            previous = read_json(path)
            if previous:
                return {"watchId": job_id, "watchState": previous["state"], "watchFile": str(path)}
            atomic_json(path, {"state": "pending", "source": source, "destination": destination,
                "turnId": turn_id, "requestId": request_id+":reply", "timeout": timeout,
                "home": str(self.runtime.home), "binary": self.runtime.binary, "appSocket": self.runtime.app_socket})
            kwargs = {"start_new_session": True} if os.name != "nt" else {"creationflags": subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP}
            source_root = Path(__file__).resolve().parents[2]
            env = {**os.environ, "PYTHONPATH": os.pathsep.join([str(source_root/"src"), str(source_root/"runtime-deps")])}
            with path.with_suffix(".log").open("ab") as log:
                process = subprocess.Popen([sys.executable, "-m", "codex_provider_runtime.cli", "_watch", str(path)],
                    env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=log, **kwargs)
            return {"watchId": job_id, "watchState": "pending", "watchPid": process.pid, "watchFile": str(path)}


async def run_watcher(path: Path):
    async with locked(path.with_suffix(".lock"), timeout=1):
        job = read_json(path)
        if job["state"] == "completed":
            return job
        job.update(state="running", pid=os.getpid())
        atomic_json(path, job)
        try:
            tasks = Tasks(Runtime(job["home"], job["binary"], job["appSocket"]))
            result = await tasks.wait(job["source"], job["turnId"], job["timeout"])
            if result["status"] not in TERMINAL:
                job.update(state="waiting" if result["status"] == "waitingForInput" else "failed", result=result)
            else:
                receipt = await tasks.reply(job["destination"], job["source"], job["turnId"], request_id=job["requestId"])
                job.update(state="completed", result=receipt)
        except Exception as error:
            job.update(state="failed", error=str(error))
        atomic_json(path, job)
        return job

"""Provider routing and native task operations shared by every sender."""
from __future__ import annotations

from contextlib import asynccontextmanager, suppress
import asyncio
import hashlib
import json
import os
from pathlib import Path
import subprocess
import sys
import time
import uuid

from .inbox import CursorStore, InboxDoorbell, InboxStore, match_markers, validate_name
from .runtime import Endpoint, Runtime, RuntimeFailure
from .state import FileLock, atomic_json, key, locked, read_json
from .transport import RpcError, TransportError


TERMINAL = {"completed", "failed", "interrupted"}
SOURCE_KINDS = ["cli", "vscode", "exec", "appServer", "subAgent", "subAgentReview", "subAgentCompact", "subAgentThreadSpawn", "subAgentOther", "unknown"]
_INBOX_ERROR_KINDS = frozenset({"blocked", "error", "failed"})
_INBOX_WARNING_KINDS = frozenset({"decision", "policy_decision", "warning", "escalation", "needs_input"})


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

    @property
    def _run_dir(self) -> Path:
        return getattr(self.runtime, "run", self.runtime.state / "run")

    @property
    def inboxes(self) -> InboxStore:
        return InboxStore(self.runtime.state, self._run_dir)

    @property
    def cursors(self) -> CursorStore:
        return CursorStore(self.runtime.state, self._run_dir)

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

    async def recent_turns(self, c, thread_id, *, stop_at=None, page_size=20, single_page=False):
        """Fetch newest-first turns, stopping as soon as ``stop_at`` appears."""
        rows, cursor = [], None
        while True:
            response = await c.request("thread/turns/list", {"threadId": thread_id, "limit": page_size,
                "itemsView": "full", "cursor": cursor})
            page = response.get("data", [])
            rows.extend(page)
            if single_page or (stop_at is not None and any(t.get("id") == stop_at for t in page)):
                return rows
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

    async def _anchor_consumer_before_send(self, c, thread_id, current, *, delivery, consumer):
        if not consumer:
            return
        turns = current.get("turns", [])
        if delivery == "steer" or (delivery == "queue" and active_turn(current)):
            active = active_turn(current)
            active_id = active["id"]
            baseline = None
            for idx, turn in enumerate(turns):
                if turn.get("id") == active_id:
                    baseline = turns[idx - 1]["id"] if idx > 0 else None
                    break
            recent = await self.recent_turns(c, thread_id, stop_at=active_id, page_size=5)
            active_full = next((t for t in recent if t.get("id") == active_id), active)
            seen_count = sum(1 for item in active_full.get("items", []) if item.get("type") == "agentMessage")
            await self.cursors.anchor_thread(
                consumer, thread_id, baseline_turn_id=baseline,
                acked_state=None, seen_item_counts={active_id: seen_count},
            )
        else:
            prev = turns[-1] if turns else None
            baseline = prev["id"] if prev else None
            acked = {"turnId": prev["id"], "status": prev.get("status")} if prev else None
            await self.cursors.anchor_thread(
                consumer, thread_id, baseline_turn_id=baseline, acked_state=acked,
            )

    async def send(self, row, message, *, mode="followup", model=None, effort=None, message_id=None, consumer=None):
        message_id = message_id or str(uuid.uuid4())
        async with self.connection(row, start=True) as (ep, c):
            await self._resume(c, row)
            current = (await c.request("thread/read", {"threadId": row["id"], "includeTurns": True}))["thread"]
            active = active_turn(current)
            delivery = "steer" if mode == "followup" and active else mode
            if delivery == "followup":
                delivery = "start"
            if delivery == "queue":
                if model or effort:
                    raise RuntimeFailure("Native queue uses the thread model; change it in an idle start/resume first")
                await self._anchor_consumer_before_send(c, row["id"], current, delivery="queue", consumer=consumer)
                return await self._queue(ep, row, message, model, effort)
            if delivery == "steer":
                if not active:
                    raise RuntimeFailure("Steer requires an active turn")
                if model or effort:
                    raise RuntimeFailure("Steer cannot change model or effort; wait for idle and start")
                await self._anchor_consumer_before_send(c, row["id"], current, delivery="steer", consumer=consumer)
                response = await c.request("turn/steer", {"threadId": row["id"],
                    "expectedTurnId": active["id"], "input": text_input(message), "clientUserMessageId": message_id})
                turn_id = response["turnId"]
            elif delivery == "start":
                if active:
                    raise RuntimeFailure("Start requires an idle thread; use followup, steer or queue")
                await self._anchor_consumer_before_send(c, row["id"], current, delivery="start", consumer=consumer)
                params = {"threadId": row["id"], "input": text_input(message), "clientUserMessageId": message_id}
                if model:
                    params["model"] = model
                if effort:
                    params["effort"] = effort
                response = await c.request("turn/start", params)
                turn_id = response["turn"]["id"]
            else:
                raise RuntimeFailure("Unknown delivery mode")
            result = {"threadId": row["id"], "provider": ep.provider, "turnId": turn_id,
                "delivery": delivery, "clientUserMessageId": message_id, "accepted": True}
            if consumer:
                result["consumerAnchored"] = consumer
            return result

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
                        turns = await self.recent_turns(c, row["id"], stop_at=turn_id, single_page=turn_id is None)
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

    async def wait_multi(
        self,
        rows: list[dict],
        *,
        inboxes: list[str] | None = None,
        inbox_kinds: list[str] | None = None,
        markers: list[str] | None = None,
        marker_mode: str = "line",
        consumer: str | None = None,
        no_advance: bool = False,
        timeout: float = 3600,
        idle_timeout: float = 0,
        disconnect_grace: float = 15.0,
        poll_interval: float = 0.5,
        commit_callbacks: list | None = None,
    ) -> dict:
        unique_rows: list[dict] = []
        seen_tids: set[str] = set()
        for row in rows:
            if row["id"] not in seen_tids:
                seen_tids.add(row["id"])
                unique_rows.append(row)
        channels = list(dict.fromkeys(validate_name(ch, "channel") for ch in (inboxes or [])))
        marker_list = [m for m in (markers or []) if m]
        deadline = time.monotonic() + timeout if timeout else None

        @asynccontextmanager
        async def maybe_lease():
            if consumer:
                async with self.cursors.lease(consumer):
                    yield
            else:
                yield

        @asynccontextmanager
        async def maybe_doorbell():
            if channels:
                async with InboxDoorbell(self._run_dir, self.runtime.state, channels) as bell:
                    yield bell
            else:
                yield None

        async with maybe_lease():
            working_cursor = self.cursors.load(consumer) if consumer else {"consumer": None, "inboxes": {}, "threads": {}}
            thread_states: dict[str, dict] = {}
            for row in unique_rows:
                tid = row["id"]
                existing = working_cursor["threads"].get(tid)
                if isinstance(existing, dict):
                    state = {
                        "baselineTurnId": existing.get("baselineTurnId"),
                        "seenItemCounts": dict(existing.get("seenItemCounts") or {}),
                        "ackedState": existing.get("ackedState"),
                        "disconnected": bool(existing.get("disconnected", False)),
                    }
                else:
                    state = {
                        "_uninitialized": True,
                        "baselineTurnId": None,
                        "seenItemCounts": {},
                        "ackedState": None,
                        "disconnected": False,
                    }
                thread_states[tid] = state
                working_cursor["threads"][tid] = state

            events: list[dict] = []
            seen_event_ids: set[str] = set()
            thread_snapshots: dict[str, dict] = {}
            idle_since: dict[str, tuple[float | None, str | None]] = {}
            wake_event = asyncio.Event()

            def add_event(event: dict):
                if event["id"] not in seen_event_ids:
                    seen_event_ids.add(event["id"])
                    events.append(event)
                    wake_event.set()

            def remaining_time() -> float | None:
                if deadline is None:
                    return None
                return max(0.0, deadline - time.monotonic())

            def scan_inboxes() -> bool:
                found = False
                for ch in channels:
                    cur_seq = int((working_cursor["inboxes"].get(ch) or {}).get("lastSeq") or 0)
                    matched, high_seq = self.inboxes.read(ch, after_seq=cur_seq, kinds=inbox_kinds or None)
                    if matched:
                        for rec in matched:
                            ikind = rec.get("kind", "info")
                            if ikind in _INBOX_ERROR_KINDS:
                                severity = "error"
                            elif ikind in _INBOX_WARNING_KINDS:
                                severity = "warning"
                            else:
                                severity = "info"
                            ev = {
                                "id": rec["id"],
                                "source": "inbox",
                                "kind": "inbox_message",
                                "inboxKind": ikind,
                                "severity": severity,
                                "ts": rec["ts"],
                                "channel": ch,
                                "seq": rec["seq"],
                                "summary": rec["summary"],
                            }
                            for opt in ("sender", "threadId", "phase", "detailsPath", "payload"):
                                if opt in rec:
                                    ev[opt] = rec[opt]
                            add_event(ev)
                            found = True
                        working_cursor["inboxes"][ch] = {"lastSeq": max(cur_seq, high_seq)}
                return found

            def evaluate_thread(row: dict, turns: list[dict], pending_requests: dict, provider_thread_count: int):
                tid = row["id"]
                provider = row["modelProvider"]
                state = thread_states[tid]
                latest = turns[0] if turns else None
                latest_id = latest["id"] if latest else None
                latest_status = latest.get("status") if latest else "idle"
                thread_snapshots[tid] = {"provider": provider, "status": latest_status, "latestTurnId": latest_id}

                if state.pop("_uninitialized", False):
                    if not turns:
                        state["baselineTurnId"] = None
                    elif latest_status == "inProgress":
                        state["baselineTurnId"] = turns[1]["id"] if len(turns) > 1 else None
                    elif consumer or marker_list:
                        state["baselineTurnId"] = latest_id
                        state["ackedState"] = {"turnId": latest_id, "status": latest_status}
                    else:
                        state["baselineTurnId"] = turns[1]["id"] if len(turns) > 1 else None

                baseline = state.get("baselineTurnId")
                newer: list[dict] = []
                for turn in turns:
                    if baseline is not None and turn.get("id") == baseline:
                        break
                    newer.append(turn)
                candidates = list(reversed(newer))

                matched_reqs = []
                for req in pending_requests.values():
                    req_tid = (req.get("params") or {}).get("threadId")
                    if req_tid == tid or (req_tid is None and (latest_status == "inProgress" or provider_thread_count == 1)):
                        matched_reqs.append({"id": req.get("id"), "method": req.get("method")})
                if matched_reqs:
                    req_sig = {"turnId": latest_id, "status": "waitingForInput", "requestIds": sorted(str(r["id"]) for r in matched_reqs)}
                    if state.get("ackedState") != req_sig:
                        add_event({
                            "id": f"thr:{tid}:{latest_id or 'none'}:waiting:{','.join(req_sig['requestIds'])}",
                            "source": "thread",
                            "kind": "thread_waiting_for_input",
                            "severity": "error",
                            "ts": time.time(),
                            "threadId": tid,
                            "provider": provider,
                            "turnId": latest_id,
                            "summary": f"Thread waiting for input ({len(matched_reqs)} pending request(s))",
                            "payload": {"requests": matched_reqs},
                        })
                        state["ackedState"] = req_sig

                for turn in candidates:
                    turn_id = turn["id"]
                    status = turn.get("status")
                    agent_msgs = [it for it in turn.get("items", []) if it.get("type") == "agentMessage"]
                    if marker_list:
                        seen_counts = state.setdefault("seenItemCounts", {})
                        prev_count = int(seen_counts.get(turn_id, 0))
                        for idx, item in enumerate(agent_msgs[prev_count:], start=prev_count):
                            text = item.get("text", "")
                            for hit_idx, (m_token, m_line) in enumerate(match_markers(text, marker_list, mode=marker_mode)):
                                add_event({
                                    "id": f"thr:{tid}:{turn_id}:item:{idx}:m:{hit_idx}",
                                    "source": "thread",
                                    "kind": "thread_marker",
                                    "severity": "info",
                                    "ts": time.time(),
                                    "threadId": tid,
                                    "provider": provider,
                                    "turnId": turn_id,
                                    "summary": m_line,
                                    "payload": {"marker": m_token, "line": m_line, "text": text, "itemIndex": idx, "turnStatus": status},
                                })
                        seen_counts[turn_id] = len(agent_msgs)

                    if status in {"failed", "interrupted"}:
                        sig = {"turnId": turn_id, "status": status}
                        if state.get("ackedState") != sig:
                            add_event({
                                "id": f"thr:{tid}:{turn_id}:{status}",
                                "source": "thread",
                                "kind": f"thread_{status}",
                                "severity": "error",
                                "ts": time.time(),
                                "threadId": tid,
                                "provider": provider,
                                "turnId": turn_id,
                                "summary": f"Turn {turn_id} {status}",
                                "payload": {"turn": turn},
                            })
                            state["ackedState"] = sig
                    elif status == "completed" and not marker_list:
                        sig = {"turnId": turn_id, "status": "completed"}
                        if state.get("ackedState") != sig:
                            last_text = agent_msgs[-1].get("text", "") if agent_msgs else ""
                            add_event({
                                "id": f"thr:{tid}:{turn_id}:completed",
                                "source": "thread",
                                "kind": "thread_completed",
                                "severity": "info",
                                "ts": time.time(),
                                "threadId": tid,
                                "provider": provider,
                                "turnId": turn_id,
                                "summary": last_text[:200] if last_text else f"Turn {turn_id} completed",
                                "payload": {"turn": turn},
                            })
                            state["ackedState"] = sig

                    if status in TERMINAL:
                        state["baselineTurnId"] = turn_id
                        state.get("seenItemCounts", {}).pop(turn_id, None)

                is_active = latest_status == "inProgress"
                prev_idle = idle_since.get(tid)
                if is_active:
                    idle_since[tid] = (None, latest_id)
                elif prev_idle is None or prev_idle[1] != latest_id or prev_idle[0] is None:
                    idle_since[tid] = (time.monotonic(), latest_id)
                elif idle_timeout > 0:
                    elapsed = time.monotonic() - prev_idle[0]
                    if elapsed >= idle_timeout:
                        idle_sig = {"turnId": latest_id, "status": "idleTimeout"}
                        if state.get("ackedState") != idle_sig:
                            add_event({
                                "id": f"thr:{tid}:{latest_id or 'empty'}:idle_timeout",
                                "source": "thread",
                                "kind": "thread_idle_timeout",
                                "severity": "warning",
                                "ts": time.time(),
                                "threadId": tid,
                                "provider": provider,
                                "turnId": latest_id,
                                "summary": f"Thread idle for {elapsed:.1f}s without new turn",
                            })
                            state["ackedState"] = idle_sig

            async def watch_inboxes(bell: InboxDoorbell):
                if scan_inboxes():
                    return
                while not wake_event.is_set():
                    rem = remaining_time()
                    if rem is not None and rem <= 0:
                        return
                    step = min(5.0, rem) if rem is not None else 5.0
                    await bell.wait(timeout=step)
                    if scan_inboxes():
                        return

            async def watch_provider_group(provider: str, group_rows: list[dict]):
                disconnected_since: float | None = None
                while not wake_event.is_set():
                    rem = remaining_time()
                    if rem is not None and rem <= 0:
                        return
                    try:
                        ep = await self.runtime.endpoint(provider, start=False)
                        async with ep.rpc() as c:
                            while not wake_event.is_set():
                                rem = remaining_time()
                                if rem is not None and rem <= 0:
                                    return
                                for row in group_rows:
                                    st = thread_states[row["id"]]
                                    stop_at = None if st.get("_uninitialized") else st.get("baselineTurnId")
                                    try:
                                        turns = await self.recent_turns(
                                            c, row["id"], stop_at=stop_at,
                                            single_page=bool(st.get("_uninitialized")),
                                        )
                                    except RpcError as rpc_err:
                                        err_sig = {"turnId": None, "status": "rpcError", "message": str(rpc_err)}
                                        thread_snapshots[row["id"]] = {
                                            "provider": provider,
                                            "status": "error",
                                            "latestTurnId": thread_snapshots.get(row["id"], {}).get("latestTurnId"),
                                        }
                                        if st.get("ackedState") != err_sig:
                                            add_event({
                                                "id": f"thr:{row['id']}:rpc_error:{key(str(rpc_err))[:8]}",
                                                "source": "thread",
                                                "kind": "thread_error",
                                                "severity": "error",
                                                "ts": time.time(),
                                                "threadId": row["id"],
                                                "provider": provider,
                                                "summary": str(rpc_err),
                                            })
                                            st["ackedState"] = err_sig
                                        continue
                                    if disconnected_since is not None or st.get("disconnected"):
                                        disconnected_since = None
                                        if st.get("disconnected"):
                                            st["disconnected"] = False
                                            add_event({
                                                "id": f"thr:{row['id']}:recovered:{int(time.time() * 1000)}",
                                                "source": "thread",
                                                "kind": "thread_recovered",
                                                "severity": "info",
                                                "ts": time.time(),
                                                "threadId": row["id"],
                                                "provider": provider,
                                                "summary": f"Reconnected to {provider} endpoint",
                                            })
                                    evaluate_thread(row, turns, c.pending_requests, len(group_rows))
                                if wake_event.is_set():
                                    return
                                wait_for_tick = poll_interval if rem is None else min(poll_interval, rem)
                                if wait_for_tick <= 0:
                                    return
                                try:
                                    await asyncio.wait_for(c.notifications.get(), timeout=wait_for_tick)
                                    while not c.notifications.empty():
                                        c.notifications.get_nowait()
                                except asyncio.TimeoutError:
                                    pass
                    except (TransportError, RuntimeFailure, OSError) as error:
                        now = time.monotonic()
                        disconnected_since = disconnected_since or now
                        if now - disconnected_since >= disconnect_grace:
                            for row in group_rows:
                                st = thread_states[row["id"]]
                                thread_snapshots[row["id"]] = {
                                    "provider": provider,
                                    "status": "disconnected",
                                    "latestTurnId": thread_snapshots.get(row["id"], {}).get("latestTurnId"),
                                }
                                if not st.get("disconnected"):
                                    st["disconnected"] = True
                                    add_event({
                                        "id": f"thr:{row['id']}:disconnected:{int(disconnected_since * 1000)}",
                                        "source": "thread",
                                        "kind": "thread_disconnected",
                                        "severity": "error",
                                        "ts": time.time(),
                                        "threadId": row["id"],
                                        "provider": provider,
                                        "summary": str(error),
                                    })
                            if wake_event.is_set():
                                return
                        rem = remaining_time()
                        if rem is not None and rem <= 0:
                            return
                        await asyncio.sleep(min(poll_interval, rem) if rem is not None else poll_interval)

            async with maybe_doorbell() as bell:
                watcher_tasks: list[asyncio.Task] = []
                if bell is not None:
                    watcher_tasks.append(asyncio.create_task(watch_inboxes(bell), name="wait-inboxes"))
                by_provider: dict[str, list[dict]] = {}
                for row in unique_rows:
                    by_provider.setdefault(row["modelProvider"], []).append(row)
                for provider, group_rows in by_provider.items():
                    watcher_tasks.append(asyncio.create_task(
                        watch_provider_group(provider, group_rows), name=f"wait-provider-{provider}",
                    ))
                try:
                    rem = remaining_time()
                    if rem is None:
                        await wake_event.wait()
                    elif rem > 0:
                        with suppress(asyncio.TimeoutError):
                            await asyncio.wait_for(wake_event.wait(), timeout=rem)
                    if wake_event.is_set() and channels:
                        scan_inboxes()
                finally:
                    for task in watcher_tasks:
                        task.cancel()
                    for task in watcher_tasks:
                        with suppress(asyncio.CancelledError):
                            await task

            for st in working_cursor["threads"].values():
                st.pop("_uninitialized", None)

            def commit():
                if consumer and not no_advance and events:
                    self.cursors.save_sync(consumer, working_cursor)

            if events:
                severities = {e.get("severity", "info") for e in events}
                worst = "error" if "error" in severities else ("warning" if "warning" in severities else "info")
                if commit_callbacks is not None:
                    commit_callbacks.append(commit)
                else:
                    commit()
                return {
                    "status": "events",
                    "worst": worst,
                    "events": events,
                    "threads": thread_snapshots,
                    "cursor": {"consumer": consumer, "advanced": bool(consumer and not no_advance)},
                }
            return {
                "status": "timeout",
                "worst": "info",
                "events": [],
                "threads": thread_snapshots,
                "cursor": {"consumer": consumer, "advanced": False},
            }

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

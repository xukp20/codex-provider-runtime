import asyncio
from contextlib import asynccontextmanager
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import sys
import time
from types import SimpleNamespace
from unittest.mock import patch

import pytest

from codex_provider_runtime import cli, inbox as inbox_mod
from codex_provider_runtime.inbox import ConsumerBusyError, InboxDoorbell, InboxStore, doorbell_prefix, match_markers, poke_doorbells
from codex_provider_runtime.runtime import RuntimeFailure
from codex_provider_runtime.tasks import Tasks
from codex_provider_runtime.transport import RpcError, TransportError


class FakeRpc:
    def __init__(self, state_by_thread, *, pending_requests=None, fail_connect=None, page_size_cap=None):
        self.state_by_thread = state_by_thread
        self.pending_requests = pending_requests if pending_requests is not None else {}
        self.notifications = asyncio.Queue()
        self.fail_connect = fail_connect
        self.page_size_cap = page_size_cap

    async def request(self, method, params, timeout=None):
        if method == "thread/turns/list":
            tid = params["threadId"]
            turns = list(self.state_by_thread[tid])  # newest-first
            if callable(turns[0]) if turns else False:
                raise turns[0]()
            limit = params.get("limit", 20)
            if self.page_size_cap is not None:
                limit = min(limit, self.page_size_cap)
            start = int(params.get("cursor") or 0)
            chunk = turns[start:start + limit]
            next_cursor = str(start + limit) if start + limit < len(turns) else None
            return {"data": chunk, "nextCursor": next_cursor}
        if method == "thread/read":
            tid = params["threadId"]
            turns = list(reversed(self.state_by_thread[tid]))  # oldest-first
            return {"thread": {"id": tid, "turns": turns}}
        if method == "thread/loaded/list":
            return {"data": list(self.state_by_thread.keys()), "nextCursor": None}
        if method == "turn/start":
            tid = params["threadId"]
            new_id = f"turn_{len(self.state_by_thread[tid]) + 1}"
            self.state_by_thread[tid].insert(0, {"id": new_id, "status": "inProgress", "items": []})
            return {"turn": {"id": new_id}}
        if method == "turn/steer":
            return {"turnId": params["expectedTurnId"]}
        raise RpcError(method, {"message": f"Unhandled fake method {method}"})


class FakeEndpoint:
    def __init__(self, provider, rpc_factory):
        self.provider = provider
        self.rpc_factory = rpc_factory
        self.open_count = 0

    @asynccontextmanager
    async def rpc(self, timeout=15):
        self.open_count += 1
        conn = self.rpc_factory()
        if isinstance(conn, BaseException):
            raise conn
        yield conn


class FakeRuntime:
    def __init__(self, tmp_path, endpoints):
        self.state = tmp_path / "state"
        self.run = tmp_path / "run"
        self.state.mkdir(parents=True, exist_ok=True)
        self.run.mkdir(parents=True, exist_ok=True)
        self.endpoints = endpoints

    async def endpoint(self, provider, *, start=True):
        ep = self.endpoints[provider]
        if isinstance(ep, BaseException):
            raise ep
        return ep


def test_marker_matching_skips_code_fences_and_inline_mentions():
    text = (
        "If I get stuck I will output ::cpr:escalate in prose.\n"
        "```text\n"
        "::cpr:escalate inside backticks\n"
        "```\n"
        "~~~\n"
        "[ESCALATE] inside tildes\n"
        "~~~\n"
        "::cpr:escalate_not_a_boundary\n"
        "  ::cpr:escalate: blocked on stage 2\n"
    )
    hits = match_markers(text, ["::cpr:escalate", "[ESCALATE]"], mode="line")
    assert hits == [("::cpr:escalate", "::cpr:escalate: blocked on stage 2")]

    sub_hits = match_markers("Mentioned ::cpr:escalate inline", ["::cpr:escalate"], mode="substring")
    assert len(sub_hits) == 1 and sub_hits[0][0] == "::cpr:escalate"


async def test_inbox_concurrent_push_dedup_and_high_watermark_survives_truncate(tmp_path):
    rt = SimpleNamespace(state=tmp_path / "state", run=tmp_path / "run")
    rt.state.mkdir()
    rt.run.mkdir()
    tasks = Tasks(rt)

    results = await asyncio.gather(*(
        tasks.inboxes.push("coord", kind="milestone", summary=f"msg {i}", thread_id="t1")
        for i in range(15)
    ))
    seqs = sorted(r["seq"] for r in results)
    assert seqs == list(range(1, 16))

    # Truncate the JSONL file; high-watermark meta must prevent sequence regression.
    tasks.inboxes.events_path("coord").write_text("", encoding="utf-8")
    nxt = await tasks.inboxes.push("coord", kind="done", summary="after truncate")
    assert nxt["seq"] == 16

    # Request ID deduplication via Tasks.mutate
    desc = {"command": "inbox.push", "channel": "coord", "kind": "blocked", "summary": "dup"}
    first = await tasks.mutate("req-inbox-1", desc, lambda: tasks.inboxes.push("coord", kind="blocked", summary="dup", request_id="req-inbox-1"))
    second = await tasks.mutate("req-inbox-1", desc, lambda: tasks.inboxes.push("coord", kind="blocked", summary="dup", request_id="req-inbox-1"))
    assert first["seq"] == second["seq"] == 17 and second["reusedReceipt"] is True


async def test_inbox_cli_external_requires_sender_and_supports_list_ack(tmp_path):
    p = cli.parser()
    no_sender = p.parse_args(["--home", str(tmp_path), "--external", "inbox", "push",
                              "--channel", "ch1", "--kind", "blocked", "--summary", "hi"])
    with pytest.raises(RuntimeFailure, match="sender"):
        await cli.run(no_sender)

    with_sender = p.parse_args(["--home", str(tmp_path), "--external", "--sender", "AGY", "inbox", "push",
                                "--channel", "ch1", "--kind", "blocked", "--summary", "hi",
                                "--payload-json", '{"phase":"B"}', "--request-id", "cli-push-1"])
    pushed = await cli.run(with_sender)
    assert pushed["seq"] == 1 and pushed["sender"] == "AGY" and pushed["payload"] == {"phase": "B"}

    listed = await cli.run(p.parse_args(["--home", str(tmp_path), "--external", "inbox", "list",
                                         "--channel", "ch1", "--consumer", "c1", "--unread-only"]))
    assert len(listed["messages"]) == 1 and listed["lastSeq"] == 1

    acked = await cli.run(p.parse_args(["--home", str(tmp_path), "--external", "inbox", "ack",
                                        "--channel", "ch1", "--consumer", "c1", "--latest"]))
    assert acked["lastSeq"] == 1

    listed_after = await cli.run(p.parse_args(["--home", str(tmp_path), "--external", "inbox", "list",
                                               "--channel", "ch1", "--consumer", "c1", "--unread-only"]))
    assert listed_after["messages"] == [] and listed_after["cursorSeq"] == 1


async def test_inbox_doorbell_both_orders_kind_filter_and_stale_socket_cleanup(tmp_path):
    rt = FakeRuntime(tmp_path, {})
    tasks = Tasks(rt)

    # Order A: push before wait (race-free disk scan)
    await tasks.inboxes.push("agy", kind="progress", summary="heartbeat 1")
    await tasks.inboxes.push("agy", kind="blocked", summary="real blocker")
    res_a = await tasks.wait_multi([], inboxes=["agy"], inbox_kinds=["blocked", "done"], consumer="c1", timeout=2)
    assert res_a["status"] == "events" and res_a["worst"] == "error"
    assert len(res_a["events"]) == 1 and res_a["events"][0]["summary"] == "real blocker"
    assert tasks.cursors.load("c1")["inboxes"]["agy"]["lastSeq"] == 2

    # Create a dead AF_UNIX socket matching the channel prefix; poke must unlink it.
    if inbox_mod.UNIX_DGRAM_SUPPORTED:
        stale = rt.run / f"{doorbell_prefix('agy')}999999-dead00.sock"
        dead_sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
        dead_sock.bind(str(stale))
        dead_sock.close()
        assert stale.exists()

    # Order B: wait armed first, push arrives 60ms later (must wake fast via doorbell, not 5s fallback)
    async def delayed_push():
        await asyncio.sleep(0.06)
        await tasks.inboxes.push("agy", kind="progress", summary="ignored heartbeat")
        await asyncio.sleep(0.03)
        await tasks.inboxes.push("agy", kind="done", summary="phase complete")

    started = time.monotonic()
    push_task = asyncio.create_task(delayed_push())
    res_b = await tasks.wait_multi([], inboxes=["agy"], inbox_kinds=["blocked", "done"], consumer="c1", timeout=5)
    elapsed = time.monotonic() - started
    await push_task
    assert res_b["status"] == "events" and res_b["events"][0]["inboxKind"] == "done"
    assert elapsed < 1.5
    if inbox_mod.UNIX_DGRAM_SUPPORTED:
        assert not stale.exists()


async def test_inbox_doorbell_windows_stat_polling_fallback(tmp_path, monkeypatch):
    monkeypatch.setattr(inbox_mod, "UNIX_DGRAM_SUPPORTED", False)
    store = InboxStore(tmp_path / "state", tmp_path / "run")

    async def writer():
        await asyncio.sleep(0.08)
        await store.push("win-ch", kind="done", summary="from windows")

    async with InboxDoorbell(tmp_path / "run", tmp_path / "state", ["win-ch"], poll_interval=0.02) as bell:
        task = asyncio.create_task(writer())
        woke = await bell.wait(timeout=2.0)
        await task
    assert woke is True


async def test_multi_wait_shared_provider_connection_and_send_consumer_anchor(tmp_path):
    turns_a1 = [
        {"id": "t_old", "status": "completed", "items": [{"type": "agentMessage", "text": "::cpr:escalate old marker"}]},
    ]
    turns_a2 = [
        {"id": "t_a2_1", "status": "inProgress", "items": []},
    ]
    turns_b1 = [
        {"id": "t_b1_1", "status": "inProgress", "items": []},
    ]
    rpc_a = FakeRpc({"a1": turns_a1, "a2": turns_a2})
    rpc_b = FakeRpc({"b1": turns_b1})
    ep_a = FakeEndpoint("prov-a", lambda: rpc_a)
    ep_b = FakeEndpoint("prov-b", lambda: rpc_b)
    rt = FakeRuntime(tmp_path, {"prov-a": ep_a, "prov-b": ep_b})
    tasks = Tasks(rt)

    row_a1 = {"id": "a1", "modelProvider": "prov-a"}
    row_a2 = {"id": "a2", "modelProvider": "prov-a"}
    row_b1 = {"id": "b1", "modelProvider": "prov-b"}

    # Anchor via send --consumer; old marker in t_old must NOT fire, new turn MUST fire even if already completed before wait!
    sent = await tasks.send(row_a1, "start new work", consumer="agy")
    assert sent["consumerAnchored"] == "agy"
    new_tid = sent["turnId"]
    # Complete the new turn BEFORE wait_multi starts (simulates 2s gap between send and wait)
    turns_a1[0] = {
        "id": new_tid,
        "status": "completed",
        "items": [{"type": "agentMessage", "text": "Finished step.\n::cpr:done all good"}],
    }
    ep_a.open_count = 0
    res = await tasks.wait_multi(
        [row_a1, row_a2, row_b1],
        markers=["::cpr:escalate", "::cpr:done"],
        consumer="agy",
        timeout=2,
        poll_interval=0.02,
    )
    assert res["status"] == "events"
    assert len(res["events"]) == 1
    ev = res["events"][0]
    assert ev["threadId"] == "a1" and ev["turnId"] == new_tid and ev["kind"] == "thread_marker"
    assert ev["payload"]["marker"] == "::cpr:done"
    # Provider A has 2 threads but only opened 1 shared RPC connection
    assert ep_a.open_count == 1


async def test_multi_wait_marker_ignores_user_message_and_catches_mid_turn_items(tmp_path):
    turns = [
        {
            "id": "t1",
            "status": "inProgress",
            "items": [
                {"type": "userMessage", "content": [{"type": "text", "text": "::cpr:escalate\nPlease use that marker"}]},
                {"type": "reasoning", "text": "::cpr:escalate"},
                {"type": "agentMessage", "text": "Working on it, if blocked I will emit ::cpr:escalate"},
            ],
        }
    ]
    rpc = FakeRpc({"thr1": turns})
    rt = FakeRuntime(tmp_path, {"p": FakeEndpoint("p", lambda: rpc)})
    tasks = Tasks(rt)
    row = {"id": "thr1", "modelProvider": "p"}

    # Must NOT trigger on userMessage, reasoning, or inline mention
    timeout_res = await tasks.wait_multi([row], markers=["::cpr:escalate"], consumer="c1", timeout=0.08, poll_interval=0.02)
    assert timeout_res["status"] == "timeout"

    # Append a real agentMessage with line-start marker WHILE turn is still inProgress!
    turns[0]["items"].append({"type": "agentMessage", "text": "::cpr:escalate need review #1"})
    res1 = await tasks.wait_multi([row], markers=["::cpr:escalate"], consumer="c1", timeout=1, poll_interval=0.02)
    assert res1["status"] == "events" and res1["events"][0]["summary"] == "::cpr:escalate need review #1"
    assert res1["events"][0]["payload"]["turnStatus"] == "inProgress"

    # Re-arm while still inProgress: must NOT re-fire item #1
    timeout_res2 = await tasks.wait_multi([row], markers=["::cpr:escalate"], consumer="c1", timeout=0.08, poll_interval=0.02)
    assert timeout_res2["status"] == "timeout"

    # Emit a second marker item mid-turn -> MUST fire!
    turns[0]["items"].append({"type": "agentMessage", "text": "::cpr:escalate need review #2"})
    res2 = await tasks.wait_multi([row], markers=["::cpr:escalate"], consumer="c1", timeout=1, poll_interval=0.02)
    assert res2["status"] == "events" and res2["events"][0]["summary"] == "::cpr:escalate need review #2"

    # Turn completes without a new marker -> must NOT wake in marker mode!
    turns[0]["status"] = "completed"
    timeout_res3 = await tasks.wait_multi([row], markers=["::cpr:escalate"], consumer="c1", timeout=0.08, poll_interval=0.02)
    assert timeout_res3["status"] == "timeout"


async def test_multi_wait_sticky_anomaly_dedup_and_new_turn_recovery(tmp_path):
    turns = [{"id": "t1", "status": "failed", "items": []}]
    rpc = FakeRpc({"thr1": turns})
    rt = FakeRuntime(tmp_path, {"p": FakeEndpoint("p", lambda: rpc)})
    tasks = Tasks(rt)
    row = {"id": "thr1", "modelProvider": "p"}
    await tasks.cursors.anchor_thread("c1", "thr1", baseline_turn_id=None)

    # First wait catches failure with exit code 0 (multi-target mode separates observation from CLI failure)
    res1 = await tasks.wait_multi([row], markers=["::cpr:done"], consumer="c1", timeout=1, poll_interval=0.02)
    assert res1["status"] == "events" and res1["worst"] == "error"
    assert res1["events"][0]["kind"] == "thread_failed"
    assert cli.status_code(res1) == 0

    # Re-arming on the same sticky failed turn must NOT loop!
    res2 = await tasks.wait_multi([row], markers=["::cpr:done"], consumer="c1", timeout=0.08, poll_interval=0.02)
    assert res2["status"] == "timeout"
    assert cli.status_code(res2) == 5

    # A brand new turn that gets interrupted MUST trigger cleanly
    turns.insert(0, {"id": "t2", "status": "interrupted", "items": []})
    res3 = await tasks.wait_multi([row], markers=["::cpr:done"], consumer="c1", timeout=1, poll_interval=0.02)
    assert res3["status"] == "events" and res3["events"][0]["kind"] == "thread_interrupted" and res3["events"][0]["turnId"] == "t2"


async def test_multi_wait_disconnect_recovery_and_idle_timeout(tmp_path):
    turns = [{"id": "t1", "status": "completed", "items": []}]
    fail_flag = {"down": True}
    rpc = FakeRpc({"thr1": turns})
    ep = FakeEndpoint("p", lambda: TransportError("socket gone") if fail_flag["down"] else rpc)
    rt = FakeRuntime(tmp_path, {"p": ep})
    tasks = Tasks(rt)
    row = {"id": "thr1", "modelProvider": "p"}
    await tasks.cursors.anchor_thread("c1", "thr1", baseline_turn_id="t1", acked_state={"turnId": "t1", "status": "completed"})

    # Sustained disconnect emits one thread_disconnected event
    res_down = await tasks.wait_multi([row], markers=["::cpr:done"], consumer="c1", timeout=1, disconnect_grace=0.05, poll_interval=0.02)
    assert res_down["status"] == "events" and res_down["events"][0]["kind"] == "thread_disconnected"

    # Staying down does not spam duplicate disconnected events
    res_still_down = await tasks.wait_multi([row], markers=["::cpr:done"], consumer="c1", timeout=0.1, disconnect_grace=0.05, poll_interval=0.02)
    assert res_still_down["status"] == "timeout"

    # Reconnect emits thread_recovered
    fail_flag["down"] = False
    res_up = await tasks.wait_multi([row], markers=["::cpr:done"], consumer="c1", timeout=1, disconnect_grace=0.05, poll_interval=0.02)
    assert res_up["status"] == "events" and res_up["events"][0]["kind"] == "thread_recovered"

    # Idle timeout fires once when thread stays idle
    res_idle = await tasks.wait_multi([row], markers=["::cpr:done"], consumer="c1", timeout=1, idle_timeout=0.06, poll_interval=0.02)
    assert res_idle["status"] == "events" and res_idle["events"][0]["kind"] == "thread_idle_timeout"
    res_idle_again = await tasks.wait_multi([row], markers=["::cpr:done"], consumer="c1", timeout=0.12, idle_timeout=0.06, poll_interval=0.02)
    assert res_idle_again["status"] == "timeout"


async def test_consumer_lease_conflict_and_no_advance(tmp_path):
    rt = FakeRuntime(tmp_path, {})
    tasks = Tasks(rt)

    # Concurrent consumer lease must fail fast with ConsumerBusyError
    async with tasks.cursors.lease("busy-consumer"):
        with pytest.raises(ConsumerBusyError):
            await tasks.wait_multi([], inboxes=["ch"], consumer="busy-consumer", timeout=1)

    # --no-advance reads event without advancing disk cursor
    await tasks.inboxes.push("ch", kind="done", summary="keep unread")
    peek = await tasks.wait_multi([], inboxes=["ch"], consumer="peek-c", no_advance=True, timeout=1)
    assert len(peek["events"]) == 1 and peek["cursor"]["advanced"] is False
    assert tasks.cursors.load("peek-c")["inboxes"] == {}


def test_cli_flush_before_commit_and_usage_exit_codes(tmp_path):
    class FailingStdout:
        def write(self, _data):
            raise BrokenPipeError("stdout closed")
        def flush(self):
            pass

    home = tmp_path / "cli_home"
    home.mkdir()
    assert cli.main(["--home", str(home), "inbox", "push", "--channel", "ch2", "--kind", "done", "--summary", "s1"]) == 0

    # Flush-before-commit: BrokenPipeError on stdout prevents cursor commit!
    with patch.object(cli.sys, "stdout", FailingStdout()):
        rc = cli.main(["--home", str(home), "wait", "--inbox", "ch2", "--consumer", "flush-c", "--timeout", "1"])
    assert rc == 1

    # Cursor must still be unadvanced so the next wait gets the event!
    buf = io.StringIO()
    with patch.object(cli.sys, "stdout", buf):
        rc2 = cli.main(["--home", str(home), "wait", "--inbox", "ch2", "--consumer", "flush-c", "--timeout", "1"])
    assert rc2 == 0
    payload = json.loads(buf.getvalue())
    assert payload["events"][0]["summary"] == "s1"
    assert payload["cursor"]["advanced"] is True

    # Invalid usage exits with code 2
    assert cli.main(["--home", str(home), "wait"]) == 2
    assert cli.main(["--home", str(home), "wait", "--thread", "a", "--thread", "b", "--turn", "t1"]) == 2


async def test_pagination_catchup_beyond_single_page(tmp_path):
    # 30 turns (newest first: t30..t1); anchor at t1, marker is in t5 (second page when page_size_cap=10)
    turns = [{"id": f"t{i}", "status": "completed", "items": []} for i in range(30, 0, -1)]
    for t in turns:
        if t["id"] == "t5":
            t["items"] = [{"type": "agentMessage", "text": "::cpr:milestone buried in page 3"}]
    rpc = FakeRpc({"thr1": turns}, page_size_cap=10)
    rt = FakeRuntime(tmp_path, {"p": FakeEndpoint("p", lambda: rpc)})
    tasks = Tasks(rt)
    row = {"id": "thr1", "modelProvider": "p"}
    await tasks.cursors.anchor_thread("c1", "thr1", baseline_turn_id="t1")

    res = await tasks.wait_multi([row], markers=["::cpr:milestone"], consumer="c1", timeout=1, poll_interval=0.02)
    assert res["status"] == "events"
    assert res["events"][0]["turnId"] == "t5"
    assert tasks.cursors.load("c1")["threads"]["thr1"]["baselineTurnId"] == "t30"

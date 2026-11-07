import asyncio
import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from codex_provider_runtime.cli import parser, run, status_code
from codex_provider_runtime.runtime import RuntimeFailure, Runtime
from codex_provider_runtime.state import atomic_json, read_json
from codex_provider_runtime.tasks import Tasks, select_name
from codex_provider_runtime.transport import TransportError


def test_exact_name_and_ambiguity():
    rows = [{"id": "1", "name": "Review", "modelProvider": "a"},
            {"id": "2", "name": "Review followup", "modelProvider": "b"}]
    assert select_name(rows, "Review")["id"] == "1"
    assert select_name(rows, "FOLLOWUP")["id"] == "2"
    with pytest.raises(RuntimeFailure, match="exactly one"):
        select_name(rows, "rev")
    with pytest.raises(RuntimeFailure, match="exactly one"):
        select_name(rows, "missing")


async def test_receipt_reuses_success_and_rejects_different_body(tmp_path):
    tasks = Tasks(SimpleNamespace(state=tmp_path))
    operation = AsyncMock(return_value={"threadId": "one", "turnId": "turn"})
    a = await tasks.mutate("request", {"message": "first"}, operation)
    b = await tasks.mutate("request", {"message": "first"}, operation)
    assert a["turnId"] == b["turnId"] and b["reusedReceipt"]
    assert operation.await_count == 1
    with pytest.raises(RuntimeFailure, match="different operation"):
        await tasks.mutate("request", {"message": "second"}, operation)
    assert operation.await_count == 1


async def test_receipt_does_not_retry_uncertain_write(tmp_path):
    tasks = Tasks(SimpleNamespace(state=tmp_path))
    operation = AsyncMock(side_effect=TransportError("response lost after send"))
    with pytest.raises(TransportError):
        await tasks.mutate("request", {"message": "one"}, operation)
    with pytest.raises(RuntimeFailure, match="uncertain"):
        await tasks.mutate("request", {"message": "one"}, operation)
    assert operation.await_count == 1


async def test_simultaneous_receipt_only_executes_once(tmp_path):
    tasks = Tasks(SimpleNamespace(state=tmp_path))
    operation = AsyncMock(return_value={"accepted": True})
    results = await asyncio.gather(*(tasks.mutate("same", {"x": 1}, operation) for _ in range(3)))
    assert operation.await_count == 1
    assert sum(bool(r.get("reusedReceipt")) for r in results) == 2


async def test_external_checks_command_after_parsing_and_before_runtime():
    p = parser()
    for tail in [["send", "--thread", "some", "--message", "hello"], ["server", "ensure", "--provider", "a"]]:
        args = p.parse_args(["--external", *tail])
        with pytest.raises(RuntimeFailure, match="sender"):
            await run(args)


async def test_queue_wait_is_rejected_before_any_delivery():
    args = parser().parse_args(["send", "--thread", "some", "--message", "hello", "--mode", "queue", "--wait"])
    with pytest.raises(RuntimeFailure, match="no turn ID"):
        await run(args)


def test_owned_process_identity_cannot_be_a_pid_only():
    import os
    assert Runtime.process_matches({"owned": True, "pid": os.getpid(), "created": 0, "socket": "foreign"}) is False
    assert Runtime.process_matches({"owned": False, "pid": os.getpid()}) is False


def test_terminal_exit_codes():
    assert [status_code({"status": s}) for s in ["completed", "failed", "interrupted", "timeout", "waitingForInput"]] == [0, 3, 4, 5, 6]


def test_json_error_roundtrips_unicode_in_gbk_console(tmp_path):
    import os
    import subprocess
    import sys
    missing = tmp_path/"不存在\u00a0\U0001f9ea.txt"
    result = subprocess.run([sys.executable, "-m", "codex_provider_runtime.cli", "send", "--thread", "unused",
        "--message-file", str(missing)], capture_output=True,
        env={**os.environ, "PYTHONIOENCODING": "gbk"})
    assert result.returncode == 1
    error = json.loads(result.stderr.decode("ascii"))["error"]
    assert "不存在" in error and "\U0001f9ea" in error


def test_json_result_roundtrips_unicode_in_gbk_console():
    import io
    from unittest.mock import patch
    from codex_provider_runtime import cli
    buffer = io.BytesIO()
    stream = io.TextIOWrapper(buffer, encoding="gbk")
    result = {"name": "中文\u00a0\U0001f9ea"}
    with patch.object(cli, "run", AsyncMock(return_value=result)), patch.object(cli.sys, "stdout", stream):
        assert cli.main(["doctor"]) == 0
        stream.flush()
        assert json.loads(buffer.getvalue().decode("ascii")) == result

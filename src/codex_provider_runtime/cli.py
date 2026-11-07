"""One JSON CLI used by Codex, Claude, and one-shot reply watchers."""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
from pathlib import Path
import sys
import uuid

from .runtime import Runtime, RuntimeFailure
from .tasks import Tasks, TERMINAL, run_watcher, select_name
from .transport import RpcError, TransportError


def parser():
    p = argparse.ArgumentParser(description="Local provider-isolated native Codex tasks")
    p.add_argument("--home")
    p.add_argument("--binary")
    p.add_argument("--app-socket")
    p.add_argument("--external", action="store_true")
    p.add_argument("--sender")
    commands = p.add_subparsers(dest="command", required=True)

    def target(q):
        group = q.add_mutually_exclusive_group(required=True)
        group.add_argument("--thread")
        group.add_argument("--name")
        q.add_argument("--provider")

    def mutation(q):
        q.add_argument("--request-id")

    def config(q, context=True):
        q.add_argument("--model")
        q.add_argument("--effort", choices=["none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra"])
        if context:
            q.add_argument("--context-window", type=int)
            q.add_argument("--auto-compact-token-limit", type=int)

    def message(q, required=False):
        group = q.add_mutually_exclusive_group(required=required)
        group.add_argument("--message")
        group.add_argument("--message-file")
        q.add_argument("--wait", action="store_true")
        q.add_argument("--reply-to", help="Authorized destination thread ID")
        q.add_argument("--timeout", type=float, default=3600)

    commands.add_parser("doctor")
    server = commands.add_parser("server").add_subparsers(dest="server_command", required=True)
    for name in ["status", "ensure", "stop"]:
        q = server.add_parser(name)
        q.add_argument("--provider", required=name != "status")
        if name != "status":
            mutation(q)
    for name in ["list", "find"]:
        q = commands.add_parser(name)
        q.add_argument("--name", required=name == "find")
        q.add_argument("--provider")
        q.add_argument("--archived", action="store_true")
    for name in ["create", "fork"]:
        q = commands.add_parser(name)
        if name == "fork":
            target(q)
        else:
            q.add_argument("--provider")
        q.add_argument("--title")
        q.add_argument("--cwd")
        config(q)
        message(q)
        mutation(q)
    q = commands.add_parser("resume")
    target(q)
    config(q)
    message(q)
    mutation(q)
    q = commands.add_parser("send")
    target(q)
    config(q, context=False)
    message(q, required=True)
    q.add_argument("--mode", choices=["followup", "start", "steer", "queue"], default="followup")
    mutation(q)
    for name in ["read", "wait", "interrupt"]:
        q = commands.add_parser(name)
        target(q)
        if name in {"wait", "interrupt"}:
            q.add_argument("--turn")
        if name == "wait":
            q.add_argument("--timeout", type=float, default=3600)
        if name == "interrupt":
            mutation(q)
    q = commands.add_parser("reply")
    target(q)
    q.add_argument("--source", required=True)
    q.add_argument("--turn", required=True)
    q.add_argument("--wait", action="store_true")
    q.add_argument("--timeout", type=float, default=3600)
    mutation(q)
    commands.add_parser("_watch", help=argparse.SUPPRESS).add_argument("path", type=Path)
    return p


def status_code(result):
    return {"failed": 3, "interrupted": 4, "timeout": 5, "waitingForInput": 6}.get(result.get("status"), 0)


async def run(args):
    command = args.command
    if command == "_watch":
        return await run_watcher(args.path)
    mutating = command in {"create", "fork", "resume", "send", "reply", "interrupt"} or (command == "server" and args.server_command != "status")
    if args.external and mutating and not args.sender:
        raise RuntimeFailure("External writes require --sender attribution")
    if getattr(args, "timeout", 0) < 0:
        raise RuntimeFailure("Timeout must be nonnegative")
    for field in ["context_window", "auto_compact_token_limit"]:
        value = getattr(args, field, None)
        if value is not None and value <= 0:
            raise RuntimeFailure("Context limits must be positive")
    body = getattr(args, "message", None)
    if getattr(args, "message_file", None):
        body = Path(args.message_file).read_text(encoding="utf-8")
    if args.external and body is not None:
        body = f"[From {args.sender}, an external agent, via codex-provider-runtime.]\n\n"+body
    if command in {"create", "fork", "resume"} and body is None and (args.wait or args.reply_to):
        raise RuntimeFailure("--wait/--reply-to require a message")
    if command == "send" and args.mode == "queue" and (args.wait or args.reply_to):
        raise RuntimeFailure("Native queue has no turn ID yet; wait/read the resulting turn separately")
    runtime = Runtime(args.home, args.binary, args.app_socket)
    tasks = Tasks(runtime)
    request_id = getattr(args, "request_id", None) or str(uuid.uuid4())
    description = {k: v for k,v in vars(args).items() if k not in {"request_id", "wait", "reply_to", "timeout"}}
    if body is not None:
        description["message"] = body
    if command in {"doctor", "server"}:
        if command == "doctor" or args.server_command == "status":
            rows = await runtime.status()
            if getattr(args, "provider", None):
                rows = [r for r in rows if r["provider"] == args.provider]
            return {"home": str(runtime.home), "binary": runtime.binary, "runtimeDirectory": str(runtime.run), "servers": rows}
        async def server_operation():
            if args.server_command == "ensure":
                return asdict(await runtime.endpoint(args.provider))
            return await runtime.stop(args.provider)
        return await tasks.mutate(request_id, description, server_operation)
    if command in {"list", "find"}:
        rows = await tasks.list(args.provider, args.archived)
        if command == "find":
            return {"thread": select_name(rows, args.name)}
        if args.name:
            rows = [r for r in rows if args.name.casefold() in (r.get("name") or "").casefold()]
        return {"threads": rows}
    source = None
    if command == "fork":
        source = await tasks.resolve(args.thread, args.name)
    elif command != "create":
        row = await tasks.resolve(args.thread, args.name, args.provider)
    destination = await tasks.resolve(thread=args.reply_to) if getattr(args, "reply_to", None) else None
    context = {}
    for attr, field in [("context_window", "model_context_window"), ("auto_compact_token_limit", "model_auto_compact_token_limit")]:
        if getattr(args, attr, None) is not None:
            context[field] = getattr(args, attr)
    if command in {"create", "fork"}:
        result = await tasks.mutate(request_id, description, lambda: tasks.create(provider=args.provider, source=source,
            title=args.title, cwd=args.cwd, model=args.model, effort=args.effort, context=context))
        row = {"id": result["threadId"], "modelProvider": result["provider"]}
    elif command == "resume":
        result = await tasks.mutate(request_id, description, lambda: tasks.resume(row, model=args.model, effort=args.effort, context=context))
    elif command == "read":
        return {"thread": await tasks.read(row)}
    elif command == "wait":
        return await tasks.wait(row, args.turn, args.timeout)
    elif command == "interrupt":
        return await tasks.mutate(request_id, description, lambda: tasks.interrupt(row, args.turn))
    elif command == "reply":
        source = await tasks.resolve(thread=args.source)
        if args.wait:
            state = await tasks.wait(source, args.turn, args.timeout)
            if state["status"] not in TERMINAL:
                return state
        return await tasks.reply(row, source, args.turn, request_id=request_id)
    else:
        result = {}
    if body is not None:
        send_id = request_id if command == "send" else request_id+":send"
        receipt = await tasks.mutate(send_id, {"command": "send", "thread": row["id"], "body": body,
            "mode": getattr(args, "mode", "followup"), "model": args.model, "effort": args.effort},
            lambda: tasks.send(row, body, mode=getattr(args, "mode", "followup"), model=args.model, effort=args.effort, message_id=send_id))
        result = {**result, **receipt}
        if destination:
            result.update(tasks.launch_watcher(row, receipt["turnId"], destination, send_id, timeout=0))
        if args.wait:
            result.update(await tasks.wait(row, receipt["turnId"], args.timeout))
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        result = asyncio.run(run(args))
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return status_code(result)
    except (RuntimeFailure, RpcError, TransportError, OSError, ValueError, TimeoutError) as error:
        print(json.dumps({"error": str(error), "errorType": type(error).__name__}, ensure_ascii=False), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print(json.dumps({"error": "Client interrupted; native task may still be running. Read before resending."}), file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())

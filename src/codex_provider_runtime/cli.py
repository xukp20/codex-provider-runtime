"""One JSON CLI used by Codex, Claude, and one-shot reply watchers."""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import asdict
import json
from pathlib import Path
import sys
import uuid

from . import __version__
from .inbox import ConsumerBusyError
from .runtime import Runtime, RuntimeFailure
from .tasks import Tasks, TERMINAL, run_watcher, select_name
from .transport import RpcError, TransportError


class UsageFailure(RuntimeFailure):
    """Invalid semantic CLI argument combination (maps to exit code 2)."""


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
        q.add_argument("--consumer", help="Anchor consumer cursor to this send before delivery")

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
    for name in ["read", "interrupt"]:
        q = commands.add_parser(name)
        target(q)
        if name == "interrupt":
            q.add_argument("--turn")
            mutation(q)
    q = commands.add_parser("wait")
    q.add_argument("--thread", action="append", default=[])
    q.add_argument("--name", action="append", default=[])
    q.add_argument("--provider")
    q.add_argument("--turn")
    q.add_argument("--inbox", action="append", default=[])
    q.add_argument("--inbox-kind", action="append", default=[])
    q.add_argument("--wake-on-marker", action="append", default=[])
    q.add_argument("--marker-match", choices=["line", "substring"], default="line")
    q.add_argument("--consumer")
    q.add_argument("--no-advance", action="store_true")
    q.add_argument("--timeout", type=float, default=3600)
    q.add_argument("--idle-timeout", type=float, default=0)
    inbox = commands.add_parser("inbox").add_subparsers(dest="inbox_command", required=True)
    ipush = inbox.add_parser("push")
    ipush.add_argument("--channel", required=True)
    ipush.add_argument("--kind", required=True)
    ipush.add_argument("--summary", required=True)
    ipush.add_argument("--thread")
    ipush.add_argument("--phase")
    ipush.add_argument("--details-path")
    payload_group = ipush.add_mutually_exclusive_group()
    payload_group.add_argument("--payload-json")
    payload_group.add_argument("--payload-file")
    mutation(ipush)
    ilist = inbox.add_parser("list")
    ilist.add_argument("--channel", required=True)
    ilist.add_argument("--consumer")
    ilist.add_argument("--unread-only", action="store_true")
    ilist.add_argument("--kind", action="append", default=[])
    ilist.add_argument("--limit", type=int)
    iack = inbox.add_parser("ack")
    iack.add_argument("--channel", required=True)
    iack.add_argument("--consumer", required=True)
    ack_group = iack.add_mutually_exclusive_group(required=True)
    ack_group.add_argument("--seq", type=int)
    ack_group.add_argument("--latest", action="store_true")
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


def _install_meta():
    meta_path = Path(__file__).resolve().parents[2] / "BUILD_META.json"
    try:
        return json.loads(meta_path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None


async def run(args, *, commit_callbacks=None):
    command = args.command
    if command == "_watch":
        return await run_watcher(args.path)
    mutating = (
        command in {"create", "fork", "resume", "send", "reply", "interrupt"}
        or (command == "server" and args.server_command != "status")
        or (command == "inbox" and args.inbox_command == "push")
    )
    if args.external and mutating and not args.sender:
        raise RuntimeFailure("External writes require --sender attribution")
    if getattr(args, "timeout", 0) < 0:
        raise RuntimeFailure("Timeout must be nonnegative")
    if getattr(args, "idle_timeout", 0) < 0:
        raise UsageFailure("Idle timeout must be nonnegative")
    for field in ["context_window", "auto_compact_token_limit"]:
        value = getattr(args, field, None)
        if value is not None and value <= 0:
            raise RuntimeFailure("Context limits must be positive")
    body = getattr(args, "message", None)
    if getattr(args, "message_file", None):
        body = Path(args.message_file).read_text(encoding="utf-8")
    if args.external and body is not None:
        body = f"[From {args.sender}, an external agent, via codex-provider-runtime.]\n\n"+body
    if command in {"create", "fork", "resume"} and body is None and (args.wait or args.reply_to or args.consumer):
        raise RuntimeFailure("--wait/--reply-to/--consumer require a message")
    if command == "send" and args.mode == "queue" and (args.wait or args.reply_to):
        raise RuntimeFailure("Native queue has no turn ID yet; wait/read the resulting turn separately")

    is_multi_wait = False
    if command == "wait":
        if not args.thread and not args.name and not args.inbox:
            raise UsageFailure("Specify at least one --thread, --name, or --inbox")
        if args.inbox_kind and not args.inbox:
            raise UsageFailure("--inbox-kind requires --inbox")
        if args.no_advance and not args.consumer:
            raise UsageFailure("--no-advance requires --consumer")
        is_multi_wait = (
            (len(args.thread) + len(args.name) != 1)
            or bool(args.inbox or args.wake_on_marker or args.consumer or args.idle_timeout or args.no_advance)
        )
        if args.turn and is_multi_wait:
            raise UsageFailure("--turn is only supported for single-thread wait without multi-target, inbox, marker, consumer, or idle-timeout options")

    if command == "inbox":
        if args.inbox_command == "list":
            if args.unread_only and not args.consumer:
                raise UsageFailure("--unread-only requires --consumer")
            if args.limit is not None and args.limit < 0:
                raise UsageFailure("--limit must be nonnegative")
        elif args.inbox_command == "ack" and args.seq is not None and args.seq < 0:
            raise UsageFailure("--seq must be nonnegative")

    needs_binary = not (command == "inbox" or (command == "wait" and not args.thread and not args.name))
    runtime = Runtime(args.home, args.binary, args.app_socket, require_binary=needs_binary)
    tasks = Tasks(runtime)
    request_id = getattr(args, "request_id", None) or str(uuid.uuid4())
    description = {k: v for k, v in vars(args).items() if k not in {"request_id", "wait", "reply_to", "timeout", "consumer"}}
    if body is not None:
        description["message"] = body

    if command == "inbox":
        if args.inbox_command == "push":
            payload = None
            if args.payload_json is not None:
                try:
                    payload = json.loads(args.payload_json)
                except json.JSONDecodeError as error:
                    raise UsageFailure(f"Invalid --payload-json: {error}") from error
            elif args.payload_file is not None:
                try:
                    payload = json.loads(Path(args.payload_file).read_text(encoding="utf-8"))
                except json.JSONDecodeError as error:
                    raise UsageFailure(f"Invalid --payload-file JSON: {error}") from error
            push_desc = {
                "command": "inbox.push",
                "channel": args.channel,
                "kind": args.kind,
                "summary": args.summary,
                "sender": args.sender,
                "thread": args.thread,
                "phase": args.phase,
                "detailsPath": args.details_path,
                "payload": payload,
            }
            return await tasks.mutate(
                request_id,
                push_desc,
                lambda: tasks.inboxes.push(
                    args.channel,
                    kind=args.kind,
                    summary=args.summary,
                    sender=args.sender,
                    thread_id=args.thread,
                    phase=args.phase,
                    details_path=args.details_path,
                    payload=payload,
                    request_id=request_id,
                ),
            )
        if args.inbox_command == "list":
            cursor_seq = None
            after_seq = 0
            if args.consumer:
                cursor_data = tasks.cursors.load(args.consumer)
                cursor_seq = int((cursor_data["inboxes"].get(args.channel) or {}).get("lastSeq") or 0)
                if args.unread_only:
                    after_seq = cursor_seq
            messages, high_seq = tasks.inboxes.read(
                args.channel, after_seq=after_seq, kinds=args.kind or None, limit=args.limit,
            )
            return {
                "channel": args.channel,
                "lastSeq": high_seq,
                "cursorSeq": cursor_seq,
                "messages": messages,
            }
        seq = tasks.inboxes.latest_seq(args.channel) if args.latest else args.seq
        return await tasks.cursors.ack_inbox(args.consumer, args.channel, seq)

    if command in {"doctor", "server"}:
        if command == "doctor" or args.server_command == "status":
            rows = await runtime.status()
            if getattr(args, "provider", None):
                rows = [r for r in rows if r["provider"] == args.provider]
            out = {"version": __version__, "home": str(runtime.home), "binary": runtime.binary,
                "runtimeDirectory": str(runtime.run), "servers": rows}
            meta = _install_meta()
            if meta is not None:
                out["installMeta"] = meta
            return out
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
    if command == "wait":
        if not is_multi_wait:
            row = await tasks.resolve(args.thread[0] if args.thread else None, args.name[0] if args.name else None, args.provider)
            return await tasks.wait(row, args.turn, args.timeout)
        rows = []
        for tid in args.thread:
            rows.append(await tasks.resolve(thread=tid, provider=args.provider))
        for title in args.name:
            rows.append(await tasks.resolve(name=title, provider=args.provider))
        return await tasks.wait_multi(
            rows,
            inboxes=args.inbox,
            inbox_kinds=args.inbox_kind,
            markers=args.wake_on_marker,
            marker_mode=args.marker_match,
            consumer=args.consumer,
            no_advance=args.no_advance,
            timeout=args.timeout,
            idle_timeout=args.idle_timeout,
            commit_callbacks=commit_callbacks,
        )
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
        consumer = getattr(args, "consumer", None)
        receipt = await tasks.mutate(send_id, {"command": "send", "thread": row["id"], "body": body,
            "mode": getattr(args, "mode", "followup"), "model": args.model, "effort": args.effort},
            lambda: tasks.send(row, body, mode=getattr(args, "mode", "followup"), model=args.model,
                effort=args.effort, message_id=send_id, consumer=consumer))
        result = {**result, **receipt}
        if destination:
            result.update(tasks.launch_watcher(row, receipt["turnId"], destination, send_id, timeout=0))
        if args.wait:
            result.update(await tasks.wait(row, receipt["turnId"], args.timeout))
    return result


def main(argv=None):
    args = parser().parse_args(argv)
    commit_callbacks = []
    try:
        result = asyncio.run(run(args, commit_callbacks=commit_callbacks))
        # ASCII JSON survives Windows legacy console encodings; JSON decoders
        # recover the original Unicode titles, messages and paths exactly.
        print(json.dumps(result, ensure_ascii=True, indent=2), flush=True)
        for callback in commit_callbacks:
            callback()
        return status_code(result)
    except (UsageFailure, ConsumerBusyError) as error:
        print(json.dumps({"error": str(error), "errorType": type(error).__name__}, ensure_ascii=True), file=sys.stderr)
        return 2
    except (RuntimeFailure, RpcError, TransportError, OSError, ValueError, TimeoutError) as error:
        print(json.dumps({"error": str(error), "errorType": type(error).__name__}, ensure_ascii=True), file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print(json.dumps({"error": "Client interrupted; native task may still be running. Read before resending."}), file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())

"""Append-only file-backed inboxes, zero-polling AF_UNIX doorbells, and consumer cursors."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager, suppress
import copy
import json
import os
from pathlib import Path
import socket
import time
import uuid

from .state import FileLock, atomic_json, key, locked, private_dir, read_json


UNIX_DGRAM_SUPPORTED = os.name != "nt" and hasattr(socket, "AF_UNIX")
_MARKER_BOUNDARY_CHARS = frozenset(" \t:|]-),.;!?")


class ConsumerBusyError(RuntimeError):
    """Another wait process already holds the lease for this consumer."""


def validate_name(value: str, label: str) -> str:
    cleaned = (value or "").strip()
    if not cleaned or len(cleaned) > 128 or any(ord(ch) < 32 for ch in cleaned):
        raise ValueError(f"Invalid {label}: must be 1-128 printable characters")
    return cleaned


def extract_non_fenced_lines(text: str) -> list[str]:
    """Return lines outside Markdown triple-backtick or triple-tilde code fences."""
    lines: list[str] = []
    fence: str | None = None
    for raw_line in text.splitlines():
        stripped = raw_line.lstrip()
        if fence is None:
            if stripped.startswith("```") or stripped.startswith("~~~"):
                fence = stripped[:3]
                continue
            lines.append(raw_line)
        elif stripped.startswith(fence):
            fence = None
    return lines


def match_markers(text: str, markers: list[str], *, mode: str = "line") -> list[tuple[str, str]]:
    """Match configured markers in agent message text.

    In ``line`` mode (default), only lines outside Markdown code fences that start
    with the marker token match, preventing accidental triggers from inline prose
    mentions or quoted examples.
    """
    if not text or not markers:
        return []
    matches: list[tuple[str, str]] = []
    if mode == "substring":
        for marker in markers:
            if marker and marker in text:
                line = next((ln.strip() for ln in text.splitlines() if marker in ln), marker)
                matches.append((marker, line))
        return matches

    for raw_line in extract_non_fenced_lines(text):
        candidate = raw_line.strip()
        if not candidate:
            continue
        for marker in markers:
            if not marker or not candidate.startswith(marker):
                continue
            if len(candidate) == len(marker) or candidate[len(marker)] in _MARKER_BOUNDARY_CHARS:
                matches.append((marker, candidate))
                break
    return matches


def doorbell_prefix(channel: str) -> str:
    return f"ibx-{key(channel)[:8]}-"


def poke_doorbells(run_dir: Path, channel: str) -> int:
    """Best-effort wake all active AF_UNIX DGRAM doorbells for ``channel``."""
    if not UNIX_DGRAM_SUPPORTED or not run_dir.is_dir():
        return 0
    prefix = doorbell_prefix(channel)
    notified = 0
    sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
    sock.setblocking(False)
    try:
        for path in run_dir.glob(f"{prefix}*.sock"):
            try:
                sock.sendto(b"1", str(path))
                notified += 1
            except ConnectionRefusedError:
                # Only remove provably dead listener sockets.
                with suppress(OSError):
                    path.unlink(missing_ok=True)
            except (FileNotFoundError, BlockingIOError, OSError):
                continue
    finally:
        sock.close()
    return notified


class InboxDoorbell:
    """Zero-polling AF_UNIX DGRAM listener on POSIX; stat-polling fallback on Windows."""

    def __init__(self, run_dir: Path, state_dir: Path, channels: list[str], *, poll_interval: float = 0.25):
        self.run_dir = run_dir
        self.state_dir = state_dir
        self.channels = list(dict.fromkeys(validate_name(ch, "channel") for ch in channels))
        self.poll_interval = poll_interval
        self.event = asyncio.Event()
        self._sockets: list[tuple[socket.socket, Path]] = []
        self._loop: asyncio.AbstractEventLoop | None = None
        self._snapshots: dict[str, tuple[int, int]] = {}

    def _jsonl_path(self, channel: str) -> Path:
        return self.state_dir / "inboxes" / f"{key(channel)}.jsonl"

    def _stat_snapshot(self, channel: str) -> tuple[int, int]:
        try:
            st = self._jsonl_path(channel).stat()
            return (st.st_size, st.st_mtime_ns)
        except FileNotFoundError:
            return (0, 0)

    async def __aenter__(self):
        self._loop = asyncio.get_running_loop()
        if UNIX_DGRAM_SUPPORTED:
            private_dir(self.run_dir)
            for channel in self.channels:
                sock_path = self.run_dir / f"{doorbell_prefix(channel)}{os.getpid()}-{uuid.uuid4().hex[:6]}.sock"
                sock = socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM)
                sock.setblocking(False)
                try:
                    sock.bind(str(sock_path))
                    os.chmod(sock_path, 0o600)
                except BaseException:
                    sock.close()
                    with suppress(OSError):
                        sock_path.unlink(missing_ok=True)
                    raise
                self._sockets.append((sock, sock_path))
                self._loop.add_reader(sock.fileno(), self._on_readable, sock)
        else:
            for channel in self.channels:
                self._snapshots[channel] = self._stat_snapshot(channel)
        return self

    def _on_readable(self, sock: socket.socket):
        while True:
            try:
                sock.recv(1024)
            except (BlockingIOError, InterruptedError):
                break
            except OSError:
                break
        self.event.set()

    async def wait(self, timeout: float | None = None) -> bool:
        if UNIX_DGRAM_SUPPORTED:
            try:
                if timeout is None:
                    await self.event.wait()
                else:
                    await asyncio.wait_for(self.event.wait(), timeout)
                self.event.clear()
                return True
            except asyncio.TimeoutError:
                return False

        deadline = time.monotonic() + timeout if timeout is not None else None
        while True:
            changed = False
            for channel in self.channels:
                current = self._stat_snapshot(channel)
                if current != self._snapshots.get(channel):
                    self._snapshots[channel] = current
                    changed = True
            if changed or self.event.is_set():
                self.event.clear()
                return True
            if deadline is not None:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                sleep_for = min(self.poll_interval, remaining)
            else:
                sleep_for = self.poll_interval
            await asyncio.sleep(sleep_for)

    async def __aexit__(self, *_):
        for sock, sock_path in self._sockets:
            if self._loop is not None:
                with suppress(Exception):
                    self._loop.remove_reader(sock.fileno())
            with suppress(OSError):
                sock.close()
            with suppress(OSError):
                sock_path.unlink(missing_ok=True)
        self._sockets.clear()


class InboxStore:
    """Append-only JSONL inbox channels with monotonic high-watermark sequence numbers."""

    def __init__(self, state_dir: Path, run_dir: Path):
        self.state_dir = state_dir
        self.run_dir = run_dir
        self.root = state_dir / "inboxes"

    def events_path(self, channel: str) -> Path:
        return self.root / f"{key(validate_name(channel, 'channel'))}.jsonl"

    def meta_path(self, channel: str) -> Path:
        return self.root / f"{key(validate_name(channel, 'channel'))}.meta.json"

    def lock_path(self, channel: str) -> Path:
        return self.root / f"{key(validate_name(channel, 'channel'))}.lock"

    def _read_lines(self, channel: str) -> list[dict]:
        path = self.events_path(channel)
        if not path.is_file():
            return []
        rows: list[dict] = []
        with path.open("r", encoding="utf-8") as stream:
            for raw in stream:
                line = raw.strip()
                if not line:
                    continue
                try:
                    parsed = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(parsed, dict) and isinstance(parsed.get("seq"), int):
                    rows.append(parsed)
        return rows

    def latest_seq(self, channel: str) -> int:
        channel = validate_name(channel, "channel")
        meta = read_json(self.meta_path(channel), {}) or {}
        high = int(meta.get("lastSeq") or 0)
        rows = self._read_lines(channel)
        if rows:
            high = max(high, max(r["seq"] for r in rows))
        return high

    async def push(
        self,
        channel: str,
        *,
        kind: str,
        summary: str,
        sender: str | None = None,
        thread_id: str | None = None,
        phase: str | None = None,
        details_path: str | None = None,
        payload=None,
        request_id: str | None = None,
    ) -> dict:
        channel = validate_name(channel, "channel")
        kind = validate_name(kind, "kind")
        if not summary or not summary.strip():
            raise ValueError("Inbox summary must be non-empty")
        ch_key = key(channel)
        private_dir(self.root)
        async with locked(self.lock_path(channel), timeout=30):
            seq = self.latest_seq(channel) + 1
            now = time.time()
            record: dict = {
                "id": f"ibx:{ch_key[:8]}:{seq}",
                "seq": seq,
                "ts": now,
                "channel": channel,
                "kind": kind,
                "summary": summary,
            }
            if sender is not None:
                record["sender"] = sender
            if thread_id is not None:
                record["threadId"] = thread_id
            if phase is not None:
                record["phase"] = phase
            if details_path is not None:
                record["detailsPath"] = details_path
            if payload is not None:
                record["payload"] = payload
            if request_id is not None:
                record["requestId"] = request_id
            events_file = self.events_path(channel)
            with events_file.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(record, ensure_ascii=False, separators=(",", ":")) + "\n")
                stream.flush()
                os.fsync(stream.fileno())
            atomic_json(self.meta_path(channel), {"channel": channel, "lastSeq": seq, "updatedAt": now})
        notified = poke_doorbells(self.run_dir, channel)
        return {**record, "notifiedListeners": notified}

    def read(
        self,
        channel: str,
        *,
        after_seq: int = 0,
        kinds: list[str] | None = None,
        limit: int | None = None,
    ) -> tuple[list[dict], int]:
        channel = validate_name(channel, "channel")
        meta = read_json(self.meta_path(channel), {}) or {}
        high = int(meta.get("lastSeq") or 0)
        rows = self._read_lines(channel)
        if rows:
            high = max(high, max(r["seq"] for r in rows))
        kind_set = set(kinds) if kinds else None
        matched = [
            row
            for row in rows
            if row["seq"] > after_seq and (kind_set is None or row.get("kind") in kind_set)
        ]
        if limit is not None and limit >= 0:
            matched = matched[-limit:] if limit > 0 else []
        return matched, high


class CursorStore:
    """Unified consumer watermark store and non-blocking lease lock."""

    def __init__(self, state_dir: Path, run_dir: Path):
        self.state_dir = state_dir
        self.run_dir = run_dir
        self.root = state_dir / "cursors"

    def path(self, consumer: str) -> Path:
        return self.root / f"{key(validate_name(consumer, 'consumer'))}.json"

    def lock_path(self, consumer: str) -> Path:
        return self.root / f"{key(validate_name(consumer, 'consumer'))}.lock"

    def lease_path(self, consumer: str) -> Path:
        return self.run_dir / f"consumer-{key(validate_name(consumer, 'consumer'))}.lock"

    def load(self, consumer: str) -> dict:
        consumer = validate_name(consumer, "consumer")
        raw = read_json(self.path(consumer))
        if not isinstance(raw, dict):
            return {"consumer": consumer, "inboxes": {}, "threads": {}}
        data = copy.deepcopy(raw)
        data["consumer"] = consumer
        data.setdefault("inboxes", {})
        data.setdefault("threads", {})
        return data

    def save_sync(self, consumer: str, data: dict) -> None:
        consumer = validate_name(consumer, "consumer")
        payload = copy.deepcopy(data)
        payload["consumer"] = consumer
        payload["updatedAt"] = time.time()
        with FileLock(self.lock_path(consumer), timeout=30):
            atomic_json(self.path(consumer), payload)

    async def save(self, consumer: str, data: dict) -> None:
        consumer = validate_name(consumer, "consumer")
        payload = copy.deepcopy(data)
        payload["consumer"] = consumer
        payload["updatedAt"] = time.time()
        async with locked(self.lock_path(consumer), timeout=30):
            atomic_json(self.path(consumer), payload)

    async def ack_inbox(self, consumer: str, channel: str, seq: int) -> dict:
        consumer = validate_name(consumer, "consumer")
        channel = validate_name(channel, "channel")
        if seq < 0:
            raise ValueError("Sequence number must be nonnegative")
        async with locked(self.lock_path(consumer), timeout=30):
            data = self.load(consumer)
            data["inboxes"][channel] = {"lastSeq": seq}
            data["updatedAt"] = time.time()
            atomic_json(self.path(consumer), data)
            return {"consumer": consumer, "channel": channel, "lastSeq": seq}

    async def anchor_thread(
        self,
        consumer: str,
        thread_id: str,
        *,
        baseline_turn_id: str | None,
        acked_state: dict | None = None,
        seen_item_counts: dict[str, int] | None = None,
    ) -> dict:
        consumer = validate_name(consumer, "consumer")
        async with locked(self.lock_path(consumer), timeout=30):
            data = self.load(consumer)
            entry = {
                "baselineTurnId": baseline_turn_id,
                "seenItemCounts": dict(seen_item_counts or {}),
                "ackedState": acked_state,
                "disconnected": False,
            }
            data["threads"][thread_id] = entry
            data["updatedAt"] = time.time()
            atomic_json(self.path(consumer), data)
            return entry

    @asynccontextmanager
    async def lease(self, consumer: str):
        consumer = validate_name(consumer, "consumer")
        lock = FileLock(self.lease_path(consumer), timeout=0)
        try:
            lock.acquire()
        except TimeoutError as error:
            raise ConsumerBusyError(f"Consumer '{consumer}' is already locked by another wait process") from error
        try:
            yield
        finally:
            lock.release()

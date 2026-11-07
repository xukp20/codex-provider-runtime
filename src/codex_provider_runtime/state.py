"""Small atomic records and cross-process locks, without a second database."""
from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time


def key(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:24]


def private_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True, mode=0o700)
    return path


def read_json(path: Path, default=None):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return default


def atomic_json(path: Path, value) -> None:
    private_dir(path.parent)
    fd, name = tempfile.mkstemp(prefix=".new-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            json.dump(value, stream, ensure_ascii=False, indent=2)
            stream.write("\n")
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


class FileLock:
    def __init__(self, path: Path, timeout: float = 30):
        self.path, self.timeout, self.stream = path, timeout, None

    def acquire(self):
        private_dir(self.path.parent)
        self.stream = self.path.open("a+b")
        self.stream.seek(0, 2)
        if self.stream.tell() == 0:
            self.stream.write(b"\0")
            self.stream.flush()
        deadline = time.monotonic() + self.timeout
        while True:
            try:
                self.stream.seek(0)
                if os.name == "nt":
                    import msvcrt
                    msvcrt.locking(self.stream.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self.stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except (BlockingIOError, OSError):
                if time.monotonic() >= deadline:
                    self.stream.close()
                    self.stream = None
                    raise TimeoutError(f"Lock is busy: {self.path}")
                time.sleep(.05)

    def release(self):
        if self.stream is None:
            return
        if os.name == "nt":
            import msvcrt
            self.stream.seek(0)
            msvcrt.locking(self.stream.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(self.stream.fileno(), fcntl.LOCK_UN)
        self.stream.close()
        self.stream = None

    def __enter__(self):
        return self.acquire()

    def __exit__(self, *_):
        self.release()


@asynccontextmanager
async def locked(path: Path, timeout: float = 30):
    lock = FileLock(path, timeout)
    # Shield acquisition: cancellation must not leave a later acquired lock open.
    task = asyncio.create_task(asyncio.to_thread(lock.acquire))
    try:
        await asyncio.shield(task)
    except BaseException:
        await task
        lock.release()
        raise
    try:
        yield
    finally:
        lock.release()

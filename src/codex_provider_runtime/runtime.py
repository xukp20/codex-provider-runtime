"""Find the App endpoint; ensure at most one owned local server per provider."""
from __future__ import annotations

import asyncio
import csv
from dataclasses import asdict, dataclass
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
import tomllib

import psutil

from .state import atomic_json, key, locked, private_dir, read_json
from .transport import Rpc, RpcError, TransportError


class RuntimeFailure(RuntimeError):
    pass


def discover_binary(explicit: str | None = None) -> str:
    candidate = explicit or os.environ.get("CPR_CODEX_BINARY")
    if candidate:
        found = shutil.which(candidate) or candidate
        if not Path(found).is_file():
            raise RuntimeFailure("Configured Codex binary does not exist")
        return str(Path(found).absolute())
    # Prefer the App's actual bundled native daemon on Windows over an older PATH CLI.
    if os.name == "nt":
        home = Path(os.environ.get("CODEX_HOME") or Path.home()/".codex")
        daemons = sorted(home.glob("packages/app-server-daemon/releases/*/bin/codex.exe"),
                         key=lambda p: p.stat().st_mtime, reverse=True)
        if daemons:
            return str(daemons[0])
        root = Path(os.environ.get("LOCALAPPDATA", ""))
        found = sorted(root.glob("OpenAI/Codex/**/codex.exe"), key=lambda p: p.stat().st_mtime, reverse=True)
        if found:
            return str(found[0])
    found = shutil.which("codex")
    if not found:
        raise RuntimeFailure("Codex binary unavailable; use --binary or CPR_CODEX_BINARY")
    return found


def private_socket_directory(path: Path):
    if path.is_symlink():
        raise RuntimeFailure("Runtime socket directory must not be a symbolic link")
    private_dir(path)
    if os.name == "nt":
        # Native Windows AF_UNIX validates owner and ACL, not Python's mode bits.
        identity = subprocess.run(["whoami", "/user", "/fo", "csv", "/nh"], capture_output=True, text=True, check=True)
        sid = next(csv.reader(identity.stdout.strip().splitlines()))[-1]
        subprocess.run(["icacls", str(path), "/setowner", "*"+sid], capture_output=True, check=True)
        subprocess.run(["icacls", str(path), "/inheritance:r", "/grant:r", "*"+sid+":(OI)(CI)F"], capture_output=True, check=True)


@dataclass
class Endpoint:
    provider: str
    socket: str
    binary: str
    owned: bool = False
    pid: int | None = None
    version: str | None = None

    def rpc(self, timeout: float = 15):
        return Rpc(self.socket, self.binary, timeout)


class Runtime:
    def __init__(self, home=None, binary=None, app_socket=None):
        self.home = Path(home or os.environ.get("CODEX_HOME") or Path.home()/".codex").expanduser().resolve()
        self.binary = discover_binary(binary)
        self.app_socket = str(app_socket or os.environ.get("CODEX_APP_SERVER_SOCKET") or self.home/"app-server-control"/"app-server-control.sock")
        self.state = private_dir(self.home/"provider-runtime")
        # Keep AF_UNIX paths short. This is transient machine-local state.
        base = Path(os.environ.get("CPR_RUNTIME_DIR") or tempfile.gettempdir())
        self.run = private_dir(base/("cpr-"+key(str(self.home)+str(os.getuid() if hasattr(os, "getuid") else Path.home()))[:12]))
        private_socket_directory(self.run)
        self.config = self._read_config()
        profile = self.config.get("profiles", {}).get(self.config.get("profile"), {})
        self.default_provider = profile.get("model_provider", self.config.get("model_provider", "openai"))
        self._app_checked = False

    def _read_config(self):
        try:
            return tomllib.loads((self.home/"config.toml").read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {}

    async def app(self) -> Endpoint:
        ep = Endpoint(self.default_provider, self.app_socket, self.binary)
        async with ep.rpc(timeout=3) as c:
            response = await c.request("config/read", {"includeLayers": False})
            provider = response.get("config", {}).get("model_provider") or self.default_provider
            self.default_provider = provider
            ep.provider = provider
        self._app_checked = True
        return ep

    def record_path(self, provider):
        return self.run/(key(provider)+".json")

    def record(self, provider):
        return read_json(self.record_path(provider))

    @staticmethod
    def process_matches(record):
        if not record or not record.get("owned"):
            return False
        try:
            process = psutil.Process(record["pid"])
            return (abs(process.create_time()-record["created"]) < .01
                    and record["socket"] in " ".join(process.cmdline())
                    and "app-server" in process.cmdline())
        except (psutil.Error, KeyError):
            return False

    async def _probe(self, ep: Endpoint):
        async with ep.rpc(timeout=3) as c:
            response = await c.request("config/read", {"includeLayers": False})
            actual = response.get("config", {}).get("model_provider") or "openai"
            if actual != ep.provider:
                raise RuntimeFailure(f"Endpoint provider mismatch: expected {ep.provider}, received {actual}")
        return ep

    async def endpoint(self, provider: str, *, start: bool = True) -> Endpoint:
        # Check the running App's effective provider, including startup overrides.
        if not self._app_checked:
            try:
                await self.app()
            except (TransportError, RpcError, OSError):
                self._app_checked = True
        if provider == self.default_provider:
            try:
                return await self.app()
            except (TransportError, RpcError, OSError) as error:
                raise RuntimeFailure(f"Default App endpoint unavailable for {provider}; reconnect the App") from error
        providers = self.config.get("model_providers", {})
        if provider != "openai" and provider not in providers:
            raise RuntimeFailure(f"Unknown native provider ID: {provider}")
        async with locked(self.run/(key(provider)+".lock")):
            record = self.record(provider)
            if record:
                ep = Endpoint(**{k: record.get(k) for k in Endpoint.__dataclass_fields__})
                try:
                    await self._probe(ep)
                    ep.owned = self.process_matches(record)
                    return ep
                except (TransportError, RpcError, OSError):
                    if self.process_matches(record):
                        raise RuntimeFailure(f"Managed {provider} process is alive but unreachable; inspect server status")
            if not start:
                raise RuntimeFailure(f"No running local server for {provider}")
            sock = str(self.run/(key(provider)[:12]+".sock"))
            # A restored/missing record must not make an existing socket our process.
            try:
                return await self._probe(Endpoint(provider, sock, self.binary))
            except (TransportError, RpcError, OSError):
                pass
            try:
                Path(sock).unlink(missing_ok=True)
            except OSError as error:
                raise RuntimeFailure("Cannot remove stale owned runtime socket") from error
            command = [self.binary, "-c", "model_provider="+json.dumps(provider), "app-server", "--listen", "unix://"+sock]
            env = {**os.environ, "CODEX_HOME": str(self.home)}
            kwargs = {"start_new_session": True} if os.name != "nt" else {
                "creationflags": subprocess.DETACHED_PROCESS | subprocess.CREATE_NEW_PROCESS_GROUP}
            log_path = self.run/(key(provider)+".log")
            with log_path.open("ab") as log:
                process = subprocess.Popen(command, env=env, stdin=subprocess.DEVNULL, stdout=log, stderr=log, **kwargs)
            record = None
            try:
                created = psutil.Process(process.pid).create_time()
                version = subprocess.run([self.binary, "--version"], capture_output=True, text=True, timeout=10).stdout.strip()
                ep = Endpoint(provider, sock, self.binary, True, process.pid, version)
                record = {**asdict(ep), "created": created, "log": str(log_path)}
                atomic_json(self.record_path(provider), record)
                deadline = time.monotonic()+30
                while time.monotonic() < deadline:
                    if process.poll() is not None:
                        raise RuntimeFailure(f"Native server exited ({process.returncode}); inspect {log_path}")
                    try:
                        return await self._probe(ep)
                    except (TransportError, RpcError, OSError):
                        await asyncio.sleep(.15)
                raise RuntimeFailure(f"Native server did not become ready; inspect {log_path}")
            except BaseException:
                if process.poll() is None and self.process_matches(record):
                    process.terminate()
                    await asyncio.to_thread(process.wait, 10)
                self.record_path(provider).unlink(missing_ok=True)
                raise

    async def catalog(self):
        try:
            return await self.app()
        except (TransportError, RpcError, OSError):
            for path in self.run.glob("*.json"):
                record = read_json(path)
                ep = Endpoint(**{k: record.get(k) for k in Endpoint.__dataclass_fields__})
                try:
                    return await self._probe(ep)
                except (TransportError, RpcError, OSError):
                    continue
        raise RuntimeFailure("No reachable catalog endpoint; connect the App or ensure a provider server")

    async def status(self):
        rows = []
        try:
            rows.append({**asdict(await self.app()), "reachable": True, "kind": "app"})
        except (TransportError, RpcError, OSError):
            rows.append({"provider": self.default_provider, "socket": self.app_socket, "owned": False, "kind": "app", "reachable": False})
        for path in self.run.glob("*.json"):
            record = read_json(path)
            ep = Endpoint(**{k: record.get(k) for k in Endpoint.__dataclass_fields__})
            try:
                await self._probe(ep)
                reachable = True
            except (TransportError, RpcError, OSError):
                reachable = False
            rows.append({**asdict(ep), "owned": self.process_matches(record), "reachable": reachable, "kind": "managed", "log": record.get("log")})
        return rows

    async def stop(self, provider):
        # Stop is deliberately restricted to our recorded process identity.
        try:
            await self.app()
        except (TransportError, RpcError, OSError):
            pass
        if provider == self.default_provider:
            raise RuntimeFailure("The default App server is never stopped by this tool")
        async with locked(self.run/(key(provider)+".lock")):
            record = self.record(provider)
            if not self.process_matches(record):
                raise RuntimeFailure("No matching owned process; refusing to stop a foreign or stale PID")
            ep = Endpoint(**{k: record.get(k) for k in Endpoint.__dataclass_fields__})
            async with ep.rpc() as c:
                r = await c.request("thread/loaded/list", {})
                for tid in r.get("data", []):
                    row = await c.request("thread/read", {"threadId": tid, "includeTurns": True})
                    if any(t.get("status") == "inProgress" for t in row["thread"].get("turns", [])):
                        raise RuntimeFailure("Owned server has active tasks; interrupt the intended task first")
            process = psutil.Process(record["pid"])
            process.terminate()
            await asyncio.to_thread(process.wait, 15)
            self.record_path(provider).unlink(missing_ok=True)
            Path(record["socket"]).unlink(missing_ok=True)
            return {"provider": provider, "stopped": True, "pid": record["pid"]}

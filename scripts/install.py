#!/usr/bin/env python3
"""Install real source trees and vendor dependencies without changing Codex config.

Dependencies are installed with --python into runtime-deps, outside any virtual
environment, and included in the installed source tree for backup/restore on the
same platform and Python ABI. Reinstall for another platform/Python ABI. Every
selected target is prepared before activation. Updates use same-parent renames
with rollback: replacing an existing real directory requires two renames, so it
is not a crash-atomic multi-home transaction. Old trees remain as rollback copies.
--skip-deps is for controlled tests or environments providing dependencies.
"""
from __future__ import annotations

import argparse
from contextlib import contextmanager, ExitStack
import datetime
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import tomllib
import uuid


NAME = "codex-provider-runtime"
CODEX_SKILLS = ("codex-provider-tasks", "codex-provider-external", "codex-delegation")
FILES = ("README.md", "pyproject.toml", "docs/usage.md", "scripts/run.py", "scripts/install.py")


class InstallError(RuntimeError):
    pass


def exists(path):
    return os.path.lexists(path)


def is_link(path):
    if path.is_symlink():
        return True
    # Python 3.11 has no Path.is_junction; Windows junctions are reparse points.
    return os.name == "nt" and bool(getattr(path.lstat(), "st_file_attributes", 0) & 0x400)


def remove_link(path):
    if path.is_symlink():
        path.unlink()
    else:
        path.rmdir()  # Removes a Windows junction itself, never its target.


@contextmanager
def install_lock(parent):
    path = parent / ".codex-provider-runtime-install.lock"
    try:
        descriptor = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
    except FileExistsError as error:
        raise InstallError(f"Installer lock exists: {path}; check for an active installer before removing a stale lock") from error
    try:
        os.write(descriptor, str(os.getpid()).encode())
        os.close(descriptor)
        yield
    finally:
        path.unlink(missing_ok=True)


def copy_source(source, destination):
    """Copy the public allowlist, rejecting links to untracked external content."""
    def copy(path, target):
        if path.is_symlink() or not path.resolve().is_relative_to(source):
            raise InstallError(f"Source symlinks are not installable: {path.relative_to(source)}")
        if path.is_dir():
            target.mkdir(mode=0o700, parents=True, exist_ok=True)
            for child in sorted(path.iterdir()):
                if child.name in {"__pycache__", ".git", ".venv", ".pytest_cache"} or child.suffix in {".pyc", ".pyo"}:
                    continue
                copy(child, target / child.name)
        elif path.is_file():
            target.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
            shutil.copy2(path, target)
        else:
            raise InstallError(f"Missing or unsupported source input: {path}")

    for relative in FILES:
        copy(source / relative, destination / relative)
    copy(source / "src" / "codex_provider_runtime", destination / "src" / "codex_provider_runtime")
    for name in CODEX_SKILLS:
        copy(source / "skills" / name, destination / "skills" / name)
        if not (destination / "skills" / name / "SKILL.md").is_file():
            raise InstallError(f"Missing skill: {name}")
    for name in ("LICENSE", "LICENSE.md", "LICENSE.txt"):
        if (source / name).is_file():
            copy(source / name, destination / name)


def install_dependencies(stage, python):
    metadata = tomllib.loads((stage / "pyproject.toml").read_text(encoding="utf-8"))
    dependencies = metadata.get("project", {}).get("dependencies", [])
    target = stage / "runtime-deps"
    if dependencies:
        result = subprocess.run([
            python, "-m", "pip", "install", "--disable-pip-version-check", "--no-input",
            "--upgrade", "--target", str(target), *dependencies,
        ], capture_output=True, text=True)
        if result.returncode:
            # pip output can include private index credentials; do not echo it.
            raise InstallError("Dependency installation failed; existing installations were retained")
    target.mkdir(mode=0o700, exist_ok=True)
    checked = subprocess.run([
        python, "-I", "-B", "-c",
        "import sys; from pathlib import Path; assert sys.version_info >= (3, 11); "
        "sys.path.insert(0, sys.argv[1]); import websockets, wsproto, psutil; "
        "assert all(Path(m.__file__).resolve().is_relative_to(Path(sys.argv[1]).resolve()) "
        "for m in (websockets, wsproto, psutil))",
        str(target),
    ], capture_output=True, text=True)
    if checked.returncode:
        raise InstallError("Vendored dependency import verification failed")


def check_skill_link(home, name, target):
    link = home / "skills" / name
    if exists(link):
        if not is_link(link) or link.resolve() != target.resolve():
            raise InstallError(f"Refusing to replace an unrelated skill: {link}")
    return link


def link_skill(home, name, target):
    link = check_skill_link(home, name, target)
    if exists(link):
        if not (link / "SKILL.md").is_file():
            raise InstallError(f"Existing skill link is incomplete: {link}")
        return None
    link.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    temporary = link.with_name(".new-" + name + "-" + uuid.uuid4().hex)
    try:
        try:
            temporary.symlink_to(os.path.relpath(target, link.parent), target_is_directory=True)
        except OSError:
            if os.name != "nt":
                raise
            # Junctions need no symlink privilege and retain one self-contained tree.
            result = subprocess.run(["cmd", "/d", "/c", "mklink", "/J", str(temporary), str(target)],
                                    capture_output=True, text=True)
            if result.returncode:
                raise InstallError(f"Cannot create a skill symlink or junction: {link}")
        if temporary.resolve() != target.resolve() or not (temporary / "SKILL.md").is_file():
            raise InstallError(f"Skill link verification failed: {link}")
        os.replace(temporary, link)
        return link
    finally:
        if exists(temporary):
            remove_link(temporary)


def install(source, codex_home, claude_home, target="both", python=sys.executable, skip_deps=False):
    source = Path(source).expanduser().resolve()
    selected = []
    for kind, home in (("codex", codex_home), ("claude", claude_home)):
        if target not in (kind, "both"):
            continue
        home = Path(home).expanduser().resolve()
        selected.append({"kind": kind, "home": home, "destination": home / "tool-sources" / NAME,
                         "skills": CODEX_SKILLS if kind == "codex" else ("codex-provider-external",)})
    if len({item["home"] for item in selected}) != len(selected):
        raise InstallError("Codex and Claude homes must be distinct")
    for item in selected:
        destination = item["destination"]
        if exists(destination) and (is_link(destination) or not destination.is_dir()):
            raise InstallError(f"Runtime destination must be a real directory: {destination}")
        for name in item["skills"]:
            check_skill_link(item["home"], name, destination / "skills" / name)
    stages, activated, created_links = [], [], []
    stamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    with ExitStack() as locks:
        try:
            for item in sorted(selected, key=lambda row: str(row["home"])):
                parent = item["destination"].parent
                parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                locks.enter_context(install_lock(parent))
            for item in selected:
                stage = Path(tempfile.mkdtemp(prefix=".cpr-stage-", dir=item["destination"].parent))
                item["stage"] = stage
                stages.append(stage)
                copy_source(source, stage)
            if not skip_deps:
                install_dependencies(selected[0]["stage"], str(python))
                for item in selected[1:]:
                    shutil.copytree(selected[0]["stage"] / "runtime-deps", item["stage"] / "runtime-deps")
            # Activate only after every selected source/dependency tree is ready.
            for item in selected:
                destination = item["destination"]
                backup = destination.with_name(NAME + ".before-" + stamp) if destination.exists() else None
                item["backup"] = backup
                if backup is not None:
                    os.replace(destination, backup)
                try:
                    os.replace(item["stage"], destination)
                except BaseException:
                    if backup is not None:
                        os.replace(backup, destination)
                    raise
                activated.append(item)
            for item in selected:
                for name in item["skills"]:
                    link = link_skill(item["home"], name, item["destination"] / "skills" / name)
                    if link is not None:
                        created_links.append(link)
            return {"installed": [{"target": item["kind"], "path": str(item["destination"]),
                      "backup": str(item["backup"]) if item["backup"] else None,
                      "skills": list(item["skills"])} for item in selected],
                    "dependencies": "skipped" if skip_deps else "vendored", "python": str(python)}
        except BaseException:
            for link in reversed(created_links):
                remove_link(link)
            for item in reversed(activated):
                os.replace(item["destination"], item["stage"])
                if item["backup"] is not None:
                    os.replace(item["backup"], item["destination"])
            raise
        finally:
            for stage in stages:
                if stage.exists():
                    shutil.rmtree(stage)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--source", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--codex-home", type=Path, default=Path(os.environ.get("CODEX_HOME", str(Path.home() / ".codex"))))
    parser.add_argument("--claude-home", type=Path, default=Path.home() / ".claude")
    parser.add_argument("--target", choices=("codex", "claude", "both"), default="both")
    parser.add_argument("--python", default=sys.executable)
    parser.add_argument("--skip-deps", action="store_true", help="Skip vendoring; tests or externally supplied dependency environments only")
    args = parser.parse_args(argv)
    try:
        result = install(args.source, args.codex_home, args.claude_home, args.target, args.python, args.skip_deps)
        print(json.dumps(result, indent=2))
        return 0
    except (InstallError, OSError, ValueError) as error:
        print(json.dumps({"error": str(error), "errorType": type(error).__name__}), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

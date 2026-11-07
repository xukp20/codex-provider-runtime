#!/usr/bin/env python3
"""Run the shared CLI from a source checkout or installed runtime tree."""
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "src"), str(ROOT / "runtime-deps")]

from codex_provider_runtime.cli import main


if __name__ == "__main__":
    raise SystemExit(main())

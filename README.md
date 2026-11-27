# Codex Provider Runtime

Manage local Codex tasks through native App Server connections. The default App is reused; other native providers get one reusable local service each. Existing thread metadata selects the provider automatically.

The runtime uses native Codex provider configuration and credentials. It stores service identity and operation receipts, with no private provider catalog, global routing policy, external harness runner, or compatibility wrappers. Native subagents inherit their parent's provider and select models and effort directly.

Use Python 3.11 or newer and a compatible Codex binary. Install the project's declared dependencies in your chosen Python environment, then run from the repository root:

```bash
python scripts/run.py doctor
python scripts/run.py find --name "Task title"
python scripts/run.py create --provider PROVIDER_ID --title "A task" --cwd /absolute/project --message "The task brief" --wait
```

Command results and runtime errors emit JSON; standard help and usage errors remain text. Full access and no approvals are fixed task settings; unknown native requests remain unanswered. A completed delivery and a completed target turn are separate outcomes. Additional services may be usable without appearing in the App UI.

The three skills are [codex-provider-tasks](skills/codex-provider-tasks/SKILL.md), [codex-provider-external](skills/codex-provider-external/SKILL.md), and [codex-delegation](skills/codex-delegation/SKILL.md). Codex uses all three; external agents use the external skill and the same implementation. Delegation reads the user's division document only when needed.

Install self-contained source and dependencies for Codex, Claude, and optional extra external agent homes (e.g. `~/.gemini/config`):

```bash
python scripts/install.py --target both --external-home ~/.gemini/config
```

The installer links all three Codex skills and only the external skill for Claude and `--external-home` targets. Dependencies are vendored inside each home for backup; reinstall when changing platform or Python ABI. Existing source trees are retained as rollback copies. It does not change native Codex configuration or remove older tools.

See [usage](docs/usage.md) for CLI operations, delivery semantics, multi-target `wait` multiplexing, and structured `inbox` channels. Tested with native Codex 0.167.0 on Linux and Windows. Run `python -m pytest -q`; native integration tests additionally need `CPR_TEST_BINARY` pointing to a native Codex executable and use local mock providers only.

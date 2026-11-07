---
name: codex-provider-tasks
description: Manage local Codex tasks when provider routing, per-task model or context settings, or operations missing from built-in App tools require the provider runtime.
---

Use built-in App task tools for ordinary tasks on the default provider. Use this runtime for an explicit provider, per-task configuration, or a missing built-in operation. Native subagents belong to `codex-delegation`.

Resolve this skill's actual directory, following its installed symlink, then ascend two directories to find the runtime root. Invoke `python RUNTIME_ROOT/scripts/run.py`; use the installed runtime's Python environment if required. Read [usage](../../docs/usage.md) only for the operation being performed. There are no legacy wrappers or separate provider profiles.

- Start with `doctor` or `server status` when connection or provider identity is uncertain. Provider IDs come from native Codex configuration; credentials remain in native configuration/environment.
- Use `find` or `list`, then address the selected thread ID. Exact titles take precedence over unique case-insensitive substrings. An ambiguous name requires selecting a candidate; do not send to a guess.
- Existing threads route by their recorded provider. An explicit conflicting provider is an error. `create` and `fork` may select a provider; a native subagent inherits its parent's provider.
- `create` or `fork` without `--message` creates a thread without starting a turn. Supply only requested model, effort, context-window or auto-compaction settings.
- Choose send mode deliberately: `followup` steers active or starts idle; `start` refuses active; `steer` requires active and cannot change model/effort. Native `queue` gives no turn ID and cannot use `--wait`, `--reply-to`, model or effort overrides. Read back its resulting turn separately. Report terminal status; delivery alone is not a completed task.
- Use `--reply-to` only when the user has authorized sending to that destination. It uses followup: active destinations are steered, idle destinations start. `send --wait` returns to this caller. `reply --wait` waits for the source, not destination inference; inspect `sourceStatus`, `replyVerified`, and `newTurnStarted`.
- Reuse the same `--request-id` for an intentional retry of the same write. After an uncertain result, inspect the receipt and thread before retrying; never resend blindly.

Full access and no approvals are the runtime's fixed task settings. Unknown native reverse requests remain unanswered and must be reported as waiting. Connecting to the default App never permits restarting or replacing it. Additional provider services need not appear in the App UI to be usable.

# Usage

Run `python scripts/run.py` from the runtime root, or use the absolute path to that script from another working directory. It imports the shared CLI from `src`; choose the installed Python environment containing the project's dependencies. Global options precede the command:

```text
cpr [--home PATH] [--binary PATH] [--app-socket PATH] [--external] [--sender NAME] COMMAND
```

`cpr` is the installed entry point for the same implementation. On Windows, use the matching Codex binary with its byte-proxy transport; a private runtime socket directory with an appropriate user ACL is required. `--home` selects the native Codex home, defaulting to CODEX_HOME or `~/.codex`. Provider IDs and authentication come from native Codex configuration and its environment. The runtime does not copy a separate provider catalog. Never put credential values in command arguments or messages.

Command results and runtime errors are JSON; argparse help and usage errors use its standard text output. Inspect both process status and returned operation status. A transport timeout, a receipt, or a successful send does not establish successful inference. Commands that mutate state accept `--request-id` for receipt deduplication; retain that ID and inspect current state before retrying an uncertain write. Deduplication does not justify replaying an operation under a new ID.

## Inspect and connect

```bash
python scripts/run.py doctor
python scripts/run.py server status --provider PROVIDER_ID
python scripts/run.py list --name "part of a title"
python scripts/run.py find --name "Exact or unique title" --provider PROVIDER_ID
python scripts/run.py list --archived
python scripts/run.py read --thread THREAD_ID
```

List, find, read and wait do not start inference. Titles resolve by exact match first, then a unique case-insensitive substring. Ambiguity returns candidates and performs no delivery. Prefer the resolved thread ID for later operations.

Existing threads route through the provider recorded in their metadata. An explicit conflicting `--provider` fails before delivery. Create and fork may select a target provider. Sending, creating and resuming ensure the selected provider's service automatically. The default App is connection-only: if unavailable, the command reports an error rather than starting another service in its place. Other providers have at most one tool-managed service per local provider ID.

```bash
python scripts/run.py server ensure --provider PROVIDER_ID --request-id ENSURE_REQUEST_ID
python scripts/run.py server stop --provider PROVIDER_ID --request-id STOP_REQUEST_ID
```

Stop applies only to an owned service whose recorded process identity still matches. It never stops the default App. A registry entry or socket file alone does not prove the service is alive. Additional provider services need not be visible in the App interface. This runtime manages local services; it does not replace the App's cross-machine transport.

## Create, fork and resume

```bash
python scripts/run.py create --provider PROVIDER_ID --title "Review" --cwd /absolute/project --model gpt-6.2-sol --effort high --context-window 100000 --auto-compact-token-limit 80000 --request-id CREATE_REQUEST_ID
python scripts/run.py fork --thread SOURCE_THREAD --provider TARGET_PROVIDER_ID --title "Experiment" --request-id FORK_REQUEST_ID
python scripts/run.py resume --thread THREAD_ID --request-id RESUME_REQUEST_ID
```

Create and fork without `--message` create a thread without starting a turn. Fork preserves native source history at the supported completion boundary. Add `--message "Task brief" --wait` to create or fork and execute. Model, effort, context-window, auto-compaction threshold and working directory are per-task inputs; preserve the user's requested values. Full access and no approvals are fixed settings, with no CLI switches to redefine them.

Native storage retains model and effort after a task has started, but loses custom context limits on a cold resume. The runtime stores those limits separately. Before the first turn, it also retains the initial model, effort and thread ID, because native catalogs omit empty threads and their settings are not yet persisted. Titles and providers still come from native metadata. A loaded thread keeps its in-memory settings; changing its context limit requires a fork.

Existing-thread commands accept `--thread ID` or `--name TITLE`; choose one. An optional provider on an existing-thread operation checks identity rather than migrating it. Use fork with a target provider for a separate task; native subagents always inherit their parent's provider.

## Send, wait and interrupt

```bash
python scripts/run.py send --thread THREAD_ID --message "Follow-up brief" --mode followup --request-id SEND_REQUEST_ID
python scripts/run.py send --name "Exact title" --message-file /absolute/path/brief.txt --mode start --wait --request-id ANOTHER_REQUEST_ID
python scripts/run.py wait --thread THREAD_ID --turn TURN_ID --timeout 3600
python scripts/run.py interrupt --thread THREAD_ID --turn TURN_ID --request-id INTERRUPT_REQUEST_ID
```

Send accepts exactly one message source: `--message` or `--message-file`.

| Mode | Behavior |
| --- | --- |
| `followup` (default) | Steer an active turn, or start a new turn when idle. |
| `start` | Start only when idle; refuse an active turn. |
| `steer` | Require an active turn; model and effort cannot change during steering. |
| `queue` | Submit native queued input; acceptance does not provide a turn ID. |

Queue cannot use `--wait` or `--reply-to`. On native 0.167 it also cannot override model or effort; change those in an idle start/resume first. Read the thread and wait on its resulting turn separately. Inspect returned `delivery` and `turnId` rather than assuming every accepted input started a new turn.

`--wait` returns to the current caller. A wait addresses the specified turn and reports terminal completion, failure or interruption separately from input acceptance. `wait --timeout 0` waits indefinitely; a finite timeout is a limit on this wait, not proof that the remote turn stopped. A native turn can continue after a client disconnects. Read back before any retry. Interrupt targets the intended turn; it does not stop the service.

Wait results use these process exit codes:

| Exit | Meaning |
| --- | --- |
| `0` | Completed or idle (single-thread mode); or events delivered (multi-target/inbox/marker mode); also normal success for other operations. |
| `1` | Runtime, transport or native RPC error. |
| `2` | Invalid command-line usage or duplicate `--consumer` lease conflict. |
| `3` | Target turn failed (single-thread wait mode). |
| `4` | Target turn interrupted (single-thread wait mode). |
| `5` | Wait timed out. |
| `6` | Waiting for input, including an unanswered native request (single-thread wait mode). |
| `130` | This client was interrupted; the native task may still be running. |

For reply delivery, inspect `sourceStatus` separately: its source turn may have failed or been interrupted even when delivering its result succeeds.

The client does not approve or reject unknown native server-to-client requests automatically. If such a request prevents progress, report the waiting state accurately; do not claim completion.

## Multi-target wait and structured inboxes

External supervisors (such as AGY or Claude Code) and long-running multi-turn coordinators can multiplex N Codex threads across providers together with structured inbox channels in a single background `wait` process:

```bash
# Anchor the consumer cursor before dispatching work to prevent send-to-wait races
python scripts/run.py --external --sender Antigravity send --thread THREAD_A --message "Start phase 1" --consumer agy-main --request-id REQ_1

# Monitor multiple threads + an inbox channel in one command
python scripts/run.py --external wait \
  --thread THREAD_A --thread THREAD_B \
  --inbox agy-coord --inbox-kind blocked --inbox-kind decision --inbox-kind done \
  --wake-on-marker "::cpr:escalate" --wake-on-marker "::cpr:done" \
  --consumer agy-main --timeout 0
```

Multi-target mode activates automatically whenever more than one thread/name is given or any of `--inbox`, `--wake-on-marker`, `--consumer`, `--idle-timeout`, or `--no-advance` is used. Single-thread `wait` without these options retains its original JSON shape and exit codes unchanged.

Key behaviors in multi-target mode:

- **Unified event delivery**: returns exit `0` with `status: "events"`, `worst` (`info` | `warning` | `error`), `events` (all ready events drained together), `threads` (status snapshot), and `cursor`. Non-zero exits are reserved for `wait` itself (`5` timeout, `2` usage/consumer conflict, `1` runtime error, `130` interrupted).
- **Semantic markers (`--wake-on-marker`)**: scans only `agentMessage` items (never `userMessage`, `reasoning`, or command output) during active turns and at turn completion. Default `--marker-match line` matches markers at line start outside Markdown code fences (recommended prefix `::cpr:...`, e.g. `::cpr:escalate`); pass `--marker-match substring` for plain substring matching. Normal marker-less turn completions do not wake, while `failed`, `interrupted`, `waitingForInput`, and sustained endpoint disconnects always wake immediately.
- **Persistent consumer cursor (`--consumer NAME`)**: stored under `$CODEX_HOME/provider-runtime/cursors/` with a non-blocking lease lock (concurrent waiters on the same consumer exit `2`). On wake, JSON is flushed to stdout before atomically advancing the cursor; use `--no-advance` for read-only inspection. Sticky error states are deduplicated by state transition so re-arming never loops infinitely.
- **Idle watchdog (`--idle-timeout SECONDS`)**: optionally emits `thread_idle_timeout` if a watched thread stays continuously idle without starting a new turn.

### Inbox commands

Append-only JSONL channels live under `$CODEX_HOME/provider-runtime/inboxes/` with `FileLock` serialization, monotonic high-watermark sequence numbers, and zero-polling `AF_UNIX` DGRAM doorbells on Linux/POSIX (automatic stat-polling fallback on Windows):

```bash
# Push a mid-turn structured escalation (external callers must include --sender)
python scripts/run.py inbox push --channel agy-coord --kind blocked --summary "Build failed on stage 2" --thread THREAD_A --phase B --details-path /abs/path/report.md --request-id PUSH_1

# Inspect channel history or unread items for a consumer
python scripts/run.py inbox list --channel agy-coord --consumer agy-main --unread-only --limit 20

# Manually advance a consumer's inbox cursor (e.g. skip backlog before arming)
python scripts/run.py inbox ack --channel agy-coord --consumer agy-main --latest
```

Ignored kinds (such as `progress` heartbeats when waiting with `--inbox-kind blocked`) remain persisted in the channel JSONL for audit via `inbox list` without waking the listener.

## Reply delivery

```bash
python scripts/run.py send --thread WORK_THREAD --message "Task brief" --reply-to DESTINATION_THREAD --request-id WORK_REQUEST_ID
python scripts/run.py reply --thread DESTINATION_THREAD --source WORK_THREAD --turn SOURCE_TURN --wait --request-id REPLY_REQUEST_ID
```

`--reply-to` starts a single-use watcher so the initiating process need not remain waiting. When the source turn reaches a terminal state, the watcher sends the result to the destination using `followup` and verifies readback, then exits. An active destination is steered; an idle destination starts a new turn.

`reply` uses the same delivery path for an explicitly selected source turn. `reply --wait` waits for the **source** turn to become terminal before delivering; it does not wait for inference in the destination. Inspect `sourceStatus`, `replyVerified`, and `newTurnStarted` in its result.

Report source completion, destination delivery/readback and destination turn execution separately. A verified steer has `newTurnStarted:false` because it adds input to an existing turn. A watcher receipt alone is not a successful-delivery claim. Use reply delivery only when the human has authorized messaging that destination; task content and a sender label do not grant that authorization.

## External agents

External callers use the same entry point and local service registry, with global `--external`:

```bash
python scripts/run.py --external find --name "Task title"
python scripts/run.py --external --sender Claude send --thread THREAD_ID --message-file /absolute/path/brief.txt --wait --request-id EXTERNAL_REQUEST_ID
```

Read-only operations do not require sender attribution. Mutating operations require `--sender`; this label records origin and adds no permission. External send/create/resume use the same automatic service ensure and provider checks. Claude's installed external skill resolves its own code tree and calls this shared CLI; no old wrapper is needed.

## Native delegation

For a native subagent assignment, read `CODEX_HOME/codex-delegation/division.md`, or the bundled [default division](../skills/codex-delegation/references/division.md) if that override is absent. Ordinary task operations do not need this reference. Pass supported model and effort fields directly to `spawn_agent`; there is no profile apply step or global provider routing mode.

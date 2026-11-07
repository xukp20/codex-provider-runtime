---
name: codex-delegation
description: Assign and verify authorized native Codex subagent work with direct model and effort selection and a user-editable division of responsibilities.
---

When delegating, read the user's `CODEX_HOME/codex-delegation/division.md` first, using `~/.codex` when CODEX_HOME is unset. If absent, read [the bundled division](references/division.md). Do not load the task runtime manual for an ordinary native subagent assignment. A request to discuss delegation or edit the division does not launch an agent.

Select the logical responsibility from that division, then pass its model and reasoning effort directly to native `spawn_agent`. Current defaults are sol: `gpt-6.1-sol` / `high`, astra: `gpt-6-astra` / `medium`, luna: `gpt-6-luna` / `max`; prefer sol and minimize luna use. These are logical responsibilities; the exposed native 0.167 schema has no `agent_type` field. Respect explicit user choices and the native tool's supported fields; do not generate provider roles or silently substitute another model.

Native children inherit the parent's provider. For a task that must run on a different provider, use a separate task through `codex-provider-tasks`. When explicit model or effort overrides require fresh or bounded context, set `fork_turns="none"` or an appropriate supported history limit and include the necessary brief; a full-history fork may reject overrides.

Give each assignment a concrete outcome, necessary context, owned paths or responsibilities, constraints, and a verification method. Tell code-writing agents they share the workspace and must preserve others' edits. Delegate only work authorized by the user and current session rules; no global delegation policy is installed.

Read the result and evidence before accepting it. Check the relevant behavior, identify unresolved limits, and give a focused correction when needed. The lead retains integration and final acceptance. Keep Cross-Session Coordination and other method skills available when their separate workflows apply; this skill adds no harness runner, profile registry, or routing mode.

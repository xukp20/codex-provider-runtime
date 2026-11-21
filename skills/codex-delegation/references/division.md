# Default division of native subagent work

The user's `CODEX_HOME/codex-delegation/division.md` replaces this document when present. Edit that private override to change model choices or responsibilities; no apply or profile registration step is needed.

| Responsibility | Model | Reasoning effort | Typical assignment |
| --- | --- | --- | --- |
| sol | `gpt-6.2-sol` | `high` | Default implementation, diagnosis, research synthesis, and verification with a concrete brief. |
| astra | `gpt-6.2-astra` | `medium` | A bounded question needing deeper judgment: architecture, difficult tradeoffs, a contradictory result, or an independent assessment. |
| luna | `gpt-6-luna` | `max` | Occasional narrowly specified mechanical work with an objective check. Minimize use; sol remains the default. |

Choose by the assignment rather than opening one task per model. A small number of useful assignments is easier to integrate than filling all available agent slots. The lead keeps overall architecture, interfaces, scope, scheduling, and acceptance.

Use direct native fields. For example, a fresh implementation assignment to sol uses `spawn_agent` with `model="gpt-6.2-sol"`, `reasoning_effort="high"`, and `fork_turns="none"`, plus a complete brief. Those fields choose a model and effort inside the inherited provider; they do not change providers. Follow the actual tool schema if it differs.

A useful brief states the desired outcome; relevant source and current evidence; owned files or responsibility; boundaries and dependencies; and the smallest meaningful check. When files are shared, state that other writers are present and their edits must be preserved. Supply enough context that a fresh child can execute without guessing.

Accept a result against the requested behavior and returned evidence. Treat a failed check as a focused follow-up, not a reason to broaden the project. Report a missing dependency, uncertain result, or blocked interface promptly. Do not treat an agent's confidence as a substitute for observable verification.

# Shade — Architecture & Milestone Plan

Status: implementation plan, not a record of completed work. Updated 2026-11-02.

Shade is a personal agent harness built in Rust on Infinity. You can chat with it from a terminal or Discord; scheduled jobs, webhooks, and observed MCP resource changes can start background runs. Tools and permissions come from a human-controlled configuration file.

The runtime stays small: one daemon, a thin client, a small Cargo workspace, SQLite, thin adapters, and Infinity's existing model and MCP bridges. Service-specific behavior, upstream polling, authentication, and caching belong in independent MCP servers.

This document covers constraints, architecture, feature semantics, milestones, and remaining risks. `AGENTS.md` contains lasting coding instructions; this document contains implementation decisions and delivery checks. The separate personal-MCP workspace has its own plan.

## 1. Constraints and verified assumptions

### 1.1 Project constraints

- Rust, with a deliberately simple runtime.
- Support model tool calls, MCP servers, durable schedules, external chat, webhooks, and conditional event triggers.
- Only owner chat may manage schedules and watches through agent tools. Administrative CLI commands are an explicit owner-operated path.
- Each root run has a grant. Descendants cannot gain authority beyond their root.
- Policy is human-controlled. Agent-accessible files, tools, and MCP roots must not provide a path to modifying Shade's configuration, executable, credentials, or database.
- No approval step for ordinary schedule/watch creation. Validate authority, persist the definition, and emit a runtime-written receipt.
- Follow `AGENTS.md`: no `unwrap`/`expect`, short readable functions, explicit errors, and restrained cloning and call chains. Ownership copies needed for correct asynchronous code are allowed; coding style must not force blocking I/O or weaker isolation.

### 1.2 Infinity assumptions and M1 gates

The v0.2 documentation supports the following starting points. Exact behavior is checked against the selected source revision in M1; no later milestone may rely on an unanswered API question.

| Topic | Design consequence |
| --- | --- |
| Local agents require a `tokio::task::LocalSet` because their work is `!Send`. | Keep Infinity state in a local core; adapters communicate over channels. |
| Infinity exposes pluggable conversation and state stores. | Implement SQLite-backed stores with the same semantics as the reference stores. |
| `ThreadBuilder` configuration is process-local. | Reconstruct tools and prompts through `ThreadConfigSource`, backed by persisted grants. |
| Configuration is documented as resolving when a thread loads. | Do not assume refresh on every input, completion, or tool invocation. Prove the lifecycle before using it for memory refresh. |
| `ThreadBuilder::launch()` generates an ID and attaches a handle. | Do not assume the grant can be inserted after launch but before configuration resolves. Establish a safe registration path in M1. |
| `without_builtin_tools()` exists in v0.2.0. | Built-in tools are a policy choice, not an unavoidable permission exception. Enable spawning only with enforceable shared run limits. |
| Tokio sleep tools use process-local timers. | Do not register them. Durable scheduling belongs to Shade. |
| Tools return results asynchronously through the input path. | Persist Shade-owned results before delivery, and handle missing results as unknown outcomes after a crash. |
| `infinity-mcp-bridge` exposes stdio/HTTP MCP tools. | Reuse it for model-facing tools. Verify notification support before selecting the watch client. |
| `infinity-provider-rig` bridges model providers. | Reuse the provider; configure model limits explicitly. |
| Automatic compaction bounds model context. | Long-lived chat is reasonable; compaction does not bound database growth or replace long-term memory. |

M1 records exact crate versions and source revision. Version examples such as `0.2.x` are explanatory placeholders, not valid final exact pins. Commit `Cargo.lock` and use concrete compatible versions.

M1 answers, with source links and probes, are in `docs/infinity-notes.md`. Where they differ from the assumptions above, the notes win and the sections below have been updated.

## 2. Architecture

### 2.1 Process and ownership

Shade is a daemon and a client:

- `shaded [--config PATH]`: the daemon. It has no subcommands. It validates the config, acquires the execution lock, migrates the database, runs startup recovery, then starts the core, adapters, scheduler, watches, and control socket. It runs until SIGINT/SIGTERM and then shuts down gracefully.
- `shade`: the client. Every command is a request to the running daemon over the control socket:
  - `chat [--thread NAME]`: interactive terminal conversation.
  - `ask <prompt>`: one-shot run in a fresh root; prints the reply.
  - `schedule list|pause|resume|rm`, `watch list|pause|resume|rm`.
  - `threads list`, `audit`, `usage`, and `backup <path>`.

When the daemon is not running, `shade` fails with a clear error that names the socket path and how to start the daemon. The client never opens the database or starts Infinity.

The daemon is the only process that opens `shade.db` or drives Infinity. It acquires an exclusive `flock` on `<data_dir>/shade.lock` before touching the database or socket; a second daemon fails clearly. Holding the lock proves no other daemon owns the directory, so the daemon may then remove a stale socket file. Migrations run only at daemon startup. Administrative requests (pause, resume, remove) take effect immediately because the daemon executes them; there is no reconciliation pass for external database edits.

#### Control socket

- Path `<data_dir>/shade.sock`, where the data directory is `$XDG_DATA_HOME/shade`, else `~/.local/share/shade`; relative or empty values are ignored. Both binaries resolve it with the same `shade-proto` function. Unix socket paths are limited to 108 bytes, so a very long data directory fails at bind with a clear error. The data directory is created `0700` and the socket is `0600`. The daemon also checks the peer UID (`SO_PEERCRED`) and rejects other users.
- Newline-delimited JSON. Each connection starts with a hello carrying the protocol version; a mismatch fails with a clear error. Client and daemon ship from the same workspace and are upgraded together, so there is no compatibility layer for old protocol versions.
- One request per connection, answered by zero or more events and one final `done` or `error`. `chat` opens one connection per user line, so the daemon keeps no session state.
- Lines and queues are bounded. Request and response types live in `shade-proto`.
- A connection carries owner authority. Terminal chat and `ask` enter as `Chat` origin through the same root registration and durable delivery path as Discord. Administrative requests are an explicit owner-operated path and are not agent tools.
- Closing the client does not cancel a turn. The turn finishes and remains in history; only output delivery to that connection stops.
- Any process running as the owner can reach the socket. Owner-run MCP servers are already trusted at that level, since they can edit the config. The shell sandbox and MCP roots must never expose the socket or the data directory (F5).

### 2.2 Execution model

| Component | Execution context | Responsibility |
| --- | --- | --- |
| Control socket, Discord, HTTP adapters | Tokio tasks; `Send` where required | Authenticate, parse, bound input, forward it, and stream or render output. |
| MCP watcher | Tokio or local task, according to SDK requirements | Maintain subscriptions, read snapshots, evaluate conditions, and persist trigger transitions. |
| Core/router | `LocalSet` | Register roots and grants, route persisted inputs, attach output pumps, and own Infinity. |
| Scheduler | Core task | Claim due occurrences transactionally and create durable pending runs. |
| Database worker | Dedicated blocking thread | Own the SQLite connection and execute bounded requests/transactions. |
| Shell child | Bubblewrap sandbox | Execute one bounded command with grant-dependent mounts and network access. |

Use bounded channels at Shade's adapter boundaries. A full queue means explicit backpressure or a retryable error, not unbounded allocation. Coalesce resource invalidations per `(server, uri)`; they indicate that a snapshot should be reread, not that each notification must start work.

The core owns all Infinity handles and mutable run state. Adapters do not select tools, create grants, or decide permission policy. All external inputs enter through the same root registration and durable delivery path.

### 2.3 Module layout

```text
crates/
  proto/                    shade-proto: protocol types and data-dir/socket path
    src/lib.rs
  cli/                      shade-cli: the `shade` client binary
    src/
      main.rs               argument parsing and rendering
      client.rs             blocking socket connection
      error.rs
  daemon/                   shade-daemon: the `shaded` binary
    src/main.rs             --config, logging, signals, calls shade_core::run
  core/                     shade-core: the runtime library
    src/
      lib.rs
      error.rs              typed errors
      lock.rs               data directory creation and execution lock
      config.rs             TOML parsing and policy validation
      secrets.rs            optional private .env next to the config
      store/                the SQLite adapter: all SQL lives here
        db.rs               database worker and migrations (SQL in crates/core/migrations/)
        conversation.rs     Infinity ConversationStore
        state.rs            Infinity StateStore
        roots.rs            root registration and conversation names
        grants.rs           grants, per-turn budgets, atomic charging
        operations.rs       tool operations and unanswered-call lookup
        inbox.rs            accepted input and its acknowledgment
      thread_config.rs      per-thread tools (close_thread for compaction threads)
      policy.rs             capabilities, origins, ceilings, limit checks
      authority.rs          grant/budget admission for config, provider requests, tools
      model.rs              provider and completion-budget enforcement
      router.rs             durable ingress, routing, output pumps, resume, shutdown
      router/turn.rs        what a turn's sender sees
      runs.rs               run lifecycle, shared limits, cancellation (M11)
      scheduler.rs          schedule claims and overlap handling
      condition.rs          pure typed comparisons and watch transitions
      watcher.rs            MCP subscriptions and reconnects
      memory.rs             bounded authorized memory index
      recovery.rs           answers tool calls left waiting by a previous daemon
      tools/
        mod.rs
        shell.rs
        schedule.rs
        watch.rs
        notify.rs
        now.rs
      sandbox.rs            bubblewrap mounts, environment, bounded runs, startup probe
      sandbox/paths.rs      protected-path validation
      sandbox/output.rs     bounded head/tail capture
      adapters/
        socket.rs           control socket server
        discord.rs          owner chat, chunked replies, typing, safe mentions
        webhook.rs
```

The project is a Cargo workspace. Shared versions, metadata, and lints live in the root `Cargo.toml`.

- `shade-proto` has no dependency beyond serde, so the client builds without Infinity, SQLite, or Tokio.
- Of the workspace crates, `shade-cli` depends only on `shade-proto`. It uses a blocking `std` Unix stream and needs no async runtime.
- `shade-daemon` stays trivial: parse `--config`, set up logging, call into `shade-core`, and map the result to an exit code. All behavior lives in `shade-core`.

Split a module from `shade-core` into its own crate when it has a distinct responsibility and a clear dependency boundary, for example an adapter with a heavy dependency.

### 2.4 Technology choices

| Area | Choice | Reason |
| --- | --- | --- |
| Persistence | `rusqlite` with `bundled`, WAL, foreign keys | One local transactional database. |
| Database access | One worker thread owns the connection; callers send short closures that own their parameters over a bounded queue and await a oneshot reply | SQLite work cannot stall the local agent core. Closures never capture core state. |
| Model | Infinity's rig bridge; Anthropic Messages or OpenAI Chat Completions, optionally through a gateway (`base_url`) | Reuse supported provider integration. |
| MCP agent tools | Infinity MCP bridge | Avoid implementing a client inside Shade. |
| MCP watches | Official `rmcp` if the bridge lacks required APIs | Resource notifications need a long-lived client outside a model tool invocation. |
| Configuration | TOML with serde | Readable, human-controlled policy. |
| Secrets | Variables referenced by name, from the environment or a `0600` `.env` next to the config (environment wins; the file is parsed into a map, never into the process environment) | Keep secrets out of configuration, git, and child processes. |
| Discord / HTTP | `serenity` / `axum` | Use existing gateway and HTTP machinery. |
| Cron / timezone | `croner` and compatible timezone support verified in M1 | Delegate parsing; define Shade's DST and catch-up policy explicitly. |
| CLI / diagnostics | `clap`, `tracing`, `tracing-subscriber` | Small adapters and consistent stderr logging. |
| Control protocol | Newline-delimited JSON (`serde_json`) over a Unix socket | No HTTP server or RPC framework for a local single-owner channel. |
| Errors | `thiserror` enum and explicit domain errors | Preserve causes and useful context without pervasive mapping chains. |
| Shell isolation | Bubblewrap | Local Linux sandbox without a container daemon. |

Database requests are short closures over owned parameters, never long-lived closures holding core state. Never hold a mutex or SQLite transaction across an `.await`. Backup work runs on a separate blocking connection. Add bounded SQLite busy handling and report persistent storage failures rather than accepting work that cannot be saved.

### 2.5 Coding and testing policy

```toml
# Cargo.toml
[lints.clippy]
unwrap_used = "deny"
expect_used = "deny"
redundant_clone = "warn"
too_many_lines = "warn"
cognitive_complexity = "warn"
disallowed_methods = "deny"
```

```toml
# clippy.toml
too-many-lines-threshold = 40
disallowed-methods = [
  { path = "core::result::Result::map_err", reason = "Use an explicit domain error or From conversion" },
]
```

Cheap handle clones are normal when ownership is shared. Avoid unnecessary copies of data; use a short explanation for a non-obvious necessary copy. Do not require a comment for every ordinary ownership transfer or handle clone. A forty-line lint is a review aid, not permission to split coherent logic into arbitrary fragments.

Tests protect silent edge cases, persistence semantics, and authority boundaries. Prefer pure functions with explicit time and state. Use plain Rust tests and table-driven cases. Async boundaries may use `#[tokio::test]`; real SQLite and subprocess tests are appropriate when those boundaries are the behavior being tested. No mocking or snapshot framework is required.

Do not test serde derives, constants, getters, dependency internals, or implementation-shaped mocks. Do test policy validation even if it happens during config parsing, because the compiler and serde do not enforce permission policy. Crash recovery and transactional handoff need integration tests; pure comparison tests cannot establish those guarantees.

Each milestone leaves `main` building and runs formatting, `cargo clippy --all-targets -- -D warnings`, and relevant tests through `check.sh`.

### 2.6 Capabilities and grants

A capability names authority. A grant records the authority attached to a root run. Prompts describe the task; grants determine available tools and resources.

Shade groups are `now`, `notify`, `shell`, `shell_network`, `schedules`, and `watches`. MCP server names are additional capabilities. Reserve Shade group names so an MCP server cannot collide with them. `shell_network` only changes shell networking and is invalid without `shell`; MCP server network access is governed by that server's configuration and authority.

MCP granularity is initially one configured server. For read-only access, run a server in an enforced read-only mode and register it separately. A read-only flag must reject writes server-side, not merely hide tool descriptions.

```toml
[chat]
caps = ["now", "notify", "shell", "shell_network", "schedules", "watches", "vault", "email_read", "email"]
prompt = "You are chatting with your owner."

[jobs]
prompt = "You are running a background job. Use notify when the result needs the owner's attention."

[hooks.github]
caps = ["now", "notify"]

[mcp.email_read]
description = "Read mail without sending or modifying it."
command = ["email-mcp", "--read-only"]

[mcp.email]
description = "Read and modify mail, including sending messages."
command = ["email-mcp"]
```

Origins are typed: `Chat`, `Schedule`, `Watch`, or `Webhook`, with stable source identifiers. Every background occurrence gets a new root grant, distinct from its creator's conversation.

Rules:

1. **Delegation attenuates.** A schedule/watch's requested caps must be a subset of the creator's effective caps. Reject extra caps and identify them in the tool result.
2. **Management is chat-only.** `schedules` and `watches` cannot be delegated. Config validation rejects these caps on hooks, and management tools verify the root origin as well as the capability. A chat descendant shares its root's authority and limits.
3. **Stored grants remain capped.** Chat and hook grants store no capability list: they follow their origin's current ceiling, so a long-lived conversation gains or loses caps when `[chat]` changes. Job grants are intersected with current chat caps minus management caps. Unknown or removed stored capabilities are dropped and logged. A changed MCP registration is a changed trust decision and requires owner review.
4. **Fail closed.** An unknown thread, missing grant, missing origin, deleted hook configuration, or broken ancestry receives no model execution or Shade tool dispatch until resolved. Log the error. Do not rely on residual built-in tools as a safe fallback.
5. **Permission ancestry is structural.** Follow persisted parent relationships to the actual root. Fresh-context boundaries limit inherited conversation history, not grant ownership. Detect missing parents and cycles. Reuse `get_ancestor_chain` only if M1 proves it preserves the required structural lineage; otherwise walk `get_thread_parent_info`.
6. **Tool execution checks authority.** Hiding a tool from the model is useful, but dispatch also validates the caller's root grant, origin, and remaining limits. Every bridge tool is bound to its MCP server capability.
7. **Policy changes require restart initially.** Parse and validate config once. Stop acceptance and shut down current runs before restarting with a new config. No hot-reload or immediate revocation promise. Fresh resolution and dispatch use that process's immutable validated policy.

Root registration must commit the thread identity, grant, run metadata where applicable, and first pending input before attaching or sending anything that can load the thread. M1 confirmed the path: Shade picks the root ID, commits the rows, then calls `LaunchingSystem::thread_handle(id)`; configuration resolves on the first input, not on attach. Do not call `launch()`. Infinity calls `ensure_root_thread` for any thread it loads, so Shade's store must never create a root there; registration is the only way a root comes to exist.

Job updates apply to future runs. Active roots retain their recorded grant snapshot, bounded by the current process policy. Pause/delete stops future trigger creation; it does not silently roll back or terminate external work already in progress. Shutdown is the initial owner mechanism for stopping active work.

Creation/update tools emit a runtime-written receipt with ID, title, trigger, timezone, and caps. Definitions are durable before success is reported. Tool retries with the same call ID reuse the stored mutation receipt instead of creating another job.

The job-cap schema lists the configured delegable capabilities and their descriptions. Tools still validate arguments; a schema is not an authorization check. No confirmation prompt is added for valid creation.

### 2.7 Shared run limits

Limits live in protected config and apply across a root and all descendants. They are checked at completion start and tool dispatch, not enforced through prompt wording.

```toml
[limits]
max_completion_rounds = 40
max_tool_calls = 100
max_run_seconds = 900
max_children = 8
max_concurrent_background_runs = 4
max_notifications = 10
```

These are example defaults, to be tuned during implementation. Each limit is added with the feature it bounds: M5 enforces rounds, tool calls, and run time; `max_children` waits for model-controlled spawning, `max_concurrent_background_runs` for M11 (the first background producer), and `max_notifications` for M10. A completion round is one provider request, retries included. Limits cap accepted work; they are not a guarantee of an exact currency amount. Token usage is recorded when available; a token/spend budget can be added later without replacing these basic bounds.

For background work, the root run is one trigger occurrence. For chat, the budget covers a user turn and its descendant work, not the entire weeks-long conversation. Persist the budget scope ID and counters so restart or spawning cannot reset them. A new chat input cannot silently reset limits for still-running descendants from an earlier turn.

When a limit is reached, stop further model/tool dispatch, cancel owned tasks where supported, record `limit_exceeded`, and send one runtime failure notice. Reserve that notice outside the model's notification allowance. Cancellation cannot undo external side effects already committed.

M1 found no enforceable spawn hook, so Shade uses `without_builtin_tools()` and defers model-controlled subagents; do not build a custom orchestration framework merely to preserve them in the first release. Auto-compaction still needs `close_thread`, so the config source adds Infinity's public `CloseThreadTool` for compaction threads only.

## 3. Features and failure semantics

### F1. Persistent conversations and state

Persist threads, parent links, messages, compaction summaries, processed IDs, metadata, subscriptions, and pending choices in `shade.db`. Store Infinity messages as JSON with IDs and order columns; do not query their JSON representation for ordinary routing.

Implement store semantics by reading the pinned reference implementation. Keep history cutoffs, Fresh-context behavior, structural parent lookup, compaction selection, and message-ID handling consistent. Multi-row appends and each store method's required invariants are transactional. Do not invent a cross-method atomicity guarantee that Infinity does not provide.

Migrations are ordered SQL statements tracked by `PRAGMA user_version`, applied transactionally by the daemon at startup, after it acquires the execution lock. Refuse a database from a newer schema. Pin Infinity versions and back up before upgrades; message-format changes require a deliberate conversion migration.

Automatic compaction reduces prompt context, not stored history. Initial retention is explicit: conversations and job histories remain until the owner removes or archives them; bounded audit arguments and rotated logs prevent accidental output growth. A later retention command can prune safely with ancestry constraints understood.

### F2. Durable input and run delivery

SQLite is the durable boundary for accepted Shade work. An in-memory `mpsc` send is not acknowledgment of durable processing.

Suggested logical records, with exact SQL decided in the relevant milestone:

| Record | Important fields |
| --- | --- |
| `thread_grants` | root ID, origin kind/source, stored caps, budget scope |
| `inbox` | stable input ID, root ID, input payload/version, source occurrence key, delivery state |
| `runs` | run ID, source kind/ID, scheduled/event time, root ID, status, start/finish time, error |
| `tool_operations` | root/thread/call IDs, operation kind, durable result if captured, outcome state |
| `mutation_receipts` | tool-call ID, resulting object ID, fixed receipt |

For a schedule occurrence, one transaction claims the occurrence, creates its pending run/grant/input, and advances the schedule. For a watch, one transaction records the observation/transition and any pending run. For a webhook, persist acceptance before returning `202`.

After commit, dispatch the same persisted root and stable input ID. Replay unfinished delivery on startup. Never create a new root on each retry. Deduplicate schedule occurrences with a unique `(schedule_id, scheduled_for)` key and provider webhook deliveries with `(hook_id, provider_delivery_id)` when available.

Infinity's local mode has no acknowledgment hook, and it writes history and processed IDs in two separate store calls. Shade's `append_messages` therefore records processed IDs for user and subscription-event messages in the same transaction as the history rows. An inbox record is accepted once its input ID is in processed IDs; checking that after `CompletionFinished` or the thread's idle event is the durable boundary. A channel send, event subscription, or observer callback alone is insufficient evidence.

If local mode cannot expose a reliable boundary, use Infinity's supported step/input interface for durable handoff and recovery, or make a small upstream integration change. Resolve this before shipping unattended triggers. Do not silently claim durable delivery on top of a volatile local queue.

Guarantee: accepted trigger records survive restart and have deduplicated replay according to the verified integration contract. This does not imply exactly-once model completions or exactly-once external side effects.

Run statuses distinguish queued, running, completed, failed, interrupted, cancelled, and limit exceeded. Completion means the run has finished, not merely that one model completion emitted an event. Pending children/tools count as unfinished work according to the verified Infinity lifecycle.

### F3. Chat: terminal and Discord

`conversations(adapter, key, root_id)` maps a conversation to a long-lived root. Terminal keys are `default` or `--thread NAME`; Discord keys are channel IDs. `/new` changes the mapping and retains the prior history.

Persist accepted user input through the same delivery path. Discord message IDs provide input deduplication. Authenticate before forwarding; no adapter can supply arbitrary capabilities or impersonate an internal origin.

An output pump attaches to each active conversation and turns Infinity events into adapter output. Buffer text per completion and send when finished; respect cancellation/interruption events so stale partial output is not presented as a finished answer. A model completion with tool calls is not necessarily a finished user turn. Verify handle reattachment and idle/wake behavior in M1.

New user text may interrupt model generation using Infinity's supported semantics. This does not imply cancellation of already-dispatched shell/MCP side effects. Explain that distinction in operational documentation and support bounded shutdown cleanup.

Discord requires a non-empty owner allowlist. Respond in DMs, and to mentions in guild channels. Guild replies are visible to that channel's members; default to DMs for private tasks. Ignore bot messages, prevent unwanted mentions with explicit allowed-mention settings, and bound input/output sizes. Split replies within Discord limits at line boundaries with safe fallback for long lines and Unicode. Attachments are URLs as text initially.

Typing indicators track active completion work. Optional tool status lines are sanitized and do not expose arbitrary secrets or full arguments.

### F4. Tools and result handling

| Tool/group | Behavior |
| --- | --- |
| `now` | Current time and configured timezone. |
| `shell` | Sandboxed `sh -c` in the workspace, bounded process lifetime/output. |
| `notify` | Send to the configured owner destination, within shared run limits. |
| `schedule_create/list/update/delete` | Durable schedule management, chat-origin only. Update supports prompt, title, timing, caps, pause/resume. |
| `watch_create/list/update/delete` | Durable watch management, chat-origin only. Update resets a baseline when source/condition semantics change. |
| MCP server bridge | Server-scoped list/invoke tools, gated at dispatch. |

Validate every argument. Invalid model input becomes a visible tool error result the model can correct. Infrastructure failures are typed and logged; do not turn storage failure into a false success. If durable recording fails, stop dependent work.

Shade-owned async tools persist their captured result before enqueueing it for Infinity. Persist local schedule/watch mutations and their tool-call receipts in one transaction. A crash after mutation but before result delivery therefore replays the receipt rather than repeats the mutation.

### F5. Shell and sandbox

The shell is sandboxed from the first supported release. Bubblewrap is required when any configured origin can use `shell`; validate actual sandbox setup, not just executable presence. A development-only `sandbox = "off"` is an explicit owner opt-out with a prominent warning: shell grants then confer the user's authority.

Construct an empty filesystem and mount only what the command needs:

- Required system binaries/libraries and narrowly selected system configuration read-only. Account for Arch and NixOS layouts explicitly; do not bind the entire host root or home as a shortcut.
- Workspace read-write at a fixed sandbox location.
- Vault read-write only when the effective root grant also includes `vault`. `shell` without `vault` does not expose vault contents.
- Minimal `/dev`, a proc filesystem in an isolated PID namespace, and private tmpfs storage.
- No Shade config/data directories, secrets, SSH keys, browser profiles, host `/run/user` sockets, Docker socket, SSH agent, or session D-Bus.
- A cleared environment with a configured safe allowlist. No API keys or inherited control-socket variables by default.
- No host networking without `shell_network`. That capability permits shell network access, including any reachable services; it is not a per-domain firewall.

Validate resolved workspace/vault paths: they must not overlap protected Shade paths, contain writable Shade executables/config/MCP server code, or expose control sockets. The sandbox cannot make later trusted execution of agent-writable code safe. Configure MCP roots with the same protected-path discipline.

Use namespaces and process cleanup so descendants cannot simply escape timeout handling by creating a new session. Run with a default timeout of 30 seconds, bounded by config. On timeout, cancellation, normal command exit, or daemon shutdown, terminate the entire sandbox process tree and reap it. Use parent-death behavior and verify abrupt daemon death cleanup with real subprocess tests.

Drain stdout/stderr continuously while retaining bounded head/tail buffers, 16 KiB each by default. Capping the returned string after collecting unbounded output is insufficient. Return exit code, timeout/cancellation status, and truncation counts. Preserve valid text across UTF-8 boundaries.

Sandboxing controls access, not CPU/RAM/disk quotas. Resource-limit enforcement may be added if workloads require it; do not describe process/time/output limits as a complete resource quota.

### F6. Scheduled jobs

Definitions live in SQLite, not duplicated in config. Suggested fields: ID, title, kind, spec, timezone, prompt, caps, next/last occurrence, enabled, creator root, definition version, and optional pending catch-up occurrence. Runs reference the definition/version used.

Poll every 30 seconds. For a due job, claim its occurrence transactionally through F2 and compute the next occurrence strictly after the current scheduling time. One-shot jobs disable future triggering in the same transaction that records their pending occurrence. A disabled definition does not erase an already accepted run.

Missed cron occurrences coalesce to one delayed run after downtime. Include the original scheduled time and actual trigger time in the input. Do not produce hundreds of missed briefs. A job already active has at most one pending catch-up occurrence; further occurrences coalesce into it. Different jobs share the global background concurrency limit. Pending work remains durable while waiting for capacity.

Default overlap policy is one active root run per job. A hung run reaches its deadline; restart and retry do not bypass the overlap constraint. A run waiting on unresolved external outcome is visibly interrupted rather than assumed safe to repeat.

Timezone policy is explicit:

- Cron schedules use a persisted IANA timezone, defaulting to configured `Europe/Paris` or the owner's configured zone.
- For a nonexistent local time during spring-forward, skip that occurrence. Do not silently move it to another time.
- For a repeated local time during fall-back, run the first occurrence only for that local scheduled slot.
- One-shot requests require an offset or an explicitly resolved timezone. Reject ambiguous/nonexistent wall times until the model supplies an unambiguous instant.
- Store instants in UTC. Retain timezone/spec for display and next-occurrence computation.

`croner` runs fixed-time jobs right after a spring-forward gap and wildcard/interval jobs in both passes of a fall-back hour (M1). Shade filters both cases: drop an occurrence whose wall time no longer matches the pattern, and drop second-pass occurrences. Test Shade's chosen policy, not the dependency's parser internals. The previous blanket claim that a 02:30 job fires once on every DST day is intentionally removed.

Tools accept a structured trigger rather than guessing cron versus datetime from arbitrary text. For example, `{ "kind": "cron", "expression": "0 8 * * 1-5", "timezone": "Europe/Paris" }` or `{ "kind": "once", "at": "2026-11-06T09:00:00+01:00" }`. Validate future instants and cap active definitions at 100 initially.

Updates preserve ID and run history. Changed timing recomputes future occurrences; it does not manufacture historical catch-up work. Changed caps undergo the same delegation checks as creation.

### F7. Watches and typed conditions

Watches observe MCP JSON resources. The runtime evaluates conditions without model calls and starts a fresh granted run only when the transition policy fires. Upstream polling, if needed, is the MCP server's responsibility. Shade does not poll arbitrary tools.

Example condition:

```json
{ "pointer": "/price", "op": ">", "value": 100000 }
```

Parse JSON before extraction. Omitted pointer and the empty RFC 6901 pointer both select the whole parsed JSON value, not the raw text. Validate pointer escapes. Support numeric `>`, `>=`, `<`, `<=`, and equality/inequality for supported scalar types. Include boolean equality for ready-made service conditions. Objects, arrays, missing paths, unsupported types, and numeric conversion failures produce evaluation errors, not false.

Define number comparison explicitly. Do not blindly convert every JSON integer to `f64`; preserve integer comparisons and reject unsupported mixed representations rather than silently rounding large values. Money resources should document units and may expose integer minor units or a precomputed boolean.

The evaluator returns updated condition state plus a fire decision; cooldown must not prevent the condition state from tracking successful observations. Errors leave the prior valid condition state unchanged, record the error, and never fire or re-arm the watch.

Default watch behavior:

| Situation | Result |
| --- | --- |
| First successful observation | Establish baseline; do not fire, even if already matching. Explain this in the creation receipt/tool description. |
| `on_change`: false to true, outside cooldown | Persist transition and pending run atomically. |
| `on_change`: true to true | Hold. |
| `on_change`: true to false | Re-arm. |
| Crossing during cooldown | Update observed state but suppress the run. Do not defer it. |
| Cooldown later expires with no new crossing | No run. There is no cooldown-expiry timer. |
| `every_match`: successful matching observation outside cooldown | Fire, even when prior observation matched. |
| Read/extraction/freshness error | Preserve last valid condition state; surface failure. |

`on_change` is default for watches; cooldown defaults to five minutes. A suppressed crossing requires an observed false and a later true after cooldown to fire again. Webhooks use stateless `every_match` by default and do not inherit the watch cooldown unless explicitly configured.

Subscriptions indicate current-state invalidation, not a durable event stream. An MCP update notification identifies a URI; a later read returns the current resource. A short true state followed by false may be missed before the read. Disconnection can hide arbitrarily many intermediate transitions. The supported guarantee is **fire on observed transitions**, not detect every historical crossing.

If a service requires lossless event handling, its MCP integration must expose durable events with IDs and replay/cursors, or deliver identified webhook events through F9. Implement that outside the generic snapshot-watch path. Do not invent historical events from the final snapshot.

Use one client per watched server and one subscription/read pipeline per `(server, uri)`, with fan-out to all watches. Subscribe before the initial read; serialize/coalesce reads and retain a dirty flag when another notification arrives during one. A transport lacking subscriptions is rejected for watch creation.

Reconnect with exponential backoff capped at five minutes. Resubscribe and read on reconnect. Persisted `last_condition` prevents firing again merely because the snapshot still matches; a changed current snapshot may produce a newly observed transition. Notify once after five consecutive failures, reset the failure episode after successful recovery, and avoid one notification per failed attempt. Watcher health alerts are runtime operations with their own bounded rate limit.

Persist ID, title, server, URI, condition, fire mode, cooldown, prompt, caps, creator, definition version, baseline state, last valid value, last error, failure count, and last fire time. Limit active watches to 50. Condition/source changes establish a new baseline; prompt/title changes preserve it.

The job input includes the captured triggering value, URI, observation time, and trigger metadata. Use one-pass templating or a structured input section; substituted values are untrusted data and cannot introduce additional placeholder expansion. Resource/output size limits prevent one resource from filling context or memory.

### F8. Contract for watchable MCP servers and caching

Document this in `docs/mcp-servers.md`, aligned with the independent MCP-server workspace:

1. Declare resource-subscription support and accept subscriptions to the documented resource URIs.
2. Return documented JSON snapshots. Shade selects exactly one matching JSON text content entry for the requested URI; missing/ambiguous content is an error.
3. Notify when the meaningful value or availability state changes. Do not notify solely because a fetch timestamp advanced.
4. Publish freshness/availability metadata, including when the upstream value was actually observed. A cache read must not reset `observed_at`.
5. Make resource reads safe observations; do not put consequential actions behind a resource read. Shade trusts configured server code to honor this contract.

Recommended envelope for home-made watch resources:

```json
{
  "value": { "price": 100500 },
  "observed_at": "2026-11-02T20:00:00Z",
  "availability": "available"
}
```

Here the condition pointer is `/value/price`. Exact service schemas may differ, but the server documentation and watch registration must identify freshness fields and maximum acceptable observation age. Stale/unavailable data is an evaluation error, not a false condition. Reconnection must not present an old cached value as a newly observed value. No periodic model call is needed; evaluate freshness on each read/reconnect and health reconciliation where configured.

The server owns upstream rate limits, fetching/pushes, cache invalidation, and fetch coalescing. Reads normally use its cache. Read tools may expose `max_age_seconds` under the separate MCP plan: omitted/null uses the server default, zero asks for refresh, and a positive value bounds age. The host must not assume a requested refresh overrides upstream rate limits or guarantees success.

Two stdio clients may mean two server processes: Infinity's bridge plus the watcher. Independent in-memory caches can therefore mean duplicate upstream polling. That is an accepted initial tradeoff, not a promise of one fetch across all clients. Integrations needing one upstream poller should use a resident multi-client HTTP server or deliberately shared service state. Shade does not add a generic shared-cache service.

Read-only server modes must enforce account/resource boundaries. All configured MCP executables, configuration, and protected credentials are trusted host components and must remain outside agent-writable roots. A server failing to connect/start is degraded rather than a whole-daemon failure; apply timeouts/backoff and expose its state. Bridges must not block boot forever while discovering an unavailable server.

### F9. Webhooks

`POST /hook/<name>` uses a per-hook bearer secret loaded from an environment variable. Compare secrets in constant time, limit the body to 64 KiB, and bind localhost by default. Public exposure belongs to an owner-selected reverse proxy/tunnel.

```toml
[hooks.github]
token_env = "SHADE_HOOK_GITHUB"
caps = ["now", "notify"]
template = "Summarize the event and notify me if action is needed.\n<payload>\n{body}\n</payload>"
condition = { pointer = "/workflow_run/conclusion", op = "==", value = "failure" }
```

Authenticate, validate body/condition, and persist accepted work before `202`. Nonmatching valid events return `202` without an agent run. Invalid JSON required by a filter and evaluation/type errors receive a documented client error and diagnostics; they are not silently treated as a nonmatch. A filtered skip is not an accepted run.

Use a trusted configured header for provider delivery IDs where supported. Repeated IDs return the existing acceptance outcome without creating another root. Without such an ID, requests are distinct events; do not hash identical bodies and accidentally suppress legitimate repeats. Define a bounded dedup-retention window per provider.

Queue/storage failure returns a retryable error such as `503`; never acknowledge volatile-only acceptance. Bound queued jobs and per-hook admission rates. The configured token authenticates the caller; an incoming body cannot set caps, origin, notification destination, or internal run IDs.

Payloads remain untrusted. Grants limit the possible damage but do not guarantee that actions within the grant are appropriate. A hook with write authority can be induced to perform those writes. Offer narrow defaults and make broader grants an explicit owner config choice.

### F10. Notifications

`notify` targets the configured owner destination, initially a Discord DM. The model does not choose an arbitrary recipient. No adapter means a visible tool error, not a logged success.

Persist Shade-owned notification intent/result and use a stable local operation ID. On confirmed success, replay the recorded tool result. If delivery outcome is unknown after a crash and the adapter has no usable idempotency/reconciliation mechanism, record it as unknown; do not promise exactly-once notifications. Document any chosen retry behavior and possible duplicates.

Rate limits apply per root budget scope, including descendants. Runtime run-failure notices and watcher health alerts use separate bounded allowances so a model cannot exhaust its own error-reporting path. Never include raw secret-bearing tool arguments in failure notices.

### F11. Model configuration

Configure provider (`anthropic` or `openai_compatible`), model ID, API-key environment variable, optional `base_url`, optional `session_header`, context window, and maximum output tokens. Shade implements `ModelSource` itself so each request carries `User-Agent: shade/<version>` and, when `session_header` is set, the thread ID in that header (OpenCode Go requires `x-opencode-session` and rejects requests without it). Use an actual verified model ID in the checked-in example rather than an invented future model name. Limits come from provider documentation and are checked in M1.

The configured provider is reused. Shared round/deadline limits are enforced before starting a completion. Provider errors use bounded retries with clear terminal run status; retries are counted and do not escape the run deadline. Per-job models through `ModelSource` remain deferred.

### F12. Restart recovery and unknown outcomes

Recovery is part of the first async-tool milestone and every later integration; it is not postponed until after unattended use.

Before accepting new work:

1. Acquire the execution lock, migrate if needed, validate policy, and load pending runs/deliveries.
2. Reconcile captured durable results and replay them with stable IDs.
3. Find unresolved dispatched operations using the pinned Infinity message/operation semantics. Distinguish one-shot calls from active subscriptions, child communication, and other legitimate long waits.
4. For calls with no captured result, record an unknown/interrupted outcome and deliver a protocol-valid synthetic error result where appropriate.
5. Resume durable inputs and continuations through the verified integration path, respecting grants, budgets, and overlap constraints.

The synthetic text says: **"Interrupted before the result was recorded. The action may have completed. Verify its outcome before retrying an action with side effects."**

Safe reads may be retried within budget. Consequential writes require an idempotency key supported by the service, a reliable operation-status lookup, or explicit reconciliation before retry. Tool descriptions from unknown servers are not sufficient proof that replay is safe. Recommend idempotency keys/status lookups for your own MCP write tools.

Do not manufacture results for every historical subscription or assume that the last assistant turn contains every unresolved call. Test interruption, compaction, interleaved inputs, and multiple unresolved operations against the pinned runtime. Never edit serialized conversation history ad hoc to bypass runtime invariants.

Graceful shutdown stops acceptance and trigger claims, flushes accepted inputs/results, cancels owned shell tasks, winds down Infinity through its supported API, drains critical writers, and exits within a configured deadline. External actions already sent may still complete. Abrupt-death recovery remains necessary.

### F13. Long-term memory in Obsidian

Conversation history and compaction are not long-term memory. Keep a curated, human-editable memory index and small notes in the vault:

```text
Shade/MEMORY.md
Shade/memories/<slug>.md
```

The index lists a short hook and link per note. Notes carry type (`user`, `preference`, `project`, or `reference`) and update date. Read bodies through the vault MCP as needed.

Inject the index only into roots with effective `vault` authority. Hooks/jobs lacking vault access do not see private memory merely because the runtime can read it. Writes also require vault authority; shell mounts follow the same grant rule.

Cap index content at 4 KiB by default, ending at a complete line and valid UTF-8 boundary. Append an over-budget notice outside the bounded content. Read only a bounded amount from disk; do not load an arbitrarily large file before truncating. Missing index means no memory section. Other read failures are logged and handled visibly without dumping file contents into diagnostics.

Refresh at a verified conversation-load/turn boundary. If `ThreadConfigSource` does not run for every relevant wake, state the actual refresh timing or add a small supported prompt-refresh hook. Do not assert that edits are instantly visible to every active thread without a proven path.

Treat index and notes as editable reference data, not trusted policy. Prefer durable facts, update duplicates, and remove disproven entries. Vault writes can poison future memory; sandboxing and grants reduce who can write but do not establish truth. Concurrent model writes can race: prefer a single writer or an MCP update operation with version checks when needed. No embeddings/vector service initially.

### F14. Audit, usage, deployment, and backups

From the first real tool, record basic root/origin identity, dispatch, outcome, run status, and policy/budget failures. M17 adds the richer observer views; traceability is not deferred until then.

Use Infinity's observer for model/tool lifecycle and token usage, with a bounded channel to the database writer. Do not assume asynchronous observer logging is an authoritative durable tool-dispatch record. Shade-owned critical operation records are committed by the operation path. Redact known secrets and sensitive fields before logging; truncation is not redaction. Default tool audit records may retain name and safe summary rather than raw arguments.

`shade audit --since 1d` and `shade usage` show root/origin/job totals and status. Token counts are authoritative only as supplied by the provider; currency cost is an estimate using configured prices, or omitted when prices are absent.

Use XDG config/data paths and a configured workspace. Provide a systemd user unit with bounded shutdown, automatic restart, environment-file permissions, and linger instructions. Never require secrets in command-line arguments.

`shade backup <path>` asks the daemon to write a consistent SQLite snapshot on a separate connection (`VACUUM INTO` or backup API, selected for the supported SQLite version) without blocking the core thread. The client sends an absolute path; the daemon refuses an existing destination and creates the file with mode `0600`. Document that a DB backup excludes the vault/config/secret files. Supply a daily timer and a restore procedure. Test restoring a backup into an isolated data directory before calling deployment complete.

## 4. Milestones

Each milestone is one reviewable PR. `Check` is a manual demonstration; `Tests` covers the silent edge cases worth retaining. No milestone implies production readiness for features still behind unresolved API gates.

### M0 — Skeleton and guardrails

- **Goal:** a small project with enforced coding conventions.
- **Scope:** workspace with `shade-proto`, `shade-cli`, `shade-daemon`, and `shade-core` crates; concrete stable toolchain pin; client command stubs; typed errors; tracing; lints; `check.sh`; lockfile; `.gitignore`; execution lock; control socket with peer check and protocol-version hello.
- **Check:** `check.sh` passes; `shade --help` lists planned commands; `shaded --help` shows only its options; a second daemon fails; a client command reaches the daemon and gets a clear not-implemented error; with the daemon stopped, the client names the socket and how to start the daemon.
- **Tests:** none beyond meaningful policy logic if introduced.

### M1 — Infinity spike and integration gates

- **Goal:** prove the selected runtime/provider stack and the boundaries needed for safe persistence.
- **Scope:** in-memory `ask` served by the daemon and streamed to the client; minimal model config; exact dependency pins; `docs/infinity-notes.md` with source links and executable evidence.
- **Record:** model stream/message shape; bridge tool ownership and discovery behavior; notification APIs; handle lifetime and reattachment; configuration-resolution timing on launch/attach/input/idle; safe root-ID registration before resolution; full permission ancestry through Fresh-context children; completion/run lifecycle; built-in disabling and spawn-limit hooks; MCP/tool interception and result delivery; stable input-ID deduplication and continuation/ack boundaries; shutdown/interruption semantics; cron timezone options.
- **Decision gate:** pick the supported durable delivery path. If local mode lacks required acknowledgment/continuation hooks, use the supported step/input API or a small upstream change. Disable built-ins initially if shared spawning limits cannot be enforced.
- **Check:** one streamed reply, one toy async tool round trip, and a documented answer/evidence for each dependency-critical question.
- **Tests:** focused executable probes for grant registration timing and repeated stable input IDs. No real external side effects.

### M2 — SQLite worker and conversation store

- **Goal:** persist history without blocking the core.
- **Scope:** worker thread, bounded requests, migrations, WAL/foreign keys, ConversationStore, parent links, messages, and summaries. `ensure_root_thread` never creates rows; processed IDs are written with history; `CloseThreadTool` for compaction threads.
- **Check:** `ask` works with SQLite and database errors surface clearly.
- **Tests:** reference-store parity for offset bounds, compaction selection, inherited-history Fresh boundaries, and structural parent lookup through a Fresh child. Atomic append on failure; only user/subscription-event IDs become processed IDs (Infinity gives every item of one completion the same message ID, so message IDs are not unique keys); unknown thread IDs never become roots; compaction completes with built-ins off.
- **Done:** `shade.db` (WAL, foreign keys, `0600`) behind a worker thread taking owned closures over a bounded queue. Rows key on `(thread, ord)`. Unlike the reference store, appends, spawns, and summaries need an existing thread. Database errors at startup name the cause (newer schema, unreadable file).

### M3 — State store and replay primitives

- **Goal:** persistent runtime metadata and verified replay semantics.
- **Scope:** StateStore, processed IDs, subscriptions, pending choices; stable-input wrapper selected in M1; recoverable persisted acceptance/continuation boundaries.
- **Check:** restart/resume using both SQLite stores; repeat the same input ID through the actual runtime path.
- **Tests:** replacement of an existing pending choice; crashes around history/processed-ID persistence and replay, using the integration path selected in M1. A processed-ID row alone must not hide unfinished continuation work.
- **Done:** `States` on SQLite (metadata, subscriptions, pending choices; processed IDs share the table history writes). Thread configuration now refuses unregistered threads, so Infinity loading an arbitrary ID cannot reach the model even before grants exist. Restart is tested by starting a second system on the same database.
- **Finding for M7:** a thread reloaded with an unanswered tool call at the end of its history does not go idle; it stays live waiting for that result. After a crash mid-tool, recovery must deliver the captured result or an explicit unknown-outcome result under the same call ID, or the thread waits forever.

### M4 — Terminal chat

- **Goal:** usable resumable chat through the daemon.
- **Scope:** `shade chat` sessions over the control socket, conversation mapping, `/new`, and the shared root-registration service.
- **Check:** conversation resumes after a daemon restart; closing the client mid-turn lets the turn finish and keeps it in history.
- **Tests:** only lock/registration races not covered by dependency guarantees.
- **Done:** `conversations(adapter, key, root_id)` maps terminal names (`default`, `--thread NAME`, 1–64 bytes, no control characters) to roots; `/new` repoints the name and keeps the old root. `store::roots` is the only place roots are created, for `ask` and chat alike, always before attaching. A turn's pump ignores events until Infinity echoes its input (`UserInput`), so the tail of an earlier turn (a client that left mid-reply) cannot end it; a second `UserInput` means another terminal interrupted it, and the turn ends with an error instead of showing the other reply. A client leaving mid-turn is logged at debug, not as a failure.

### M5 — Grants and shared execution limits

- **Goal:** enforce root authority and finite execution before real tools exist.
- **Scope:** typed origins, grant storage, current-policy ceilings, management restrictions, dispatch checks, safe root registration, `now`, completion/tool/deadline counters, background capacity, and selected built-in policy.
- **Check:** omitted tool is unavailable and rejected if dispatched directly; invalid hook management caps fail startup; a looping toy run reaches a visible limit.
- **Tests:** missing grants fail closed; Fresh children retain structural root ownership; delegation cannot amplify; hooks cannot receive management authority; removed caps disappear after policy restart; child work shares limits; restart does not reset counters; new chat input does not reset old descendant budgets.
- **Done:** `policy.rs` holds typed caps and origins, config validation (`shell_network` needs `shell`; hooks cannot hold `schedules`/`watches`), ceilings, and limit checks. Grants live in `grants(root_id, origin, source, caps)`, written with the root; existing roots were migrated to terminal chat grants. A thread's authority is its `threads.root_id` grant, so Fresh children keep it. Budgets are rows per chat turn or run; `threads.budget_id` is set per turn on the root and copied to children at spawn, so a new turn never resets older work. `authority.rs` admits at three points: thread configuration (tools and prompt; missing grant fails closed), every provider request (a `ModelSource` wrapper; a refusal is `Fatal`), and every Shade tool call. Charging is one SQLite transaction; the first refusal is recorded in `budgets.exceeded` and stops everything after it. `now` is the first tool (`timezone` in config, UTC by default). Restart cannot reset counters because they exist only in SQLite, so there is no separate test. Delegation has no tool yet; its ceiling (current chat caps minus management) is tested through `Policy::effective`.
- **Finding for M7:** a limit refusal after a synchronous tool keeps history consistent (the result is synced with the call). An async result arrives as a later input, and a refused step leaves it unpersisted. The M7 shell must persist its captured result itself (F4) and redeliver it, or the call stays unanswered.

### M6 — Sandbox foundation

- **Goal:** establish actual filesystem/process isolation before exposing a shell tool.
- **Scope:** bubblewrap policy, protected-path validation, clean environment, grant-dependent vault mount, network isolation, bounded child lifecycle, Arch/NixOS path support, and explicit development opt-out.
- **Check:** run a fixed test command that writes the workspace.
- **Tests:** real bubblewrap integration tests for hidden Shade/secret paths, shell-without-vault, private process visibility, absent control sockets (including `shade.sock`), no network without `shell_network`, path overlap rejection, and descendants that try to detach. Missing/broken sandbox fails closed.
- **Done:** `[sandbox]` holds `workspace`, optional `vault`, an `env` pass-through list, and `isolation = "off"` as the development opt-out (logged as a warning). It is required and checked only when some origin holds `shell`. Paths are canonicalized (symlinks cannot point into protected paths) and may not overlap the data directory, the config directory, or the directory holding `shaded`, nor contain `$HOME`; the vault and workspace may not overlap each other. `bwrap` is found on `PATH` once and run with a cleared environment: `--unshare-all` (network added back with `--share-net`), `--die-with-parent`, `--new-session`, `--clearenv`, fixed `PATH`/`HOME=/workspace`/`TMPDIR`. System paths (`/usr`, `/bin`, `/lib*`, `/nix/store`, `/run/current-system`) are mounted read-only with top-level symlinks recreated, so merged-`/usr` and NixOS layouts both resolve; a short list of `/etc` entries is bound read-only following symlinks. The startup probe writes `/workspace/.shade-probe` and reads it back on the host, so a `bwrap` that exits 0 without running anything is refused. `Sandbox::run` keeps the first 16 KiB of each stream while draining the rest, and on timeout kills `bwrap`, which takes the PID namespace with it; head/tail capture and truncation counts are M7. The `vault` capability does not exist until MCP caps (M13), so callers pass `Access::vault` explicitly. Only Arch was exercised; the NixOS layout follows the same rules but is untested.
- **Finding for M7:** `--die-with-parent` uses `PR_SET_PDEATHSIG`, which fires when the spawning *thread* exits. Spawn shell commands from the core thread, never from a short-lived blocking thread. Abrupt daemon death cleanup still needs its subprocess test.

### M7 — Shell tool and initial operation recovery

- **Goal:** useful commands with bounded execution and honest restart outcomes.
- **Scope:** async shell tool, streaming bounded capture, deadlines, operation IDs, durable captured results, interruption classification, startup recovery, basic audit records, and shutdown cleanup. Startup recovery must answer every trailing unanswered call (see M3 finding).
- **Check:** list workspace; report nonzero exit; timeout; kill the daemon mid-command and recover without a hung tool or automatic blind retry.
- **Tests:** output retention stays bounded, UTF-8-safe truncation, process-tree cleanup on timeout/exit/death, result replay, and unknown-outcome recovery with stable call IDs.
- **Done:** `shell` is an async tool offered with `shell` (the description says whether `shell_network` is held). `execute` returns at once and the command runs in a `spawn_local` task on the core thread. The task admits the call (grant and budget), validates `command` and `timeout_seconds` (30 s default, `[sandbox] max_timeout_seconds` cap, 300 by default), records a `tool_operations` row as `running`, runs the sandbox, stores the rendered result as `done`, then delivers it with the call ID as dedup ID. A repeated dispatch of a recorded call never runs again: it gets the captured result or the unknown-outcome text. Each stream keeps its first and last 16 KiB with an omitted-bytes marker; a character cut by the omission is dropped whole. Startup recovery (`recovery.rs`, before the router serves) scans open threads for calls in the trailing run of calls/results that have no result (Infinity ignores late results anywhere else; `close_thread` is skipped), and answers each with its captured result or the F12 unknown-outcome text, marking the row `unknown`. Recovery failure stops startup. Graceful shutdown drops the `LocalSet`, so `kill_on_drop` kills running commands and their rows recover as `unknown`. `tool_operations` doubles as the basic audit record; the arguments stay in history only. The client turn pump no longer ends at a completion that stops on an async tool call: it counts `ToolCall`/`ToolResult` events and waits for the completion that follows the result.
- **Check (done):** against OpenCode Go with a scratch config: listing, `ls /nonexistent` (exit 2 and stderr), a 2 s timeout, and `kill -9` of `shaded` during `sleep 20`. After restart the call was answered with the unknown-outcome text, the model verified the workspace instead of rerunning, and the killed command never wrote its second line.
- **Finding for M8 (resolved in M8):** recovery continues threads that no client is attached to, so the model's reply after a recovered result lands in history but is shown nowhere until the conversation is next used. Output pumps for every root belong to M8.
- **Finding (from reading Infinity, not yet tested):** a recovered result is charged to the interrupted turn's budget. When that budget's `max_run_seconds` has passed (a restart after long downtime), the completion is refused and the result stays in memory only; once the thread idles, the next user message settles the call as interrupted, while `tool_operations` keeps the real result. Revisit with the M8 run lifecycle rather than resetting budgets.

### M8 — Daemon router and durable ingress

- **Goal:** one reliable acceptance/delivery path for all later adapters.
- **Scope:** `Inbound`/`Outbound`, persisted inbox/runs, safe thread/grant registration before attachment, bounded channels, output pumps, replay, concurrency admission, graceful daemon shutdown; refactor socket sessions onto the path.
- **Check:** terminal behavior preserved; accepted queued work survives restart; a slow tool does not stall ingress or database work.
- **Tests:** crash before/after commit, enqueue, durable acceptance, and acknowledgment; replay uses the same root/input ID; no accepted input is lost because it only reached a volatile queue. Shutdown drains critical writes.
- **Done:** adapters send `Inbound::Prompt { target, text, outbound }`. The router records the input in `inbox` in the same transaction that registers the root (on first use) and starts the turn's budget, then sends it under the row's ID and spawns a pump. There is no acknowledgment write: an input is settled when its ID is in `processed_ids` (written with the history that holds it) or when its turn ended with an error, which the pump records *before* the sender sees it, so a failure the owner saw is never rerun behind their back. Pumps run to the end of the turn even after the client leaves. `Outbound` is `Client` (a socket connection) or `Detached` (resumed work, logged). `Router::resume`, before serving, answers waiting tool calls (M7) and resends unsettled inputs with their own IDs, each with a detached pump, which resolves the M7 output finding for terminal roots; Discord roots get a standing destination in M9. Shutdown stops the socket and router loops, shuts Infinity down (flushing streamed history), then waits on a no-op database job, which drains every write queued before it. Queues are bounded at the adapter edge (16 inbound, 64 replies per connection). Tests cover a crash after commit, after enqueue, and after the answer (the input appears exactly once, in its own root, under its own ID) and a failed turn being settled before its sender hears.
- **Deferred to M11:** the `runs` table and `max_concurrent_background_runs`. Nothing creates background runs before the scheduler, and M11 already scopes the run schema and global capacity, so they arrive with their first producer and its tests.
- **Check (done):** against OpenCode Go with a scratch config: terminal chat unchanged; an `ask` answered while another conversation ran `sleep 20` in the shell; a chat message accepted and then `kill -9`ed before the model answered was resent at startup and answered once (the model, asked afterwards, knew the fact and said it was told once).

### M9 — Discord adapter

- **Goal:** owner chat from the phone.
- **Scope:** allowlist, DMs/mentions, message-ID deduplication, chunking, typing status, safe mentions, `/new`, and bounded adapter output.
- **Check:** authorized chat resumes after restart; unauthorized/bot input ignored; channel privacy behavior documented.
- **Tests:** Unicode/long-line chunk boundaries and duplicate message delivery producing one accepted input.
- **Done:** `adapters/discord.rs` on serenity 0.12 (client, gateway, model, rustls; no cache, no framework). `[discord]` names the token variable and a non-empty `owners` list of user IDs. Intents are `DIRECT_MESSAGES | GUILD_MESSAGES | MESSAGE_CONTENT`. Discord gives a bot a managed role with its name, and "@Shade" in autocomplete may pick the role; Discord withholds the text of such a message without Message Content (seen in the live check: `length=0`). A mention of the bot's role (`tags.bot_id`) counts as a mention of the bot; the role is looked up only for owners' messages that mention a role. Ignored messages are logged at debug with metadata only, never content. A message is heard when its author is an owner and not a bot, and it is a DM or mentions the bot; the mention is stripped and attachment URLs are appended. Each channel is a conversation (`conversations(discord, channel_id)`), `/new` repoints it, and the message ID is the inbox `source_key`, so a redelivered message is accepted once. Replies buffer text and post it when a completion ends (including one that pauses on an async tool), never at a superseded turn, in chunks of at most 2000 UTF-16 units split at line breaks, else between characters. Grapheme clusters may still be split. A reply buffer keeps 16 KiB between posts and says when it dropped the rest. Replies allow no mentions (not even the replied-to user) and, in server channels, reference the message they answer. "Typing…" lasts from receipt to the end of the turn. Resumed work in a Discord conversation posts to its channel. The gateway starts after `resume`, and shuts down before the router. The turn tracker now starts at its own input's text, so a fast second message cannot be mistaken for the first one's acceptance.
- **Check (done):** with a scratch config and data dir: DM with the `now` tool; a 15 s shell call with typing throughout; a 60-line reply split at line breaks; a server channel message without a mention ignored, with a user mention and with a role mention answered under the message; `@everyone` in a reply pinged nobody; the conversation remembered its start across a daemon restart and forgot it after `/new`.

### M10 — Notifications

- **Goal:** background work can report to the owner.
- **Scope:** fixed destination, durable local operation state, adapter acknowledgment, shared per-run allowance, runtime failure-notice allowance, unknown-outcome policy.
- **Check:** successful DM and clear no-adapter error.
- **Tests:** descendants share the quota; a crash after confirmed success replays the result; unconfirmed delivery is not claimed to be exactly once.

### M11 — Durable scheduler engine

- **Goal:** time triggers without lost or duplicated occurrence records.
- **Scope:** schedule/run schema, transactional claims/inbox insertion, cron and one-shot timing, catch-up once, timezone/DST policy, per-job overlap, global capacity, CLI inspection/pause/resume/delete.
- **Check:** a due row produces a fresh granted run and notification; queued work remains durable while capacity is exhausted.
- **Tests:** downtime coalescing; spring-forward skip; fall-back first occurrence only; ambiguous one-shot rejection; one-shot claim/disable atomicity; crash before dispatch; one active plus one coalesced pending occurrence per job.

### M12 — Schedule tools and updates

- **Goal:** create, change, pause, and remove jobs in natural language.
- **Scope:** structured timing schema; caps descriptions/delegation checks; create/list/update/delete; mutation receipts and runtime-written visibility messages; active-definition limit.
- **Check:** create and reschedule a reminder in Discord while preserving its ID/history; background/hook roots cannot manage schedules.
- **Tests:** repeated tool call after mutation reuses receipt; invalid update leaves original definition intact; updated timing/caps affect future runs only.

### M13 — MCP agent tools and recovery integration

- **Goal:** use external services with server-level authority.
- **Scope:** stdio/HTTP config, bridge registration/dispatch gating, read-only-mode documentation, server-scoped credentials, startup/use timeouts, degraded startup, result/outcome recovery, resource/output bounds.
- **Check:** vault read/write only with `vault`; unavailable server does not hang boot; inspect one interrupted toy MCP write with a status lookup or idempotency key.
- **Tests:** dispatch without server authority rejected; MCP operations with unknown result are not blindly repeated; late/duplicate results reconcile against the same call ID. Bridge limitations must be resolved rather than bypassing the authority boundary.

### M14 — Authorized bounded memory

- **Goal:** durable facts survive new threads without exposing the index to unauthorized roots.
- **Scope:** memory index reader, byte/line limits, verified refresh boundary, prompt guidance, and vault-only inclusion.
- **Check:** save a preference; new authorized conversation sees it; a hook without vault sees no index.
- **Tests:** bounded reads/truncation, multi-byte boundary, no complete line fitting, missing index, unauthorized omission, and the selected refresh lifecycle.

### M15 — Webhooks and typed condition evaluator

- **Goal:** authenticated filtered events become durable jobs.
- **Scope:** HTTP endpoint, per-hook tokens/limits, payload validation, stateless filters, provider delivery IDs, transactional acceptance, protected hook grants, and pure condition logic.
- **Check:** wrong token rejected; valid nonmatch skipped; match persisted before `202`; full queue/storage failure gets retryable error.
- **Tests:** duplicate provider delivery ID; valid identical events with different IDs; no acknowledgment before commit; invalid pointer/type errors; exact supported numeric comparison and large integers; boolean/string equality; no payload-controlled grant/origin.

### M16 — MCP snapshot watches

- **Goal:** inexpensive observed-transition triggers with explicit freshness and reconnect behavior.
- **Scope:** one client per server and shared URI pipelines, baseline state, watch tools/CLI, transactional transition+run, cooldown, failure episodes, freshness checks, backoff, caching/watch contract, and example server.
- **Check:** baseline 90k; 110k fires; 120k holds; 95k re-arms; 105k inside cooldown is suppressed; no delayed fire at cooldown expiry; a later 95k then 105k outside cooldown fires. Reconnect while still true does not repeat a fire.
- **Tests:** first true read establishes baseline; cooldown updates state without queuing a delayed run; errors preserve valid state; stale reconnect rejected; transition/run crash atomicity; multiple watches share reads/subscriptions; notification during a read schedules a reread. Document missed intermediate transitions explicitly.

### M17 — Audit and usage views

- **Goal:** inspect actions, errors, and consumption across origins.
- **Scope:** richer Infinity observer integration, safe summaries/redaction, bounded write path, audit and usage CLI, token accounting and optional configured price estimates. Reuse operation/run records already present.
- **Check:** trace one scheduled run, its grant, tool outcomes, and completion usage through the CLI.
- **Tests:** known sensitive fields redacted before persistence; observer pressure cannot allocate without bound or masquerade as critical durable acknowledgment.

### M18 — Recovery and shutdown fault verification

- **Goal:** validate the combined guarantees before unattended deployment.
- **Scope:** targeted fault-injection checks across accepted triggers, model turns, tool dispatch/results, notifications, watched transitions, descendants, and compaction; reconcile subscriptions correctly; operational diagnostics and isolated restore rehearsal.
- **Decision:** this verifies recovery already implemented alongside each feature; it is not the first recovery implementation.
- **Check:** abrupt death at selected boundaries leaves visible recoverable state, stable IDs, bounded orphan cleanup, and no fabricated success.
- **Tests:** only newly exposed cross-feature cases: unresolved one-shot versus legitimate subscription, parallel/interleaved pending calls, stale late result after recovery, and budget/overlap state across restart.

### M19 — Deployment and backups

- **Goal:** run unattended with a tested recovery procedure.
- **Scope:** systemd user unit, environment/permission setup, backup service/timer, consistent live backup, restore instructions, README, and documented initial operating limits.
- **Check:** reboot starts `shaded`, Discord and `shade chat` respond, backup runs while the daemon serves requests, and an isolated restore opens and resumes expected durable state without contacting real services during the rehearsal.
- **Tests:** only additional backup/schema compatibility invariants not already covered. Do not add broad duplicate test suites.

### Ordering rationale

- M0–M4 establish dependency facts, persistence, and a usable conversation.
- M5 establishes authority and finite execution before real external actions.
- M6/M7 introduce shell access together with isolation and result recovery.
- M8 establishes durable ingress before Discord and unattended triggers depend on it.
- M11/M12 use transactions and stable occurrence IDs from the outset.
- M13 precedes memory/watches because those rely on MCP boundaries.
- M15 reviews pure conditions with stateless webhook filtering before M16 adds observed state and reconnects.
- M18 validates combined behavior; it does not defer known correctness/security requirements.

## 5. Non-goals and limits

- Multi-user tenancy, a web UI, plugin marketplace, RAP/Lambda deployment, and a custom model provider are out of scope.
- No generic polling of MCP tools, expression language, vector database, or embedding pipeline.
- No lossless-event guarantee from snapshot-resource subscriptions. Add service-specific replayable events when needed.
- No exactly-once guarantee for arbitrary external side effects. Prefer idempotent operations and reconciliation.
- No hot policy reload initially. Restart applies config changes; job-definition edits affect future runs.
- No promise of a full filesystem or network policy from a capability name alone. MCP roots, credentials, shell mounts, sockets, and trusted executable locations are part of the boundary.
- The control socket is local and single-owner: no remote access, multi-user authentication, or stable public API. Every client command requires a running daemon.
- Per-job model overrides, semantic memory search, and advanced resource quotas wait for demonstrated need.

## 6. Risks and open implementation questions

| Risk/question | Required treatment |
| --- | --- |
| Early Infinity APIs may change or lack needed hooks. | Concrete pins, source-backed M1 notes, reference-store parity tests. Resolve durable-acceptance, root-registration, and dispatch-limit gates before dependent features. |
| Durable stores plus a volatile queue can still lose accepted work. | Persist ingress, stable root/input IDs, verified acknowledgment and continuation replay. |
| External action completed but result was lost. | Unknown-outcome result, idempotency/status lookup, no blind write replay. |
| Grant visibility differs from actual filesystem/MCP authority. | Dispatch checks, grant-dependent mounts, protected-path validation, server-enforced modes/account boundaries. |
| Prompt injection causes allowed but unwanted actions. | Narrow grants and explicit write authority; runtime receipts and audit. Grants limit authority, not task correctness. |
| MCP cache data looks current after reconnect. | Preserve observation time; stale/unavailable snapshots fail evaluation; document independent stdio cache/poller duplication. |
| Resource notifications hide brief or disconnected transitions. | Observed-transition guarantee only; durable service events for lossless requirements. |
| The control socket grants owner authority to anything that reaches it. | `0600` socket inside the protected data directory, peer UID check, never mounted into the sandbox or exposed by MCP roots, covered by sandbox tests. |
| SQLite or backup work stalls the agent core. | Dedicated DB worker, bounded requests, separate backup connection, bounded busy handling. |
| Model spawning/loops reset budgets or bypass limits. | Shared persisted budget scopes, enforced spawn hooks or disabled built-ins, admission control. |
| Shell can access host control channels or trusted code. | Minimal mounts, no host sockets, PID isolation, protected code/config/data, real sandbox tests. |
| Vault memory becomes poisoned or inconsistent. | Authorized inclusion/writes, reference-data treatment, owner-editable notes, supported conditional updates when concurrent writing appears. |

## 7. Primary references

These are implementation starting points, not a substitute for checking the pinned crate sources in M1:

- [Infinity dynamic configuration](https://infinity.hydro.run/docs/infinity-runtime/agent-systems/dynamic-configuration/)
- [Infinity AgentSystemBuilder API](https://docs.rs/infinity-agent-core/0.2.0/infinity_agent_core/system/struct.AgentSystemBuilder.html)
- [Infinity ConversationStore API](https://docs.rs/infinity-agent-core/0.2.0/infinity_agent_core/traits/trait.ConversationStore.html)
- [Infinity source repository](https://github.com/hydro-project/infinity)
- [MCP resource subscriptions and content](https://modelcontextprotocol.io/specification/2025-06-18/server/resources)
- [Bubblewrap usage and sandbox limitations](https://github.com/containers/bubblewrap)


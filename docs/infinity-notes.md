# Infinity integration notes (M1)

What Shade relies on in Infinity, checked against source rather than docs.
Each answer links to the pinned revision. Executable evidence lives in
`crates/core/tests/infinity.rs`; rerun it before any Infinity upgrade.

## Pinned versions

| Crate | Version | Notes |
| --- | --- | --- |
| `infinity-agent-core` | `=0.2.0` | Tag `infinity-agent-core-v0.2.0`, commit [`97c2f06`][rev] |
| `infinity-provider-protocol` | `=0.2.0` | Same commit. Tests use its `mock` feature. |
| `infinity-provider-rig` | `=0.2.0` | Same commit. |
| `rig-core` | `=0.31.0` | The version Infinity's own lockfile uses. |
| `infinity-mcp-bridge` | not added yet | Same commit; reviewed for M13. |
| `croner` | `4.0.2` (M11) | Reviewed for timezone and DST behavior. |

All Infinity crates share one release commit. Upgrade them together.

[rev]: https://github.com/hydro-project/infinity/tree/97c2f06ea4715bd62577a0f25182aab88a54e76b
[core]: https://github.com/hydro-project/infinity/blob/97c2f06ea4715bd62577a0f25182aab88a54e76b/crates/infinity-agent-core/src
[mcp]: https://github.com/hydro-project/infinity/blob/97c2f06ea4715bd62577a0f25182aab88a54e76b/crates/infinity-mcp-bridge/src/lib.rs

## Answers

### Root registration before configuration resolves

`LaunchingSystem::thread_handle(id)` attaches to any thread that
`ConversationStore::thread_exists` reports ([launch.rs:195][core-launch]).
`launch()` is not needed. Configuration resolves on the first input a driver
processes, not on attach ([thread.rs:143][core-thread-config],
[thread.rs:433][core-thread-step]).

**Safe path:** commit thread identity, grant, run metadata, and the first inbox
row in one SQLite transaction; then `thread_handle(id)`; then send. Probe:
`config_resolves_on_first_input_not_on_attach`.

Attaching spawns a driver that loads the thread (history, ancestor chain,
`ensure_root_thread`) and goes idle without resolving configuration.

[core-launch]: https://github.com/hydro-project/infinity/blob/97c2f06ea4715bd62577a0f25182aab88a54e76b/crates/infinity-agent-core/src/system/local/launch.rs#L195
[core-thread-config]: https://github.com/hydro-project/infinity/blob/97c2f06ea4715bd62577a0f25182aab88a54e76b/crates/infinity-agent-core/src/system/thread.rs#L143
[core-thread-step]: https://github.com/hydro-project/infinity/blob/97c2f06ea4715bd62577a0f25182aab88a54e76b/crates/infinity-agent-core/src/system/thread.rs#L433

### Failing closed

A `ThreadConfigSource::resolve` error makes the step return before any input is
prepared or any completion runs. The driver emits `AgentEvent::Info("Error: …")`
and drops that batch ([driver.rs][core-driver]). Probe:
`ungranted_thread_never_reaches_the_model`.

**Hazard:** loading any thread calls `ensure_root_thread` and ignores its
result ([event_processor.rs:196][core-ensure]). With the reference store, input
for an unknown ID silently creates a root. Probe:
`input_for_unknown_thread_creates_a_root`.

**Consequence for M2:** Shade's `ConversationStore::ensure_root_thread` must not
create rows. Root creation goes only through registration. Its error is ignored
by the caller, so the grant lookup in `resolve` is what actually stops the run.

[core-driver]: https://github.com/hydro-project/infinity/blob/97c2f06ea4715bd62577a0f25182aab88a54e76b/crates/infinity-agent-core/src/system/local/driver.rs
[core-ensure]: https://github.com/hydro-project/infinity/blob/97c2f06ea4715bd62577a0f25182aab88a54e76b/crates/infinity-agent-core/src/event_processor.rs#L196

### Permission ancestry through Fresh children

The reference `get_ancestor_chain` walks persisted parent links and ignores
Fresh boundaries ([stores.rs:323][core-stores]). Fresh only affects which
history `load_history_with_ancestors` includes. The first chain entry is the
structural root, so it is safe for grant lookup **if** Shade's store keeps the
same semantics. M2 parity tests must cover a chain through a Fresh child.

[core-stores]: https://github.com/hydro-project/infinity/blob/97c2f06ea4715bd62577a0f25182aab88a54e76b/crates/infinity-agent-core/src/stores.rs#L323

### Stable input IDs and the durability boundary

- Every input carries a dedup ID (`ThreadHandle::send(message, id)`). User text
  and subscription events are deduplicated through `StateStore` processed IDs;
  tool results against the history tail. Probe: `repeated_input_id_is_processed_once`.
- Inputs are not persisted until a completion produces output for them. A
  provider failure leaves the input in memory only.
- `HistoryManager::sync` appends history, **then** adds processed IDs in a
  separate call and ignores that call's error ([event_processor.rs:627][core-sync]).
  A crash between the two would let a replay append the same user text twice.
- `CompletionFinished` is emitted after `sync` ([thread.rs:566][core-commit]),
  but it does not prove the input was stored (see provider failure above).

**Consequence for M2/M3:** Shade's `append_messages` records processed IDs for
`User` and `SubscriptionEvent` messages in the same SQLite transaction as the
history rows. An inbox row is accepted when its input ID is in processed IDs;
check that after `CompletionFinished` or the thread's idle event. Unaccepted
rows are replayed with the same ID on startup.

[core-sync]: https://github.com/hydro-project/infinity/blob/97c2f06ea4715bd62577a0f25182aab88a54e76b/crates/infinity-agent-core/src/event_processor.rs#L627
[core-commit]: https://github.com/hydro-project/infinity/blob/97c2f06ea4715bd62577a0f25182aab88a54e76b/crates/infinity-agent-core/src/system/thread.rs#L566

### Message IDs (M2)

The ID passed with each message to `append_messages` is not unique: every item
of one completion (reasoning, text, tool calls) carries the same completion ID,
user text carries its dedup ID, and synthetic results derive theirs from the
tool call. Shade keys history rows on `(thread, ord)` and treats only processed
IDs as unique.

### Reloaded threads with an unanswered tool call (M3)

A thread whose stored history ends with a tool call and no result does not go
idle when attached: its driver stays live expecting the result. A result sent
later with the call ID is still accepted and reaches the model (processed IDs
do not hide it). If the result never comes, for example because the tool
process died with the daemon, the thread waits forever. Startup recovery (M7)
must send the captured result or an explicit unknown-outcome result.

### Local delivery path

Local mode uses unbounded in-process channels ([sender.rs:36][core-sender]) with
no acknowledgment hook. Durable delivery therefore stays in Shade (inbox +
replay with stable IDs, as F2 describes). Step mode is not needed: the
processed-ID check plus Shade's inbox gives the required boundary. Bound
Shade's own queues; Infinity's are unbounded but only receive what Shade admits.

[core-sender]: https://github.com/hydro-project/infinity/blob/97c2f06ea4715bd62577a0f25182aab88a54e76b/crates/infinity-agent-core/src/system/local/sender.rs#L36

### Completion lifecycle and stream shape

One step: prepare inputs, resolve model, `CompletionStarted`, stream
(`TextChunk`, `ThinkingStarted`/`ThinkingChunk`/`ThinkingEnded`, `ToolCall`,
`Info`), `sync`, `CompletionFinished { usage }`, then dispatch at most one async
tool call. Synchronous tools loop back inside the same step. A finished
completion with a tool call is not a finished user turn.

Provider errors: throttled and transient errors retry up to 10 times (30 s / 5 s
backoff); fatal errors (e.g. 401) end the step with an `Info` error and a
`CompletionFinished`.

**Correction (M5):** `CompletionStarted` and `ModelSource::resolve` happen once
per step, but the provider is called again inside the step after a synchronous
tool result or an unknown-tool error ([event_processor.rs:1518][core-loop]).
Counting rounds there would let a model loop on a missing tool without bound.
Shade counts every `ModelProvider::invoke_model` call instead, retries
included, and refuses with `ErrorClass::Fatal` so Infinity does not retry.

[core-loop]: https://github.com/hydro-project/infinity/blob/97c2f06ea4715bd62577a0f25182aab88a54e76b/crates/infinity-agent-core/src/event_processor.rs#L1518

`Info("Error: …")` before `CompletionStarted` means no completion will run for
that input. The `ask` pump in `router.rs` relies on this.

### One tool call per model turn

A model may stream several tool calls in one turn (OpenAI-style parallel
calls, Bedrock's concurrent ones). Infinity executes only the first: after
it, everything but the final chunk is dropped before it reaches history
(`event_processor.rs`, "Ignoring post-tool-call stream content"), and a
thread holds at most one terminal action per completion (`thread.rs`,
`assert!(action.is_none(), "bug: multiple terminal actions")`). The model
sees only the call that ran and can make the next one in a later round.
So a thread never has two calls outstanding, and recovery never meets a
dropped call in history. Checked in M18 (`parallel_calls_run_one_at_a_time`).

### Interruption and shutdown

User text arriving during a completion cancels it, keeps the partial turn, and
prefixes the new text with `<interrupt>` ([driver.rs:179][core-driver]).
`LaunchingSystem::shutdown` closes input; each driver cancels its in-flight step,
which still syncs what streamed, then exits. Already dispatched tool work is not
cancelled.

### Handles and reattachment

A handle receives a replay snapshot plus every later event. Dropping it
detaches without stopping the thread. Several handles may attach. Idle threads
release their driver; the next input respawns it and resolves configuration
again.

### Built-in tools and spawn limits

`SpawnThreadTool` has no limit hook, so `without_builtin_tools()` stays on and
model-controlled subagents are deferred (as §2.7 allows).

**Catch:** auto-compaction spawns a child and tells it to call `close_thread`
([event_processor.rs:994][core-compaction]). With built-ins off, compaction
cannot finish. The tool structs are public, so Shade's `ThreadConfigSource`
adds `CloseThreadTool` for compaction threads (`is_compaction_thread`). Wire
and test this in M2, where compaction becomes reachable.

[core-compaction]: https://github.com/hydro-project/infinity/blob/97c2f06ea4715bd62577a0f25182aab88a54e76b/crates/infinity-agent-core/src/event_processor.rs#L994

### Model provider

- `infinity-provider-rig` with rig's Anthropic client or its OpenAI Chat
  Completions client (`openai::CompletionsClient`), built through
  `Client::builder()` so `base_url` and headers can be set. `from_env` panics on
  a missing key; Shade reads the configured variable itself.
- `ModelSource::resolve` receives the thread ID every round, so `model.rs`
  builds the rig client there (sharing one reqwest pool) and sends the thread ID
  in the configured `session_header`. OpenCode Go returns 400
  `MissingSessionID` without `x-opencode-session`.
- rig's Chat Completions client ignores `max_tokens` and neither streams nor
  returns reasoning content (`reasoning_content`); assistant reasoning in
  history is dropped. Models that require reasoning to be sent back across tool
  calls may degrade or fail there. Verify with a real key.
- **Bug:** Infinity sends `max_tokens: None` ([event_processor.rs:1530][core-max])
  and the rig bridge does not fill it from `ModelEntry`, so Anthropic rejects
  every request. `model.rs` wraps the model to set it. Worth reporting upstream.
- Context window and output cap come from config; `StaticModel` only checks the
  ID against the one-entry catalog.
- Open: Claude Opus 5.5 always thinks and ties thinking blocks to the
  conversation; edited history can be rejected. Infinity's compaction rewrites
  the loaded history. Verify with a real key whether rig 0.31 round-trips
  thinking blocks and whether compacted conversations are accepted.

[core-max]: https://github.com/hydro-project/infinity/blob/97c2f06ea4715bd62577a0f25182aab88a54e76b/crates/infinity-agent-core/src/event_processor.rs#L1530

### MCP bridge

- Each server becomes two tools: `{server}_list_tools` and `{server}_invoke_tool`.
  One lazily connected, serialized session per `McpClient`.
- Only `tools/list` and `tools/call`. No resources, subscriptions, or
  notifications: watches need `rmcp` (F7/F8), as planned.
- `McpTool::execute` spawns the call and sends the result itself
  ([lib.rs:650][mcp]), so Shade cannot persist it first. Shade wraps the public
  `McpClient::dispatch` in its own tool: check authority, record the operation,
  call, persist the result, then deliver with the call ID as dedup ID.
- Errors and failures come back as text (`MCP tool error: …`); images become
  placeholders.

### Cron and time zones

`croner` 4.0.2 evaluates patterns on `chrono` `DateTime<Tz>` with `chrono-tz`.
Its DST rules differ from F6:

| Case | croner | F6 policy | Shade adjustment |
| --- | --- | --- | --- |
| Spring-forward gap, fixed time | Runs at the first valid time after the gap | Skip | Drop occurrences whose wall time no longer matches the pattern |
| Fall-back overlap, fixed time | Once, first pass | First only | None |
| Fall-back overlap, wildcard/interval | Both passes | First only | Drop second-pass occurrences (same local time, later instant) |

Both adjustments are pure functions over `(pattern, tz, instant)`; test them in M11.

## Open items

- ~~Real-key check~~: done through OpenCode Go (Chat Completions); `shade ask`
  streamed a reply. Anthropic directly is verified up to authentication.
- Reasoning round trip through rig's Chat Completions client (above).
- Thinking-block round trip and compaction against Claude Opus 5.5 (above).

### Owner-chat queue and client events

Infinity has one completion in flight per thread, and user text can interrupt it
(`system/local/driver.rs`). Shade withholds queued owner inputs until the prior
turn, including asynchronous tool rounds, ends. Distinct roots can still run
concurrently. The probe `sequential_turns_with_identical_text_remain_distinct`
checks that sequential, equal-text inputs preserve separate responses; router
queue tests check that dispatch is withheld through tools and retry deadlines.

`AgentEvent::ToolCall` names the tool but carries no call ID;
`AgentEvent::ToolResult` carries display segments without a call ID. The client
API reports start names and result arrivals without inventing a pairing.
`CompletionFinished` remains the history-sync boundary, not the end of a turn
that is waiting for an asynchronous tool. Provider retries now retain the
budget deadline resolved for that completion, rather than borrowing a later
turn's deadline.

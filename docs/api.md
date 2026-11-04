# Client API v1

The client API connects native apps and scripts to the same owner conversations
used by the terminal and Discord. It uses HTTP JSON for requests and
Server-Sent Events (SSE) for live output. Rust clients can use the types in
`shade-proto::api`; other clients need only an HTTP library.

The server lives in `shade-http`. Embedded Rust adapters use the typed
`shade_core::Client` handle for conversation operations and event subscriptions;
core returns domain errors, and the HTTP layer assigns status codes. The daemon
owns configuration parsing, listener startup, and shutdown signals. Queue,
permission, history projection, and recovery behavior stay in core.

## Enable it

Add this to `config.toml` and restart `shaded`:

```toml
[api]
token_env = "SHADE_API_TOKEN"

[http]
listen = "127.0.0.1:8787"
```

Put `SHADE_API_TOKEN=<token>` in the config directory's `.env` (`chmod 600`).
Generate a token with `openssl rand -hex 32`. Startup requires at least 32
bytes. Send it as `Authorization: Bearer <token>` on every API request.

The token gives access to all owner chats and their existing `[chat]`
capabilities, including approval decisions. API requests cannot choose grants.
Guest chats, scheduled runs, webhook runs, and internal threads are excluded.
New API conversations inherit `[chat]` too.

The listener accepts loopback and this machine's Tailscale addresses. For
access from your other devices, keep loopback and use:

```sh
tailscale serve --bg --https=443 http://127.0.0.1:8787
```

The API starts even when no hooks are configured. Hooks keep their own tokens
and existing `/hook/<name>` behavior on the same listener. CORS, browser login,
and multiple user accounts are outside v1.

## Endpoints

All paths below start with `/v1`. POST requests require
`Content-Type: application/json`; the complete UTF-8 body is limited to 64 KiB.
Unknown JSON fields are rejected. Identifiers returned by Shade are opaque.

| Method and path | Request | Response |
| --- | --- | --- |
| `POST /conversations` | `{}` or `{"name":"Work"}` | `201` conversation |
| `GET /conversations` | Optional `limit`, `cursor` | `200` page of conversations |
| `GET /conversations/{id}` | — | `200` conversation detail |
| `GET /conversations/{id}/messages` | Optional `limit`, `cursor` | `200` page of saved history |
| `POST /conversations/{id}/messages` | `{"text":"Hello","client_message_id":"unique-client-id"}` | `202` accepted turn |
| `GET /conversations/{id}/turns` | Optional `limit`, `cursor` | `200` page of turns |
| `GET /conversations/{id}/turns/{turn_id}` | — | `200` turn |
| `GET /conversations/{id}/events` | — | `200` SSE stream |
| `POST /conversations/{id}/approvals/{code}` | `{"decision":"approve"}` or `{"decision":"reject"}` | `200` `{"text":"..."}` receipt |

A conversation has `id`, `source` (`api`, `terminal`, or `discord`), nullable
`name`, `created_at`, and `activity`. Times are integer Unix seconds. The stable
ID is its root thread ID. Histories retained by `/new`, and unnamed one-shot
owner conversations, remain accessible. Creating a conversation always creates
a new root; reusing a name moves that name to the new root and retains the old
history under its ID.

Conversation detail contains `conversation`, nullable `active_turn`,
`queued_turns`, and pending `approvals`. An approval has `code`, `text`, and
`created_at`. Decisions use the same validation as the terminal and Discord:
stale or expired approvals cannot be applied. A decision must belong to the
requested conversation. The receipt returns immediately; the agent's follow-up
joins the conversation queue. General schedule administration is outside v1.

Pages have `{"items":[...],"next_cursor":null}`. Pass `next_cursor` unchanged to
get the next page; stop when it is null. `limit` defaults to 50 and accepts
1–100. Conversations sort by descending activity and ID. History and turns
sort oldest first. History cursors advance across hidden rows too, so an empty
page can still have a next cursor. Pagination does not freeze a changing list.

History entries have `id` (the stable row ordinal as a string), nullable
`turn_id`, and one of:

```json
{"type":"user","text":"Hello"}
{"type":"assistant","text":"Hello!"}
{"type":"tool_started","name":"now"}
{"type":"tool_finished"}
{"type":"approval","code":"abc123","text":"..."}
```

Only the content fields are shown above. `turn_id` associates tracked history
with an accepted input; old untracked inputs can have null. Reasoning, system
prompts, tool arguments, and tool result bodies are omitted. Model compaction
does not remove the saved history available through this endpoint.

## Acceptance, ordering, and recovery

A turn contains `id`, `conversation_id`, original `text`, `status`,
`received_at`, nullable `started_at`, nullable `finished_at`, and nullable
`error`. Status is `queued`, `running`, `completed`, `failed`, or `interrupted`.

`202` means the input is committed to SQLite. It may already be running or
finished when the response arrives. Use a fresh `client_message_id` for each
logical message, and reuse it with identical text when retrying an uncertain
request. The ID is scoped to the conversation. Identical retries return the
same turn, even after completion; different text with the same ID returns
`409`. Client IDs must be 1–128 bytes without control characters. Text must be
nonempty; the request-body limit includes JSON escaping and field overhead.

Owner messages queue in acceptance order across the API, terminal, and Discord.
Only one turn in a conversation runs at once, including its tool calls. Other
conversations run concurrently under the model limits. Waiting messages do not
start a budget or deadline. API replies stay with API clients; continuing a
Discord conversation through the API does not also post the answer on Discord.
Explicit `notify` tool calls retain their configured behavior.

Defaults in `[limits]`:

```toml
max_queued_chat_messages = 32
max_queued_chat_messages_total = 256
```

These count waiting inputs, excluding the active turn. Excess inputs receive
`429` before acceptance. Client disconnection does not cancel accepted work.
After a daemon restart, unprocessed inputs resume with their stable IDs and
original execution deadlines if already started. Processed inputs resume only
when existing tool recovery can continue them; otherwise an unfinished turn is
reported as interrupted. Completed inputs are never blindly rerun.

## Streaming and reconnecting

Each SSE event is one `data: <JSON>` record followed by a blank line. The stream
sends a snapshot first, then live events in observation order:

```json
{"type":"snapshot","active_turn":"turn-id","partial_text":"Working...","pending_tools":1}
{"type":"turn_status","turn_id":"turn-id","status":"running","error":null}
{"type":"text_delta","turn_id":"turn-id","text":"Hello"}
{"type":"tool_started","turn_id":"turn-id","name":"now"}
{"type":"tool_finished","turn_id":"turn-id"}
{"type":"approval","turn_id":"turn-id","code":"abc123","text":"..."}
{"type":"approval_decided","code":"abc123","decision":"approve"}
{"type":"turn_status","turn_id":"turn-id","status":"completed","error":null}
```

`active_turn` is null when idle. The snapshot's `partial_text` is the provisional
assistant text accumulated for that active turn; it can overlap saved messages
from earlier tool rounds. Use `turn_id` to render one provisional reply for the
active turn, rather than appending that text to its saved assistant entries.
Tool result arrivals do not carry call IDs, so v1 does not promise exact pairing
between live starts and finishes.

On connection or reconnection:

1. Open the event stream and read its initial snapshot; buffer subsequent events.
2. Load conversation detail, turns, and saved history. Group history by turn ID.
3. Render completed turns from saved history and the active reply from the
   snapshot plus its text deltas. Apply queued status events in order.
4. On a terminal status, reload that turn and its saved history and replace the
   provisional reply. Poll these reads if the stream is unavailable.

The snapshot and live subscription are installed together. Events have no
persistent sequence IDs; `Last-Event-ID` replay is unsupported. Heartbeat
comments arrive every 15 seconds. Each subscriber buffers 64 live events;
lagging subscribers are disconnected and must reload state. Connections share
the HTTP listener's 32-connection limit.

## Script examples

Set these in your client shell (the daemon's `.env` does not export them):

```sh
export SHADE_URL=http://127.0.0.1:8787
export SHADE_API_TOKEN='<your token>'
```

Create and list conversations:

```sh
curl -sS "$SHADE_URL/v1/conversations" \
  -H "Authorization: Bearer $SHADE_API_TOKEN" \
  -H 'Content-Type: application/json' -d '{"name":"Work"}'

curl -sS "$SHADE_URL/v1/conversations?limit=50" \
  -H "Authorization: Bearer $SHADE_API_TOKEN"
```

Copy a conversation ID into `conversation`. In one terminal, watch its events:

```sh
conversation='<conversation-id>'
curl -N "$SHADE_URL/v1/conversations/$conversation/events" \
  -H "Authorization: Bearer $SHADE_API_TOKEN"
```

In another, submit a message. Keep the same client ID if this request needs a retry:

```sh
curl -sS "$SHADE_URL/v1/conversations/$conversation/messages" \
  -H "Authorization: Bearer $SHADE_API_TOKEN" \
  -H 'Content-Type: application/json' \
  -d '{"text":"What time is it?","client_message_id":"example-message-1"}'

curl -sS "$SHADE_URL/v1/conversations/$conversation/messages" \
  -H "Authorization: Bearer $SHADE_API_TOKEN"
```

Python scripts can submit without additional packages:

```python
import json
import os
import urllib.request
import uuid

base = os.environ.get("SHADE_URL", "http://127.0.0.1:8787")
token = os.environ["SHADE_API_TOKEN"]
conversation = "<conversation-id>"
message = {"text": "Hello, Shade", "client_message_id": str(uuid.uuid4())}
request = urllib.request.Request(
    f"{base}/v1/conversations/{conversation}/messages",
    data=json.dumps(message).encode(),
    headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(request, timeout=30) as response:
    turn = json.load(response)
print(turn["id"], turn["status"])
```

## Errors

Errors use `{"error":{"code":"...","message":"..."}}`:

| Status | Meaning |
| --- | --- |
| `400 invalid_request` | Invalid JSON, fields, name, text, client ID, or pagination |
| `401 unauthorized` | Missing or wrong API bearer token |
| `404 not_found` | Unknown route, inaccessible conversation, turn, or approval |
| `409 conflict` | Reused message ID with different text, or approval validation failed |
| `413 invalid_body` | Body exceeds 64 KiB or cannot be read |
| `415 unsupported_media_type` | Missing or unsupported content type |
| `429 queue_full` | Waiting-message limit reached; retry later |
| `503 unavailable` | Storage, router capacity, shutdown, or request timeout prevented confirmation |

Retryable responses include `Retry-After: 5`. After a timeout or `503`, retry a
message with the same client ID. An approval whose receipt was lost may already
have applied: inspect pending approvals and the conversation before trying again.

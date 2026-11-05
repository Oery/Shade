# Writing MCP servers for Shade

Shade connects to every server over stdio with the 2026-07-28 MCP lifecycle
(`server/discover`). There is no legacy `initialize` fallback, so a server must
answer `server/discover`. An rmcp 3.8 server does.

A server's name is its capability. Holding it grants the server's tools and
resources alike.

## Resources

A server advertising `resources` gets two extra tools:

- `<server>_resources` lists its resources and templates.
- `<server>_read_resource` reads one by URI.

Avoid naming a tool `resources` or `read_resource`: the server's own tool wins
and hides Shade's.

Results are cut at 16 KiB for the model. Binary content is named, not shown.

## Watchable resources

A watch needs more than a readable resource. To be watchable, a server must:

1. **Advertise `resources.subscribe` and accept `subscriptions/listen`.** In
   rmcp, implement `accepted_subscription_filter` and `listen`. The deprecated
   `resources/subscribe` is not used. Shade opens one subscription per URI and
   refuses a watch on a URI the acknowledgment leaves out.
2. **Return one JSON snapshot.** A read of the URI must have exactly one text
   entry for that URI, holding valid JSON of at most 16 KiB. Missing, ambiguous,
   invalid or larger content is an error, not a false condition.
3. **Notify when the meaningful value or its availability changes.** Do not
   notify because a fetch timestamp moved: every notification costs a read.
   A notification carries no value. Shade reads afterwards.
4. **Keep reads safe.** A read is an observation. Shade reads on every change
   and every reconnect, so nothing consequential may happen behind one.

Upstream polling, if a service needs it, is the server's job. Shade never polls.

### Freshness

A watch may set `max_age_seconds`. The resource must then carry the top-level
fields `observed_at` and `availability`, next to the data it publishes:

```json
{
  "value": { "price": 100500 },
  "observed_at": "2026-11-02T20:00:00Z",
  "availability": "available"
}
```

Here the pointer is `/value/price`.

`observed_at` is an RFC 3339 time saying when the upstream value was actually
observed. A cache read must not reset it. `availability` must be `available`.
Anything else, an old value, or a timestamp over a minute in the future, is an
evaluation error: the watch keeps its state and records the failure.

## What a watch guarantees

A notification means "read this again", not "here is an event". A watch fires
on **observed** transitions:

- A value that came and went between two reads is never seen.
- A disconnection can hide any number of changes. On reconnect Shade listens,
  reads, and compares with the state it persisted, so a value that merely still
  matches does not fire again, and one that changed may.
- The first reading sets a baseline and never fires, even if it already matches.

A service that needs every event should offer durable events with IDs and
cursors, or deliver them through a webhook with a delivery ID. Do not expect
Shade to reconstruct history from snapshots.

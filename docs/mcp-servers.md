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
3. **Notify when the meaningful value changes, and after every failed upstream
   attempt.** Do not notify because a fetch timestamp moved: every notification
   costs a read. A notification carries no value. Shade reads afterwards.
4. **Keep reads safe.** A read is an observation. Shade reads on every change
   and every reconnect, so nothing consequential may happen behind one.

Upstream polling, if a service needs it, is the server's job. Shade never polls.

### Availability

A resource that cannot tell the truth should say so with a top-level
`availability` field. Anything but `available` is a failed observation for every
watch on it, whatever else the document holds: the watch keeps its state and
records the failure, and five in a row alert the owner, once.

A failed read only counts when Shade reads, which is why rule 3 asks for a
notification after every failed attempt. Otherwise a dead login counts as one
failure and never reaches the alert. `watch_create` does not check availability,
because a resource may only come alive once it is listened to.

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
observed. A cache read must not reset it. An old value, or a timestamp over a
minute in the future, is an evaluation error, handled as above.

### Lists of items

A watch on `items` follows a list instead of testing a condition, for things
that are each worth hearing about, such as new events. `items` is the JSON
pointer of a list whose entries each have an `id`, a string or a number:

```json
{
  "value": { "new_events": [{ "id": 123, "name": "Rust workshop" }] },
  "availability": "available"
}
```

Here `items` is `/value/new_events`.

Shade remembers each ID for 30 days after it was last listed, and fires only for
IDs it has not seen, passing the run just those entries. So an entry may stay
listed as long as the server likes: rereads, reconnects and restarts fire nothing
for it. There is no cooldown, as one would lose entries for good, and a firing
that waits for capacity keeps its entries.

A server can publish everything current, or only what is recent. If it publishes
only what is recent, keep an entry listed for longer than a reconnect takes.
Entries that come and go between two reads are never seen.

## What a watch guarantees

A notification means "read this again", not "here is an event". A watch on a
condition fires on **observed** transitions:

- A value that came and went between two reads is never seen.
- A disconnection can hide any number of changes. On reconnect Shade listens,
  reads, and compares with the state it persisted, so a value that merely still
  matches does not fire again, and one that changed may.
- The first reading sets a baseline and never fires, even if it already matches.

A service whose changes are new things should publish them as a list of items.
One that needs every event, even short-lived ones, should offer durable events
with cursors, or deliver them through a webhook with a delivery ID. Do not
expect Shade to reconstruct history from snapshots.

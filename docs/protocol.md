# Wire protocol and delivery semantics

One WebSocket per client at `ws://HOST:PORT/ws` (or `wss://` with `--tls-cert/--tls-key`). Every frame is a JSON text frame with a `type` and a protocol version `v` (currently `1`). The models live in `src/sigbroker/schema.py`.

## Versioning

- `v` is the **major** version. A peer rejects a frame with a version it does not support (`error` with code `unsupported_version`).
- Within a major version, changes are **additive only**: new optional fields, new frame types a peer may ignore. Every model ignores unknown fields, so an older client keeps working when the broker adds one (there is a test for this).
- Removing or renaming a field, changing a type or a meaning bumps `v`. A broker can serve two majors side by side by listing both in `SUPPORTED_VERSIONS`.

## Handshake

```
client -> {"type":"hello","v":1,"token":"<jwt>","client_id":"desk-a","slow_policy":"drop_oldest"}
broker -> {"type":"welcome","v":1,"session":"9f..","role":"subscriber","heartbeat_s":5.0,
           "slow_policy":"drop_oldest","max_queue":1000}
```

The token goes in the first frame, not the URL, so it does not end up in proxy or access logs (and browsers cannot set WebSocket headers anyway). It is an HS256 JWT with `sub`, `role` (`subscriber` or `publisher`), `topics` (fnmatch patterns such as `signals.*`), `iat`, `exp` and `iss=sigbroker`. A bad or missing token gets `error/unauthorized` and close code **4001** within `auth_timeout_s` (5 s). The broker closes a session with **4003** when its token expires. `slow_policy` in `hello` is a request; the broker may ignore it (`allow_policy_override`).

Roles are separate on purpose: a subscriber token cannot publish and a publisher token cannot subscribe (`error/forbidden`).

## Subscribing and resuming

```
client -> {"type":"subscribe","topic":"signals.rates","last_seq":41,"epoch":"3c1e.."}
broker -> {"type":"subscribed","topic":"signals.rates","epoch":"3c1e..","head_seq":57,"first_seq":1,"resume_from":42}
broker -> {"type":"event","topic":"signals.rates","seq":42,"msg_id":"..","published_ns":..,"payload":{..}}
...
```

| `last_seq` | meaning |
|---|---|
| omitted / `null` | only messages published from now on |
| `0` | everything the log still holds |
| `N` | everything after `N` |

- Sequence numbers are **per topic**, start at 1 and have no holes: the broker assigns them under a per-topic lock, and the backend's append is atomic.
- `epoch` identifies the log's lifetime. If the client's epoch differs from the broker's (the log was wiped, e.g. an in-memory broker restarted), the broker replays from the start of the log and the SDK reports an `epoch_reset` gap instead of silently dropping "duplicates" that are really new messages. A `last_seq` beyond the head with no epoch is rejected (`seq_ahead`), for the same reason.
- If the log no longer holds `last_seq + 1` (retention), the broker sends `{"type":"gap","reason":"retention","from_seq":..,"to_seq":..}` before the first event.
- Sending `subscribe` again for a topic already subscribed replaces the cursor: the broker drops what it had queued for that topic and replays from the new `last_seq`. The SDK uses this to re-sync after a gap.

## Batches

A client that sends `"batch": true` in `hello` (the SDK does) may receive

```
{"type":"batch","v":1,"events":[{"type":"event",...,"seq":42,...},{"type":"event",...,"seq":43,...}]}
```

The broker never waits to fill a batch: when its writer finds more than one event already queued for the connection (up to `max_batch`, 256), it sends them in one frame instead of one frame each, in order, and never across a control frame. At low load every batch would be a single event, so plain `event` frames are sent. Clients that do not opt in never see `batch`, which keeps the change additive within `v: 1`.

## Liveness

Two layers, because they catch different failures:

- **WebSocket ping/pong** (every 20 s, 20 s timeout, both sides) checks that the TCP path is alive.
- **App heartbeats**: the broker sends `{"type":"heartbeat","ts_ns":..,"heads":{"signals.rates":57}}` every `heartbeat_s`. The SDK treats `heartbeat_misses x heartbeat_s` (default 3 x 5 s) without any frame as a dead connection and reconnects. This also catches a broker whose event loop is stuck while the kernel still answers TCP. `heads` lets a client see how far behind it is.

## Slow consumers

Each subscriber connection has a bounded outbox (`max_queue` events; control frames do not count). What happens when it is full depends on the session's policy:

| policy | behaviour | for |
|---|---|---|
| `disconnect` (default) | the cursor waits for space; if the outbox stays full for `slow_grace_s` (it counts as recovered only after draining to half), the broker closes with **4008**. The SDK reconnects and resumes from its `last_seq`: no loss, the cost is a replay. | trading desks: every message, in order |
| `drop_oldest` | the oldest queued event is dropped at once and a `gap` notice with `reason: slow_consumer` takes its place (adjacent drops merge). The SDK either re-fetches the range from the log (`gap_mode="resync"`) or reports it and moves on (`gap_mode="skip"`). | dashboards: the latest value matters more than every value |

The policy applies to **live** traffic. A catch-up replay (everything up to the head at subscribe time) is paced by the consumer: the cursor simply waits for space, because the backlog lives in the log, not in broker memory. The publisher never waits for any subscriber.

Before the outbox fills, the operating system's socket buffers absorb a stalled consumer's traffic. On the test machine that was over a thousand small frames on loopback, so a policy acts later than `max_queue` alone suggests (see the Limitations in the README).

## Publishing

```
publisher -> {"type":"publish","topic":"signals.rates","msg_id":"run-7-UST10Y-0931","payload":{..},"sent_ns":..}
broker    -> {"type":"ack","topic":"signals.rates","msg_id":"run-7-UST10Y-0931","seq":58,"duplicate":false}
```

- `msg_id` is the idempotency key, per topic. A repeat within the idempotency window returns the original `seq` with `duplicate: true` and stores nothing. The window is: SQLite, as long as the message is retained; memory, the last 100,000 `msg_id`s per topic (`idempotency_window`); Redis, `idempotency_ttl_s` (24 h).
- The ack is sent only after the backend append returns (SQLite: committed; Redis: the Lua script ran). The publisher SDK keeps a message pending until its ack arrives and re-sends pending messages in order, with the same `msg_id`, after a reconnect.
- The payload is validated against the signal schema (`kind` = `score` or `alert`); invalid payloads get `error/invalid_payload` with the `msg_id`, and the SDK raises `PublishError`.
- During shutdown the broker answers `error/shutting_down`; the SDK keeps the message and re-sends it to the next broker.
- `sent_ns` is opaque to the broker and echoed to subscribers. The load generator puts `time.perf_counter_ns()` in it to measure latency on one machine.

## Shutdown

On SIGTERM (or `Broker.stop()`), the broker stops accepting connections, stops each subscription's cursor, flushes what is already queued, sends `{"type":"bye","reason":"shutdown","reconnect_after_ms":500}` and closes with **1001**. `/healthz` returns 503 while draining. Clients reconnect after a jittered `reconnect_after_ms` to whichever broker takes over the port or the load-balancer address.

## Close codes

| code | meaning | SDK reaction |
|---|---|---|
| 1001 | broker shutting down | reconnect after the `bye` hint |
| 1006 | connection dropped | reconnect with back-off |
| 1011 | keepalive ping timeout or internal error | reconnect with back-off |
| 4001 | unauthorized | stop, raise `SubscriberError` |
| 4003 | token expired | stop, raise `SubscriberError` (mint a new token) |
| 4008 | slow consumer | reconnect and resume |

## Delivery guarantee, precisely

On the wire: **at-least-once**, ordered per topic. To the application, through the SDK: **each seq once, in order, per topic**, as long as the log still holds the messages and the client keeps its cursor. Messages the log cannot supply are never skipped silently: they are reported as gaps with a reason. With a persistent cursor store, a message counts as handled when the application asks for the next one; after an application crash the last uncommitted messages (up to `min_interval_s` of them for the file store) come again, so an application that must not act twice should also key its own side effects on `(topic, seq)`. The cursor stores each delivery's own epoch with its seq, so a cursor saved while the application was still draining messages from before a log reset still points into the old log, and the restart is reported as an `epoch_reset` rather than skipping new messages.

**The assumption under all of this: the log never loses an acknowledged append unless it also loses its epoch.** If a log drops acked messages but keeps its epoch, a client's `last_seq` would point past messages that were later re-assigned to new content, and the SDK cannot detect that. What that requires per backend:

- **Redis**: AOF with `appendfsync always` (the compose file uses it; `everysec` is faster but can lose about a second of acked appends on a crash), `maxmemory-policy noeviction` (otherwise Redis may evict stream entries or the counter under memory pressure), and no failover to an asynchronous replica that missed acked writes. If a Redis is ever restored from an older snapshot, delete its `epoch` key so clients see a reset.
- **SQLite**: `synchronous=NORMAL` survives a process crash but not a power cut; use `FULL` if that matters. Restoring an older copy of the file has the same issue as an old Redis snapshot.
- **Memory**: loses everything on restart, and always gets a new epoch, so it never violates the assumption.

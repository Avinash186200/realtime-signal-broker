# Changelog

All notable changes are listed here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/) and the project
uses [Semantic Versioning](https://semver.org/).

## [1.0.0] - 2026-11-23

### Added

- WebSocket broker (asyncio, `websockets`): opt-in batch frames, HS256 token auth with roles and topic patterns, per-topic gap-free sequence numbers, replay from a client's `last_seq`, app-level heartbeats with per-topic heads plus WebSocket ping/pong, a bounded outbox per subscriber with two slow-consumer policies (`disconnect`, `drop_oldest` with gap notices), paced catch-up replay, graceful shutdown (drain, `bye`, close 1001), token-expiry close, optional TLS, `/metrics` (Prometheus) and `/healthz` on the same port.
- Replay logs: Redis Streams (atomic Lua append, `{seq}-0` entry ids, idempotency keys with TTL, epoch), SQLite (durable, single node) and in-memory (tests), all behind one contract test suite.
- Publisher SDK with idempotency keys and in-order re-send after reconnect.
- Subscriber SDK: reconnect with exponential back-off and full jitter, resume from `last_seq` with an epoch check, client-side dedup, gap detection with re-sync or skip-and-report, heartbeat timeout, cursor stores (memory, JSON file), typed pydantic signal payloads.
- Versioned wire protocol (pydantic models, additive-only changes within a major version).
- Fault-injection proxy and a seeded scenario suite (link cuts, partitions, broker crash and graceful restart, application restart, subscriber stalls) with development and held-out seeds; hypothesis property tests for the tracker and for random fault schedules.
- Load generator (broker, publisher and subscribers in separate processes) reporting p50/p95/p99 end-to-end latency, throughput and loss per cell, plus a catch-up-after-outage measurement.
- Dockerfile and docker-compose (Redis 7 + broker, optional demo publisher and subscriber), CLI (`serve`, `token`, `publish`, `subscribe`, `demo`, `bench`, `load`, `report`), CI on ubuntu (Python 3.11) with a gitleaks secret scan.

### Fixed during development

- Redis `first_seq` read the stream and the head counter in two round trips; an append landing in between made an empty topic report "first seq is 2", and new subscribers silently skipped seq 1. Found only against a real Redis; now one `MULTI/EXEC`.
- A publish whose connection died while the Redis append was in flight could leave a hole in the broker's recent-events cache, because the session's reader was cancelled mid-frame. The reader now finishes on its own when the socket closes, and the cache is cleared if it ever sees a non-consecutive seq. (A first fix wrapped each append in `asyncio.shield`; that forced a task switch per publish, stopped events from batching under load, and on the load benchmark took 10 subscribers at 1,000 msg/s from milliseconds to seconds of latency. It was replaced.)
- `Subscriber.close()` flushed the cursor before it stopped handing messages to the application; a consumer still iterating could take more buffered messages, which a restarted application then received again. Found by held-out scenario seed 103; regression test added.
- `FileCursorStore` skipped its first write when the monotonic clock was smaller than `min_interval_s` (a freshly started Linux container). Found by running the suite in `python:3.11-slim`.
- A cursor saved after a log reset paired the new epoch with the seq of an old-epoch message the application was still draining; a restarted application then skipped new-epoch seqs 1..seq with no gap report. `Delivery` now carries its epoch and the cursor stores it. Found by an independent review; regression test added.
- The Redis append script incremented the counter before `XADD`; if `XADD` failed, the seq was used up and left a hole. It now writes the counter only after `XADD` succeeds. Keys use `{topic}` hash tags.
- A duplicate publish whose seq was ahead of the broker's head (another writer appended to the shared log) now resets the cache and head.
- Topics created through wildcard tokens are capped (`max_topics`, idle ones evicted with their metric labels); shutdown is checked after authentication and on subscribe; absurdly nested JSON is a `bad_frame` instead of a `RecursionError`; the cursor file is fsynced before the rename and a corrupt file is a clear error; the publisher counts only true re-sends and no longer sends a frame twice on one connection.
- Batch frames: a client that says `batch: true` gets consecutive queued events in one WebSocket frame. Nothing waits to fill a batch; it only forms when events are already queued, which saves one socket write per event under load.

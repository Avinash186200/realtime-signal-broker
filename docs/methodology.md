# How the numbers are made

Two benchmarks, with different kinds of claims.

| | correctness suite (`make bench`) | load benchmark (`make load`) |
|---|---|---|
| claim | no message lost, duplicated or reordered for the application, under faults | how latency and throughput behave on one machine |
| output | `results/correctness.json` | `results/load.json` |
| reproducible? | yes: every field not ending in `_observed` is deterministic, and a test reruns the suite and compares | no: depends on the hardware and OS; only the zero-loss fields are checked |
| in CI | full rerun (several minutes) | one tiny smoke cell |

## Correctness suite

Code: `src/sigbroker/scenarios.py`. One scenario:

1. Start a broker on a fresh log (SQLite file, fakeredis, or a real Redis) and a `ChaosProxy` in front of it. All clients connect through the proxy.
2. Start six subscribers on two topics, each with its own settings (see the table in the README).
3. Publish a seeded stream: 600 messages, plus a 5,000-message burst while the three "slow" subscribers stop handling messages (a stall). The burst is sized so that the stalled connections' socket buffers fill and the broker's slow-consumer policies actually act. Topic choice and message ids come from the seed.
4. Inject six faults at seeded message indices, drawn from: `cut` (abort every TCP connection through the proxy, mid-frame), `partition` (cut and refuse new connections for 150-400 ms), `crash` (`Broker.abort()`: drop every connection without a close handshake, throw away the broker's state, start a new broker on the same log), `restart` (graceful stop, then a new broker), `app_restart` (close one subscriber and start a new one from its cursor file). No fault lands inside the stall.
5. Wait until every subscriber has reached the head of both topics, then check:
   - the five strict subscribers' received sequences are exactly `[1, 2, ..., head]` per topic (this one comparison catches loss, duplication and reordering);
   - the skip-mode dashboard received each seq at most once, in order, and every seq it did not receive is inside a gap it was told about (none unaccounted, none both delivered and reported);
   - the log holds every published `msg_id` exactly once, with seqs `1..n` and no holes.

A scenario passes only if all checks pass. Timing-dependent counts (reconnects, re-syncs, drops, slow-consumer disconnects, duration) are recorded with an `_observed` suffix: they vary from run to run and are not compared.

### Development and held-out seeds

While building I ran seeds 0-9 (and 0-4 on fakeredis) many times, and fixed what they found. The held-out seeds are kept back until the code is final and then run once. This guards against a broker that only passes the schedules I happened to debug. It does not guard against fault types the suite does not generate (see the README's Limitations).

What actually happened, in order:

1. **First held-out set, seeds 100-119**, run once: 20/20 passed.
2. The load benchmark then showed the broker could not keep up with 10 subscribers at 1,000 msg/s on this machine. I changed the hot path (batch frames, no task switch per publish; see the CHANGELOG).
3. I re-ran seeds 100-119 on the changed code: **seed 103 failed**. One subscriber, restarted from its cursor file, received 20 messages twice. Cause: `Subscriber.close()` flushed the cursor before it stopped handing out messages, and the application kept consuming buffered messages after the flush. Batching made the buffer fill faster, which exposed it. I fixed it and added a regression test that fails on the old code.
4. Because seed 103 had now guided a fix, seeds 100-119 were no longer held out. I also raised the stall burst from 3,000 to 5,000 messages, because with batching the 3,000-message stall no longer filled the socket buffers and the `disconnect` policy was not being exercised. **Second held-out set, seeds 200-219**, run once on the final code: the "held-out" row in the results.
5. An independent review then found a bug the scenarios never hit (a cursor saved after a log reset paired the new epoch with an old seq). Fixing it changed client code, so the suite, including seeds 200-219, was re-run; they passed again. **From 1.0.0 on, seeds 200-219 run in CI on every change, so they are regression seeds, not held-out ones.** A future claim of held-out results needs new seeds.

The real-Redis row (`make bench-redis`, Redis 7 in Docker) uses development seeds 0-9 and is not run in CI because it needs a Redis server. It is the run that found the `first_seq` race described in the CHANGELOG: fakeredis executes each command in-process, so the interleaving that caused the bug never happened there.

### Property tests

- `tests/test_client_logic.py`: hypothesis drives the SDK's sequence tracker with a model of a lossy, duplicating connection whose sessions restart from the wrong place. Resync mode must deliver `1..N` exactly; skip mode must account for every seq.
- `tests/test_faults.py::test_property_streams_gap_free_and_duplicate_free_after_resume`: hypothesis picks a random schedule of cuts, crashes, graceful restarts and single-subscriber kills over 120 messages and 1-3 subscribers, on real sockets and a SQLite log; every subscriber must end with exactly `1..120`.

## Load benchmark

Code: `src/sigbroker/loadgen.py`.

- The broker runs as its own process (`sigbroker serve`), with `--max-queue 10000 --slow-grace 2`.
- Subscribers run in 4 worker processes (8 for the 100- and 500-subscriber rows), many asyncio subscribers per process.
- The publisher runs in the parent process, **open loop**: message `i` is sent at `t0 + i / rate` whether or not earlier acks came back, so a slow broker shows up as latency rather than as a lower offered rate. Each message carries `sent_ns = time.perf_counter_ns()` at send.
- Latency = the subscriber's `perf_counter_ns()` when it parsed the frame minus `sent_ns`. This clock is system-wide on Windows (QueryPerformanceCounter) and Linux (CLOCK_MONOTONIC), so stamps from different processes on one machine compare; across machines they mean nothing.
- Each row runs 1 s of warm-up (excluded) plus the measured duration. p50/p95/p99 are over every (message, subscriber) delivery in the measured window.
- Each subscriber process checks independently what the application saw: duplicates (`seq <= last`) and holes (`seq > last + 1`). `lost` counts sequences between a subscriber's first message and the last published seq that it never received.
- The broker's `/metrics` endpoint is scraped at the end of each row for slow-consumer disconnects.

What it does not measure: network latency between machines, TLS cost, more than one broker process, or a Redis on another host. All processes share one machine, so at high fan-out the subscriber processes compete with the broker for CPU; the numbers are a lower bound on what a broker on its own host would do at the same load, and an upper bound on nothing.

The Redis rows come from `make load-redis`: the full grid with a Redis using `appendfsync always`, then only the Redis rows against a second Redis using `appendfsync everysec`, appended to the same file. Both containers use `maxmemory-policy noeviction`.

### Catch-up after an outage

`run_catchup`: one subscriber is connected for 2 s at 1,000 msg/s, disconnects for 5 s while publishing continues, then reconnects with its stored cursor. Reported: messages missed, time from reconnect until it has every message that existed at reconnect time, and loss/duplicates over the whole run.

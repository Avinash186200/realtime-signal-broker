"""Subscriber SDK: auto-reconnect, back-off with jitter, resume, dedup and gap detection.

    async with Subscriber(url, token, ["signals.rates"]) as sub:
        async for d in sub:
            handle(d.topic, d.seq, d.signal)

Guarantees, as long as the broker still retains the messages:

- every message is handed to the application once, in sequence order, per topic, across
  disconnects, broker restarts and slow-consumer disconnects;
- a message the broker can no longer supply (trimmed by retention, a log reset, or a
  ``drop_oldest`` gap when ``gap_mode="skip"``) is never skipped silently: it is reported
  through ``on_gap`` and ``Subscriber.gaps``.

With a persistent ``cursor_store`` a message counts as handled when the application asks
for the next one (or calls ``commit()``), so after a crash of the application process the
last uncommitted message is delivered again (at-least-once to the application).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import random
import tempfile
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass, field
from typing import Any

from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake, InvalidURI

from .schema import (SIGNAL_ADAPTER, Batch, ProtocolError, Bye, Error, Event, Gap, Heartbeat, Hello, Subscribe, Subscribed,
                     Welcome, decode_server, encode)

log = logging.getLogger("sigbroker.client")

FATAL_CLOSE_CODES = {4001, 4003}
FATAL_ERRORS = {"unauthorized", "forbidden", "unsupported_version", "seq_ahead", "too_many_subscriptions",
                "too_many_topics"}


class SubscriberError(Exception):
    """A non-retryable failure (bad token, forbidden topic, unsupported protocol)."""


# ---------------------------------------------------------------------------
# Back-off
# ---------------------------------------------------------------------------

@dataclass
class Backoff:
    """Exponential back-off with full jitter: delay ~ U(0, min(cap, base * 2**attempt)).

    Full jitter spreads a crowd of reconnecting clients over the whole window, so a broker
    restart is not met by every subscriber at the same instant.
    """
    base_s: float = 0.05
    cap_s: float = 5.0
    rng: random.Random = field(default_factory=random.Random)

    def ceiling(self, attempt: int) -> float:
        return min(self.cap_s, self.base_s * (2 ** min(attempt, 30)))

    def delay(self, attempt: int) -> float:
        return self.rng.uniform(0.0, self.ceiling(attempt))


# ---------------------------------------------------------------------------
# Cursor stores
# ---------------------------------------------------------------------------

class MemoryCursorStore:
    def __init__(self):
        self._d: dict[str, tuple[str | None, int]] = {}

    def load(self, topic: str) -> tuple[str | None, int] | None:
        return self._d.get(topic)

    def save(self, topic: str, epoch: str | None, seq: int) -> None:
        self._d[topic] = (epoch, seq)


class FileCursorStore:
    """JSON file of {topic: [epoch, last_seq]}, replaced atomically.

    Writes at most every ``min_interval_s`` (an fsync-free replace still costs about a
    millisecond on some filesystems) and on ``flush()``, which ``Subscriber.close()`` calls.
    After a hard crash the file can be up to ``min_interval_s`` behind, so those messages
    are delivered again: at-least-once to the application."""

    def __init__(self, path: str, min_interval_s: float = 0.25):
        self.path = path
        self.min_interval_s = min_interval_s
        self._dirty = False
        self._last_write = float("-inf")  # the first save always writes
        try:
            with open(path, encoding="utf-8") as f:
                raw = json.load(f)
            self._d = {k: (v[0], int(v[1])) for k, v in raw.items()}
        except FileNotFoundError:
            self._d = {}
        except (ValueError, TypeError, KeyError, IndexError, AttributeError) as e:
            # Refuse to guess: starting from "earliest" or "latest" could replay or skip.
            raise ValueError(f"cursor file {path!r} is corrupt ({e}); fix or remove it deliberately") from None

    def load(self, topic: str):
        return self._d.get(topic)

    def save(self, topic: str, epoch: str | None, seq: int) -> None:
        self._d[topic] = (epoch, seq)
        self._dirty = True
        if time.monotonic() - self._last_write >= self.min_interval_s:
            self.flush()

    def flush(self) -> None:
        if not self._dirty:
            return
        d = os.path.dirname(os.path.abspath(self.path))
        fd, tmp = tempfile.mkstemp(dir=d, prefix=".cursor-")
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as f:
            json.dump(self._d, f)
            f.flush()
            os.fsync(f.fileno())  # the new contents are on disk before the rename publishes them
        os.replace(tmp, self.path)
        self._dirty = False
        self._last_write = time.monotonic()


# ---------------------------------------------------------------------------
# Sequence tracking (pure logic, unit- and property-tested)
# ---------------------------------------------------------------------------

DELIVER, DUPLICATE, STALE, GAP = "deliver", "duplicate", "stale", "gap"


@dataclass
class TopicCursor:
    epoch: str | None = None
    last_seq: int | None = None   # None: no position yet (live-only start)
    resyncing: bool = False


@dataclass
class GapRecord:
    topic: str
    from_seq: int
    to_seq: int
    reason: str  # retention | slow_consumer | detected | epoch_reset


class SeqTracker:
    """Decides what to do with each event so the app sees seq 1, 2, 3, ... per topic.

    ``gap_mode="resync"``: on a hole, ask the broker to replay from ``last_seq`` and drop
    out-of-order events until the replay arrives. ``gap_mode="skip"``: report the hole and
    move on (for latest-value consumers).
    """

    def __init__(self, gap_mode: str = "resync"):
        if gap_mode not in ("resync", "skip"):
            raise ValueError("gap_mode must be 'resync' or 'skip'")
        self.gap_mode = gap_mode
        self.cursors: dict[str, TopicCursor] = {}

    def cursor(self, topic: str) -> TopicCursor:
        c = self.cursors.get(topic)
        if c is None:
            c = self.cursors[topic] = TopicCursor()
        return c

    def on_subscribed(self, topic: str, epoch: str, resume_from: int) -> list[GapRecord]:
        c = self.cursor(topic)
        gaps: list[GapRecord] = []
        c.resyncing = False
        if c.epoch is not None and epoch != c.epoch:
            # The log was reset (e.g. in-memory broker restarted): our seqs mean nothing now.
            gaps.append(GapRecord(topic, (c.last_seq or 0) + 1, -1, "epoch_reset"))
            c.last_seq = resume_from - 1
        elif c.last_seq is None:
            c.last_seq = resume_from - 1
        c.epoch = epoch
        return gaps

    def on_event(self, topic: str, seq: int) -> tuple[str, GapRecord | None]:
        c = self.cursor(topic)
        last = c.last_seq
        if last is None:  # event before subscribed: cannot happen on one connection
            return STALE, None
        if seq <= last:
            return DUPLICATE, None
        if seq == last + 1:
            c.last_seq = seq
            return DELIVER, None
        if c.resyncing:
            return STALE, None
        gap = GapRecord(topic, last + 1, seq - 1, "detected")
        if self.gap_mode == "resync":
            c.resyncing = True
            return GAP, gap
        c.last_seq = seq
        return DELIVER, gap

    def on_gap_frame(self, topic: str, from_seq: int, to_seq: int, reason: str) -> tuple[bool, GapRecord | None]:
        """Returns (resync_needed, gap_to_report)."""
        c = self.cursor(topic)
        if c.last_seq is None or to_seq <= c.last_seq:
            return False, None
        if reason == "slow_consumer" and self.gap_mode == "resync":
            if c.resyncing:
                return False, None
            c.resyncing = True
            return True, None
        rec = GapRecord(topic, max(from_seq, c.last_seq + 1), to_seq, reason)
        c.last_seq = to_seq
        return False, rec


# ---------------------------------------------------------------------------
# The subscriber
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class Delivery:
    topic: str
    seq: int
    msg_id: str
    payload: dict[str, Any]
    published_ns: int
    sent_ns: int | None
    received_pc_ns: int  # time.perf_counter_ns() when the frame was parsed
    epoch: str | None = None  # the log epoch this seq belongs to (a seq means nothing without it)

    @property
    def signal(self):
        """The payload as a typed ScoreSignal / AlertSignal."""
        return SIGNAL_ADAPTER.validate_python(self.payload)


@dataclass
class SubscriberStats:
    connects: int = 0
    reconnects: int = 0
    delivered: int = 0
    duplicates_dropped: int = 0
    stale_dropped: int = 0
    gaps_detected: int = 0
    resyncs: int = 0
    gaps_reported: int = 0
    heartbeat_timeouts: int = 0
    epoch_resets: int = 0
    disconnect_codes: dict[int, int] = field(default_factory=dict)
    last_heads: dict[str, int] = field(default_factory=dict)


_END = object()


class Subscriber:
    def __init__(self, url: str, token: str, topics: Iterable[str], *, start: str = "earliest",
                 gap_mode: str = "resync", slow_policy: str | None = None,
                 cursor_store=None, backoff: Backoff | None = None, buffer: int = 1000,
                 heartbeat_misses: float = 3.0, open_timeout_s: float = 5.0,
                 on_gap: Callable[[GapRecord], None] | None = None, client_id: str | None = None,
                 ssl=None):
        """``start``: where a topic with no stored cursor begins, "earliest" (seq 1, or the
        oldest retained) or "latest" (only new messages)."""
        if start not in ("earliest", "latest"):
            raise ValueError("start must be 'earliest' or 'latest'")
        self.url = url
        self.token = token
        self.topics = list(dict.fromkeys(topics))
        self.start = start
        self.slow_policy = slow_policy
        self.cursor_store = cursor_store if cursor_store is not None else MemoryCursorStore()
        self.backoff = backoff or Backoff()
        self.heartbeat_misses = heartbeat_misses
        self.open_timeout_s = open_timeout_s
        self.on_gap = on_gap
        self.client_id = client_id
        self.ssl = ssl
        self.tracker = SeqTracker(gap_mode)
        self.stats = SubscriberStats()
        self.gaps: list[GapRecord] = []
        self._queue: asyncio.Queue = asyncio.Queue(maxsize=buffer)
        self._task: asyncio.Task | None = None
        self._closed = False
        self._error: BaseException | None = None
        self._ws = None
        self._uncommitted: Delivery | None = None
        self._healthy = False
        for t in self.topics:
            saved = self.cursor_store.load(t)
            if saved is not None:
                c = self.tracker.cursor(t)
                c.epoch, c.last_seq = saved[0], int(saved[1])

    # -- lifecycle ---------------------------------------------------------------------
    async def __aenter__(self) -> "Subscriber":
        self.start_background()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    def start_background(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="sigbroker-subscriber")

    async def close(self) -> None:
        """Stop: nothing is handed to the application after this starts, and the last
        message it did receive is committed and flushed to the cursor store."""
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        self.commit()
        flush = getattr(self.cursor_store, "flush", None)
        if flush is not None:
            flush()

    def kill_connection(self) -> None:
        """Test hook: drop the current connection abruptly (no close handshake)."""
        ws = self._ws
        if ws is not None:
            ws.transport.abort()

    # -- consumption ---------------------------------------------------------------------
    def __aiter__(self):
        return self

    async def __anext__(self) -> Delivery:
        self.commit()
        if self._closed:  # a concurrent close(): deliver nothing more, so the cursor stays exact
            raise StopAsyncIteration
        if self._task is None:
            self.start_background()
        item = await self._queue.get()
        if item is _END or self._closed:
            self._queue.put_nowait(_END)
            if self._error is not None:
                raise self._error
            raise StopAsyncIteration
        self._uncommitted = item
        return item

    def commit(self) -> None:
        d = self._uncommitted
        if d is not None:
            # The delivery's own epoch, not the tracker's current one: after a log reset the
            # application may still be consuming buffered old-epoch messages, and pairing an
            # old seq with the new epoch would make a restart skip new-epoch seqs 1..seq.
            self.cursor_store.save(d.topic, d.epoch, d.seq)
            self._uncommitted = None

    # -- connection loop -------------------------------------------------------------------
    def _subscribe_frame(self, topic: str) -> str:
        c = self.tracker.cursor(topic)
        if c.last_seq is None:
            last = 0 if self.start == "earliest" else None
        else:
            last = c.last_seq
        return encode(Subscribe(topic=topic, last_seq=last, epoch=c.epoch))

    def _report(self, rec: GapRecord) -> None:
        self.gaps.append(rec)
        self.stats.gaps_reported += 1
        if self.on_gap is not None:
            self.on_gap(rec)

    async def _run(self) -> None:
        attempt = 0
        try:
            while not self._closed:
                hint_ms = 0
                try:
                    async with connect(self.url, open_timeout=self.open_timeout_s, ssl=self.ssl,
                                       ping_interval=20, ping_timeout=20, close_timeout=2,
                                       max_size=1 << 20, compression=None) as ws:
                        self._ws = ws
                        hint_ms = await self._session(ws)
                        attempt = 0 if self._healthy else attempt
                except SubscriberError:
                    raise
                except ConnectionClosed as e:
                    code = e.rcvd.code if e.rcvd is not None else 1006
                    self.stats.disconnect_codes[code] = self.stats.disconnect_codes.get(code, 0) + 1
                    if code in FATAL_CLOSE_CODES:
                        raise SubscriberError(f"broker closed the connection: {code}") from None
                    attempt = 0 if self._healthy else attempt
                except (OSError, asyncio.TimeoutError, InvalidHandshake, EOFError, ProtocolError) as e:
                    log.debug("connect failed: %r", e)
                except InvalidURI as e:
                    raise SubscriberError(str(e)) from None
                finally:
                    self._ws = None
                if self._closed:
                    break
                delay = self.backoff.delay(attempt)
                if hint_ms:
                    delay = max(delay, hint_ms / 1000 * self.backoff.rng.uniform(0.5, 1.5))
                attempt += 1
                await asyncio.sleep(delay)
        except asyncio.CancelledError:
            pass
        except BaseException as e:  # surface to the iterator
            self._error = e
        finally:
            try:
                self._queue.put_nowait(_END)
            except asyncio.QueueFull:  # delivered once the application drains the buffer
                self._end_task = asyncio.get_running_loop().create_task(self._queue.put(_END))

    async def _session(self, ws) -> int:
        """One connection: hello, subscribe, read until it drops. Returns a reconnect hint."""
        self._healthy = False
        await ws.send(encode(Hello(token=self.token, client_id=self.client_id, slow_policy=self.slow_policy,
                                   batch=True)))
        async with asyncio.timeout(self.open_timeout_s):
            first = decode_server(await ws.recv())
        if isinstance(first, Error):
            raise SubscriberError(f"{first.code}: {first.message}")
        if not isinstance(first, Welcome):
            raise SubscriberError(f"unexpected first frame {first.type}")
        self.stats.connects += 1
        if self.stats.connects > 1:
            self.stats.reconnects += 1
        for t in self.topics:
            await ws.send(self._subscribe_frame(t))
        idle_timeout = first.heartbeat_s * self.heartbeat_misses
        while True:
            try:
                # asyncio.timeout, not wait_for: on 3.11 wait_for can swallow a cancel.
                async with asyncio.timeout(idle_timeout):
                    raw = await ws.recv()
            except asyncio.TimeoutError:
                self.stats.heartbeat_timeouts += 1
                ws.transport.abort()
                return 0
            pc = time.perf_counter_ns()
            frame = decode_server(raw)
            if isinstance(frame, Event):
                await self._on_event(ws, frame, pc)
            elif isinstance(frame, Batch):
                for ev in frame.events:
                    await self._on_event(ws, ev, pc)
            elif isinstance(frame, Subscribed):
                for rec in self.tracker.on_subscribed(frame.topic, frame.epoch, frame.resume_from):
                    self.stats.epoch_resets += 1
                    self._report(rec)
                self._healthy = True
            elif isinstance(frame, Gap):
                resync, rec = self.tracker.on_gap_frame(frame.topic, frame.from_seq, frame.to_seq, frame.reason)
                if resync:
                    self.stats.resyncs += 1
                    await ws.send(self._subscribe_frame(frame.topic))
                if rec is not None:
                    self._report(rec)
            elif isinstance(frame, Heartbeat):
                self.stats.last_heads.update(frame.heads)
            elif isinstance(frame, Bye):
                await ws.close()
                return frame.reconnect_after_ms
            elif isinstance(frame, Error):
                if frame.code in FATAL_ERRORS:
                    raise SubscriberError(f"{frame.code}: {frame.message}")
                log.warning("broker error %s: %s", frame.code, frame.message)

    async def _on_event(self, ws, ev: Event, pc: int) -> None:
        action, gap = self.tracker.on_event(ev.topic, ev.seq)
        if action == DUPLICATE:
            self.stats.duplicates_dropped += 1
            return
        if action == STALE:
            self.stats.stale_dropped += 1
            return
        if action == GAP:
            self.stats.gaps_detected += 1
            self.stats.resyncs += 1
            await ws.send(self._subscribe_frame(ev.topic))
            return
        if gap is not None:  # skip mode: hole reported, event delivered
            self.stats.gaps_detected += 1
            self._report(gap)
        self.stats.delivered += 1
        self._healthy = True
        await self._queue.put(Delivery(ev.topic, ev.seq, ev.msg_id, ev.payload, ev.published_ns, ev.sent_ns, pc,
                                       epoch=self.tracker.cursor(ev.topic).epoch))

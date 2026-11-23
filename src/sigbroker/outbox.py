"""Bounded per-subscriber send queue with a slow-consumer policy.

Only events count toward the bound; control frames (welcome, subscribed, ack, gap,
heartbeat, bye) are few and always queued.

Policies when the queue is full:

- ``disconnect``: the producer waits for space. If the queue has stayed full for
  ``slow_grace_s`` (full = at the bound; it counts as recovered only once it drains to
  half), the broker closes the connection with code 4008. The client reconnects and
  resumes from its last sequence, so nothing is lost; it just pays a replay.
- ``drop_oldest``: the oldest queued event is dropped at once and a ``gap`` notice takes
  its place (adjacent drops on one topic merge into one notice). The client learns exactly
  which sequences it missed and can re-fetch them from the log or skip them. Fits a
  dashboard that wants the latest value more than every value.
"""
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field

from .schema import Gap, encode


@dataclass
class Item:
    frame: str | None          # encoded JSON; None for a gap until it is rendered
    topic: str | None = None
    seq: int | None = None      # set for events
    gap_from: int | None = None  # set for gap items
    gap_to: int | None = None
    gap_reason: str | None = None
    enqueued_ns: int = 0
    appended_pc_ns: int | None = None  # perf_counter at append, for the latency histogram

    @property
    def is_event(self) -> bool:
        return self.seq is not None

    def render(self) -> str:
        if self.frame is None:
            self.frame = encode(Gap(topic=self.topic, from_seq=self.gap_from, to_seq=self.gap_to,
                                    reason=self.gap_reason))
        return self.frame


class SlowConsumer(Exception):
    pass


@dataclass
class Outbox:
    max_events: int
    policy: str = "disconnect"
    slow_grace_s: float = 1.0
    _q: deque = field(default_factory=deque)
    _events: int = 0
    _readable: asyncio.Event = field(default_factory=asyncio.Event)
    _space: asyncio.Event = field(default_factory=asyncio.Event)
    closed: bool = False
    _full_since: float | None = None

    def __post_init__(self):
        if self.policy not in ("disconnect", "drop_oldest"):
            raise ValueError(f"unknown slow-consumer policy {self.policy!r}")
        self._space.set()

    def __len__(self) -> int:
        return len(self._q)

    @property
    def events(self) -> int:
        return self._events

    def put_control(self, item: Item) -> None:
        self._q.append(item)
        self._readable.set()

    async def put_event(self, item: Item, paced: bool = False) -> list[tuple[str, int]]:
        """Queue an event; returns the (topic, seq) pairs dropped to make room.

        ``paced=True`` (catch-up replay): wait for space as long as it takes; the log holds
        the backlog, so there is nothing to protect by dropping or disconnecting.
        Otherwise the policy applies. Raises SlowConsumer under ``disconnect`` once the
        queue has been full for ``slow_grace_s`` without draining to half.
        """
        dropped: list[tuple[str, int]] = []
        if paced:
            while self._events >= self.max_events:
                if self.closed:
                    raise SlowConsumer()
                self._space.clear()
                await self._space.wait()
        elif self._events >= self.max_events:
            if self.policy == "disconnect":
                loop = asyncio.get_running_loop()
                if self._full_since is None:
                    self._full_since = loop.time()
                deadline = self._full_since + self.slow_grace_s
                while self._events >= self.max_events:
                    self._space.clear()
                    remaining = deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        raise SlowConsumer()
                    try:
                        async with asyncio.timeout(remaining):
                            await self._space.wait()
                    except asyncio.TimeoutError:
                        raise SlowConsumer() from None
                    if self.closed:
                        raise SlowConsumer()
            else:
                while self._events >= self.max_events:
                    dropped.append(self._drop_oldest_event())
        self._q.append(item)
        self._events += 1
        self._readable.set()
        return dropped

    def _drop_oldest_event(self) -> tuple[str, int]:
        q = self._q
        for i, it in enumerate(q):
            if it.is_event:
                break
        else:  # pragma: no cover - guarded by the event count
            raise RuntimeError("no event to drop")
        del q[i]
        self._events -= 1
        prev = q[i - 1] if i > 0 else None
        if (prev is not None and prev.gap_reason == "slow_consumer" and prev.topic == it.topic
                and prev.gap_to == it.seq - 1):
            prev.gap_to = it.seq
            prev.frame = None
        else:
            q.insert(i, Item(frame=None, topic=it.topic, gap_from=it.seq, gap_to=it.seq,
                             gap_reason="slow_consumer"))
        return it.topic, it.seq

    def purge_topic(self, topic: str) -> None:
        """Remove queued events and gap notices for a topic (on re-sync or unsubscribe)."""
        keep = deque()
        for it in self._q:
            if it.topic == topic and (it.is_event or it.gap_reason is not None):
                if it.is_event:
                    self._events -= 1
                continue
            keep.append(it)
        self._q = keep
        self._space.set()
        if self._events <= self.max_events // 2:
            self._full_since = None

    async def get(self) -> Item | None:
        """Next item to send; None once the outbox is closed and empty."""
        while not self._q:
            if self.closed:
                return None
            self._readable.clear()
            await self._readable.wait()
        it = self._q.popleft()
        if it.is_event:
            self._events -= 1
            self._space.set()
            if self._full_since is not None and self._events <= self.max_events // 2:
                self._full_since = None
        return it

    def take_events(self, max_n: int) -> list[Item]:
        """Pop up to ``max_n`` events that are already at the front of the queue, without
        waiting and without reordering past a control frame."""
        out: list[Item] = []
        q = self._q
        while q and len(out) < max_n and q[0].is_event:
            out.append(q.popleft())
        if out:
            self._events -= len(out)
            self._space.set()
            if self._full_since is not None and self._events <= self.max_events // 2:
                self._full_since = None
        return out

    def clear(self) -> None:
        """Discard everything queued (the connection is being killed)."""
        self._q.clear()
        self._events = 0
        self._space.set()

    def close(self) -> None:
        self.closed = True
        self._readable.set()
        self._space.set()

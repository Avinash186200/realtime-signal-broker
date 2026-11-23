"""In-memory log: for tests and CI. Not durable; a restart starts a new epoch."""
from __future__ import annotations

import uuid
from collections import OrderedDict, deque
from typing import Any

from .base import AppendResult, StoredEvent


class _Topic:
    __slots__ = ("events", "head", "idem")

    def __init__(self):
        self.events: deque[StoredEvent] = deque()
        self.head = 0
        self.idem: OrderedDict[str, int] = OrderedDict()


class MemoryBackend:
    def __init__(self, retention: int = 100_000, idempotency_window: int = 100_000):
        self.retention = retention
        self.idempotency_window = idempotency_window
        self.epoch = uuid.uuid4().hex
        self._topics: dict[str, _Topic] = {}

    def _t(self, topic: str) -> _Topic:
        t = self._topics.get(topic)
        if t is None:
            t = self._topics[topic] = _Topic()
        return t

    async def open(self) -> None:
        return None

    async def append(self, topic: str, msg_id: str, payload: dict[str, Any], published_ns: int,
                     sent_ns: int | None = None) -> AppendResult:
        t = self._t(topic)
        seen = t.idem.get(msg_id)
        if seen is not None:
            return AppendResult(seq=seen, created=False)
        t.head += 1
        ev = StoredEvent(topic, t.head, msg_id, published_ns, payload, sent_ns)
        t.events.append(ev)
        t.idem[msg_id] = t.head
        while len(t.events) > self.retention:
            t.events.popleft()
        while len(t.idem) > self.idempotency_window:
            t.idem.popitem(last=False)
        return AppendResult(seq=t.head, created=True, event=ev)

    async def read(self, topic: str, after: int, limit: int) -> list[StoredEvent]:
        t = self._topics.get(topic)
        if t is None or not t.events or after >= t.head:
            return []
        first = t.events[0].seq
        start = max(0, after + 1 - first)
        out = []
        for i in range(start, min(len(t.events), start + limit)):
            out.append(t.events[i])
        return out

    async def head(self, topic: str) -> int:
        t = self._topics.get(topic)
        return t.head if t else 0

    async def first_seq(self, topic: str) -> int:
        t = self._topics.get(topic)
        if t is None:
            return 1
        return t.events[0].seq if t.events else t.head + 1

    async def close(self) -> None:
        return None

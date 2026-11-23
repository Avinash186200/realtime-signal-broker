"""The replay log interface every backend implements."""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class StoredEvent:
    topic: str
    seq: int
    msg_id: str
    published_ns: int
    payload: dict[str, Any]
    sent_ns: int | None = None


@dataclass(frozen=True)
class AppendResult:
    seq: int
    created: bool  # False when msg_id was already in the log (idempotent retry)
    event: StoredEvent | None = None  # the stored event, when created


class LogBackend(Protocol):
    """An append-only log per topic with gap-free sequence numbers starting at 1.

    - ``append`` assigns ``seq = head + 1`` atomically. If ``msg_id`` was already appended
      to the topic (within the idempotency window), it returns the existing seq and
      ``created=False`` and stores nothing.
    - ``read(topic, after, limit)`` returns retained events with ``seq > after`` in order.
    - Retention may trim the oldest events; ``first_seq`` is the oldest retained seq
      (``head + 1`` when the topic is empty).
    - ``epoch`` identifies this log's lifetime. A log that lost its data (for example the
      in-memory backend after a restart) has a new epoch, so clients can tell a reset from
      a resume.
    """

    epoch: str

    async def open(self) -> None: ...
    async def append(self, topic: str, msg_id: str, payload: dict[str, Any], published_ns: int,
                     sent_ns: int | None = None) -> AppendResult: ...
    async def read(self, topic: str, after: int, limit: int) -> list[StoredEvent]: ...
    async def head(self, topic: str) -> int: ...
    async def first_seq(self, topic: str) -> int: ...
    async def close(self) -> None: ...

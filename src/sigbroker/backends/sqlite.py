"""SQLite log: durable across broker restarts, no server needed.

Calls run synchronously on the event loop. Each append is one short transaction
(WAL, synchronous=NORMAL), and the broker acks a publish only after it commits, so an
acked message survives a broker process crash (not necessarily a power cut).
"""
from __future__ import annotations

import json
import sqlite3
import uuid
from typing import Any

from .base import AppendResult, StoredEvent

_SCHEMA = """
CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events (
    topic TEXT NOT NULL,
    seq INTEGER NOT NULL,
    msg_id TEXT NOT NULL,
    published_ns INTEGER NOT NULL,
    sent_ns INTEGER,
    payload TEXT NOT NULL,
    PRIMARY KEY (topic, seq)
);
CREATE UNIQUE INDEX IF NOT EXISTS events_msg ON events (topic, msg_id);
CREATE TABLE IF NOT EXISTS heads (topic TEXT PRIMARY KEY, head INTEGER NOT NULL);
"""


class SQLiteBackend:
    def __init__(self, path: str, retention: int = 1_000_000, trim_every: int = 1000):
        self.path = path
        self.retention = retention
        self.trim_every = trim_every
        self.epoch = ""
        self._db: sqlite3.Connection | None = None
        self._heads: dict[str, int] = {}
        self._appends = 0

    async def open(self) -> None:
        db = sqlite3.connect(self.path, isolation_level=None, check_same_thread=False)
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
        db.executescript(_SCHEMA)
        row = db.execute("SELECT value FROM meta WHERE key='epoch'").fetchone()
        if row is None:
            self.epoch = uuid.uuid4().hex
            db.execute("INSERT INTO meta VALUES ('epoch', ?)", (self.epoch,))
        else:
            self.epoch = row[0]
        self._heads = dict(db.execute("SELECT topic, head FROM heads").fetchall())
        self._db = db

    @property
    def db(self) -> sqlite3.Connection:
        if self._db is None:
            raise RuntimeError("SQLiteBackend not open")
        return self._db

    async def append(self, topic: str, msg_id: str, payload: dict[str, Any], published_ns: int,
                     sent_ns: int | None = None) -> AppendResult:
        db = self.db
        row = db.execute("SELECT seq FROM events WHERE topic=? AND msg_id=?", (topic, msg_id)).fetchone()
        if row is not None:
            return AppendResult(seq=row[0], created=False)
        seq = self._heads.get(topic, 0) + 1
        db.execute("BEGIN")
        try:
            db.execute("INSERT INTO events VALUES (?,?,?,?,?,?)",
                       (topic, seq, msg_id, published_ns, sent_ns, json.dumps(payload, separators=(",", ":"))))
            db.execute("INSERT INTO heads VALUES (?, ?) ON CONFLICT(topic) DO UPDATE SET head=excluded.head",
                       (topic, seq))
            db.execute("COMMIT")
        except BaseException:
            db.execute("ROLLBACK")
            raise
        self._heads[topic] = seq
        self._appends += 1
        if self._appends % self.trim_every == 0:
            self._trim()
        return AppendResult(seq=seq, created=True,
                            event=StoredEvent(topic, seq, msg_id, published_ns, payload, sent_ns))

    def _trim(self) -> None:
        for topic, head in self._heads.items():
            if head > self.retention:
                self.db.execute("DELETE FROM events WHERE topic=? AND seq<=?", (topic, head - self.retention))

    async def read(self, topic: str, after: int, limit: int) -> list[StoredEvent]:
        rows = self.db.execute(
            "SELECT seq, msg_id, published_ns, sent_ns, payload FROM events "
            "WHERE topic=? AND seq>? ORDER BY seq LIMIT ?", (topic, after, limit)).fetchall()
        return [StoredEvent(topic, s, m, p, json.loads(d), sn) for s, m, p, sn, d in rows]

    async def head(self, topic: str) -> int:
        return self._heads.get(topic, 0)

    async def first_seq(self, topic: str) -> int:
        row = self.db.execute("SELECT MIN(seq) FROM events WHERE topic=?", (topic,)).fetchone()
        return row[0] if row and row[0] is not None else self._heads.get(topic, 0) + 1

    async def close(self) -> None:
        if self._db is not None:
            self._db.close()
            self._db = None

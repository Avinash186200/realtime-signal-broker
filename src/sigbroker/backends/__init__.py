"""Replay-log backends: memory (tests), SQLite (single node, durable), Redis Streams (production)."""
from __future__ import annotations

from .base import AppendResult, LogBackend, StoredEvent
from .memory import MemoryBackend
from .sqlite import SQLiteBackend


def make_backend(spec: str, retention: int = 1_000_000) -> LogBackend:
    """Build a backend from a spec: ``memory``, ``sqlite:PATH`` or ``redis://HOST:PORT/DB``."""
    if spec == "memory":
        return MemoryBackend(retention=retention)
    if spec.startswith("sqlite:"):
        return SQLiteBackend(spec[len("sqlite:"):], retention=retention)
    if spec.startswith(("redis://", "rediss://", "unix://")):
        from .redis import RedisBackend
        return RedisBackend(spec, retention=retention)
    raise ValueError(f"unknown backend spec {spec!r} (use memory, sqlite:PATH or redis://...)")


__all__ = ["AppendResult", "LogBackend", "StoredEvent", "MemoryBackend", "SQLiteBackend", "make_backend"]

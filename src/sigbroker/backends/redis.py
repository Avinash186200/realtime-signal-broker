"""Redis Streams log: the production backend.

Layout per topic (prefix ``sigbroker:`` by default). The ``{topic}`` part is a Redis
Cluster hash tag, so all keys of one topic land in one slot and the append script may
touch them together:

- ``{p}{topic}:stream``  a stream whose entry ids are ``{seq}-0``, so ``XRANGE`` from
  ``{after+1}-0`` returns exactly the messages after ``after``;
- ``{p}{topic}:seq``     the head counter;
- ``{p}{topic}:idem:{msg_id}``  idempotency key, expires after ``idempotency_ttl_s``;
- ``{p}epoch``           the log's epoch id.

Append is one Lua script, so the idempotency check, the counter and ``XADD`` are atomic
even with several broker processes writing to the same Redis. The script writes nothing
before ``XADD`` succeeds (it reads the counter, adds the entry, then stores the counter),
so a failed ``XADD`` cannot leave a hole in the sequence.

Assumptions: a single Redis primary (the hash tags make topics cluster-ready, but that is
untested; and an asynchronous replica promoted after a failover can lose acked writes), with
``maxmemory-policy noeviction`` so Redis refuses writes when full instead of evicting
stream entries or counters, and AOF persistence (``appendfsync always`` for no loss of
acked appends on a crash; ``everysec`` risks about a second). See docs/protocol.md.
"""
from __future__ import annotations

import json
import uuid
from typing import Any

from .base import AppendResult, StoredEvent

_APPEND_LUA = """
local seen = redis.call('GET', KEYS[3])
if seen then return {tonumber(seen), 0} end
local seq = (tonumber(redis.call('GET', KEYS[2])) or 0) + 1
redis.call('XADD', KEYS[1], 'MAXLEN', '~', ARGV[5], seq .. '-0',
           'm', ARGV[1], 'p', ARGV[2], 's', ARGV[3], 'd', ARGV[4])
redis.call('SET', KEYS[2], seq)
redis.call('SET', KEYS[3], seq, 'EX', ARGV[6])
return {seq, 1}
"""


class RedisBackend:
    def __init__(self, url: str, prefix: str = "sigbroker:", retention: int = 1_000_000,
                 idempotency_ttl_s: int = 86_400, client=None):
        self.url = url
        self.prefix = prefix
        self.retention = retention
        self.idempotency_ttl_s = idempotency_ttl_s
        self.epoch = ""
        self._r = client
        self._script = None

    async def open(self) -> None:
        if self._r is None:
            import redis.asyncio as aioredis  # optional dependency
            self._r = aioredis.from_url(self.url, decode_responses=True)
        await self._r.set(self.prefix + "epoch", uuid.uuid4().hex, nx=True)
        self.epoch = await self._r.get(self.prefix + "epoch")
        self._script = self._r.register_script(_APPEND_LUA)

    def _k(self, topic: str, kind: str) -> str:
        return f"{self.prefix}{{{topic}}}:{kind}"

    def _keys(self, topic: str, msg_id: str) -> list[str]:
        return [self._k(topic, "stream"), self._k(topic, "seq"), self._k(topic, f"idem:{msg_id}")]

    async def append(self, topic: str, msg_id: str, payload: dict[str, Any], published_ns: int,
                     sent_ns: int | None = None) -> AppendResult:
        data = json.dumps(payload, separators=(",", ":"))
        seq, created = await self._script(
            keys=self._keys(topic, msg_id),
            args=[msg_id, published_ns, "" if sent_ns is None else sent_ns, data, self.retention,
                  self.idempotency_ttl_s])
        seq = int(seq)
        if not created:
            return AppendResult(seq=seq, created=False)
        return AppendResult(seq=seq, created=True,
                            event=StoredEvent(topic, seq, msg_id, published_ns, payload, sent_ns))

    async def read(self, topic: str, after: int, limit: int) -> list[StoredEvent]:
        rows = await self._r.xrange(self._k(topic, "stream"), min=f"{after + 1}-0", max="+",
                                    count=limit)
        out = []
        for entry_id, f in rows:
            sent = f.get("s") or None
            out.append(StoredEvent(topic, int(entry_id.split("-")[0]), f["m"], int(f["p"]),
                                   json.loads(f["d"]), int(sent) if sent else None))
        return out

    async def head(self, topic: str) -> int:
        v = await self._r.get(self._k(topic, "seq"))
        return int(v) if v else 0

    async def first_seq(self, topic: str) -> int:
        # One MULTI/EXEC: reading the stream and the counter separately races with an
        # append (empty stream, then head already 1 -> "first is 2", and seq 1 is skipped).
        async with self._r.pipeline(transaction=True) as pipe:
            pipe.xrange(self._k(topic, "stream"), min="-", max="+", count=1)
            pipe.get(self._k(topic, "seq"))
            rows, head = await pipe.execute()
        if rows:
            return int(rows[0][0].split("-")[0])
        return (int(head) if head else 0) + 1

    async def close(self) -> None:
        if self._r is not None:
            await self._r.aclose()
            self._r = None

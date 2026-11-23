"""One contract, every backend: memory, SQLite, Redis Streams (fakeredis; a real Redis if
SIGBROKER_TEST_REDIS_URL is set)."""
import asyncio
import uuid

import pytest

from conftest import REDIS_URL, run
from sigbroker.backends import MemoryBackend, SQLiteBackend, make_backend
from sigbroker.backends.redis import RedisBackend

KINDS = ["memory", "sqlite", "fakeredis"] + (["redis"] if REDIS_URL else [])


def make(kind, tmp_path, retention=1000, server=None):
    if kind == "memory":
        return MemoryBackend(retention=retention)
    if kind == "sqlite":
        return SQLiteBackend(str(tmp_path / "log.sqlite"), retention=retention, trim_every=1)
    if kind == "fakeredis":
        import fakeredis
        client = fakeredis.FakeAsyncRedis(server=server or fakeredis.FakeServer(), decode_responses=True)
        return RedisBackend("redis://fake", retention=retention, client=client)
    return RedisBackend(REDIS_URL, prefix=f"test-{uuid.uuid4().hex[:8]}:", retention=retention)


@pytest.mark.parametrize("kind", KINDS)
def test_append_read_and_idempotency(kind, tmp_path):
    async def go():
        b = make(kind, tmp_path)
        await b.open()
        assert await b.head("t") == 0 and await b.first_seq("t") == 1 and await b.read("t", 0, 10) == []
        for i in range(5):
            r = await b.append("t", f"m{i}", {"i": i}, 100 + i, sent_ns=7)
            assert r.created and r.seq == i + 1
        dup = await b.append("t", "m2", {"i": "changed"}, 999)
        assert not dup.created and dup.seq == 3
        assert await b.head("t") == 5 and await b.head("other") == 0
        evs = await b.read("t", 2, 2)
        assert [(e.seq, e.msg_id, e.payload["i"], e.sent_ns) for e in evs] == [(3, "m2", 2, 7), (4, "m3", 3, 7)]
        assert await b.read("t", 5, 10) == []
        await b.close()
    run(go())


@pytest.mark.parametrize("kind", KINDS)
def test_concurrent_appends_are_gap_free(kind, tmp_path):
    async def go():
        b = make(kind, tmp_path)
        await b.open()
        res = await asyncio.gather(*[b.append("t", f"m{i}", {}, i) for i in range(200)])
        assert sorted(r.seq for r in res) == list(range(1, 201))
        evs = await b.read("t", 0, 500)
        assert [e.seq for e in evs] == list(range(1, 201))
        assert len({e.msg_id for e in evs}) == 200
        await b.close()
    run(go())


@pytest.mark.parametrize("kind", ["memory", "sqlite"])
def test_retention_moves_first_seq(kind, tmp_path):
    async def go():
        b = make(kind, tmp_path, retention=10)
        await b.open()
        for i in range(25):
            await b.append("t", f"m{i}", {}, i)
        assert await b.first_seq("t") == 16 and await b.head("t") == 25
        assert [e.seq for e in await b.read("t", 0, 100)] == list(range(16, 26))
        await b.close()
    run(go())


def test_redis_retention_is_approximate_but_reads_stay_contiguous(tmp_path):
    async def go():
        b = make("fakeredis", tmp_path, retention=10)
        await b.open()
        for i in range(300):
            await b.append("t", f"m{i}", {}, i)
        first = await b.first_seq("t")
        seqs = [e.seq for e in await b.read("t", 0, 1000)]
        assert 1 <= first <= 291 and seqs == list(range(first, 301))
        await b.close()
    run(go())


def test_sqlite_survives_reopen_with_same_epoch(tmp_path):
    async def go():
        b = make("sqlite", tmp_path)
        await b.open()
        for i in range(3):
            await b.append("t", f"m{i}", {"i": i}, i)
        epoch = b.epoch
        await b.close()
        b2 = make("sqlite", tmp_path)
        await b2.open()
        assert b2.epoch == epoch and await b2.head("t") == 3
        assert (await b2.append("t", "m1", {}, 0)).seq == 2  # idempotency survives a restart
        assert (await b2.append("t", "new", {}, 0)).seq == 4
        await b2.close()
    run(go())


def test_redis_survives_reconnect_with_same_epoch(tmp_path):
    import fakeredis
    server = fakeredis.FakeServer()

    async def go():
        b = make("fakeredis", tmp_path, server=server)
        await b.open()
        await b.append("t", "a", {}, 1)
        epoch = b.epoch
        await b.close()
        b2 = make("fakeredis", tmp_path, server=server)
        await b2.open()
        assert b2.epoch == epoch and await b2.head("t") == 1
        assert (await b2.append("t", "a", {}, 1)).created is False
        await b2.close()
    run(go())


def test_memory_restart_is_a_new_epoch():
    assert MemoryBackend().epoch != MemoryBackend().epoch


def test_make_backend_specs(tmp_path):
    assert isinstance(make_backend("memory"), MemoryBackend)
    assert isinstance(make_backend(f"sqlite:{tmp_path / 'x.sqlite'}"), SQLiteBackend)
    assert isinstance(make_backend("redis://localhost:6379/0"), RedisBackend)
    with pytest.raises(ValueError):
        make_backend("kafka://nope")

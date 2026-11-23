"""Fault injection over real sockets: dropped links, broker crashes and restarts, slow
consumers, lost publish acks. Every check is the same: each subscriber's stream is
exactly 1..N (or, in skip mode, every seq is delivered once or reported in a gap)."""
import asyncio
import random
from datetime import datetime, timezone

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from conftest import run
from sigbroker import Broker, BrokerConfig, Publisher, Subscriber
from sigbroker.backends import SQLiteBackend
from sigbroker.chaos import ChaosProxy
from sigbroker.client import Backoff
from sigbroker.metrics import counter_value
from sigbroker.scenarios import ScenarioConfig, run_scenario
from sigbroker.schema import ScoreSignal

T = datetime(2026, 1, 5, tzinfo=timezone.utc)
TOPIC = "signals.rates"


def sig(i):
    return ScoreSignal(model="m", instrument="X", value=float(i), as_of=T).model_dump(mode="json")


def fast(seed):
    return Backoff(0.01, 0.2, random.Random(seed))


async def collect(sub, out, until, gate=None):
    async for d in sub:
        out.append(d.seq)
        if gate is not None:
            await gate.wait()
        if d.seq >= until:
            return


async def wait_for(cond, timeout=20):
    async with asyncio.timeout(timeout):
        while not cond():
            await asyncio.sleep(0.02)


def test_connections_killed_mid_stream(secret, tokens):
    async def go():
        async with Broker(secret) as b:
            proxy = await ChaosProxy(b.port).start()
            got = [[], []]
            subs = [Subscriber(proxy.url, tokens(), [TOPIC], backoff=fast(i)) for i in range(2)]
            tasks = [asyncio.create_task(collect(s, got[i], 300)) for i, s in enumerate(subs)]
            async with Publisher(proxy.url, tokens("publisher"), backoff=fast(9)) as pub:
                for i in range(300):
                    if i % 60 == 30:
                        assert proxy.cut_all() >= 1
                    await pub.publish(TOPIC, sig(i), msg_id=f"m{i}")
            await asyncio.gather(*tasks)
            for s in subs:
                await s.close()
            await proxy.stop()
            assert proxy.cuts >= 5
            assert got[0] == got[1] == list(range(1, 301))
            assert all(s.stats.reconnects >= 5 for s in subs)
    run(go())


def test_individual_subscriber_link_drops(secret, tokens):
    async def go():
        async with Broker(secret) as b:
            async with Publisher(b.url, tokens("publisher")) as pub:
                got = []
                sub = Subscriber(b.url, tokens(), [TOPIC], backoff=fast(1))
                task = asyncio.create_task(collect(sub, got, 200))
                for i in range(200):
                    await pub.publish(TOPIC, sig(i))
                    if i % 50 == 25:
                        sub.kill_connection()
                await task
                await sub.close()
            assert got == list(range(1, 201))
    run(go())


def test_broker_crash_and_restart_on_a_durable_log(secret, tokens, tmp_path):
    async def go():
        db = str(tmp_path / "log.sqlite")
        b = await Broker(secret, SQLiteBackend(db)).start()
        proxy = await ChaosProxy(b.port).start()
        got = []
        sub = Subscriber(proxy.url, tokens(), [TOPIC], backoff=fast(1))
        task = asyncio.create_task(collect(sub, got, 300))
        pub = Publisher(proxy.url, tokens("publisher"), backoff=fast(2))
        await pub.start()
        epoch = b.backend.epoch
        for i in range(300):
            if i in (100, 200):
                if i == 100:
                    await b.abort()                     # crash: no close handshake
                else:
                    await b.stop(drain_timeout_s=1)     # graceful restart
                proxy.cut_all()
                b = await Broker(secret, SQLiteBackend(db)).start()
                proxy.upstream_port = b.port
                assert b.backend.epoch == epoch
            await pub.publish(TOPIC, sig(i), msg_id=f"m{i}")
        await task
        await sub.close()
        await pub.close()
        stored = await b.backend.read(TOPIC, 0, 1000)
        await b.stop()
        await proxy.stop()
        assert got == list(range(1, 301))
        assert [e.msg_id for e in stored] == [f"m{i}" for i in range(300)]
        assert sub.stats.epoch_resets == 0
    run(go())


def test_slow_consumer_disconnect_policy_resumes_without_loss(secret, tokens):
    async def go():
        cfg = BrokerConfig(max_queue=32, slow_grace_s=0.1, slow_policy="disconnect", socket_send_buffer=16384)
        async with Broker(secret, config=cfg) as b:
            gate = asyncio.Event()
            got = []
            sub = Subscriber(b.url, tokens(), [TOPIC], buffer=8, backoff=fast(1))
            task = asyncio.create_task(collect(sub, got, 10 ** 9, gate))
            slow = lambda: counter_value(b.metrics.registry, "sigbroker_slow_disconnects_total")  # noqa: E731
            n = 0
            async with Publisher(b.url, tokens("publisher")) as pub:
                await wait_for(lambda: sub.stats.connects >= 1)
                # The consumer is stalled; keep publishing until the policy fires. How much
                # that takes depends on the OS socket buffers (more on Linux than Windows).
                while slow() < 1 and n < 200_000:
                    await asyncio.gather(*[pub.publish(TOPIC, sig(i)) for i in range(n, n + 200)])
                    n += 200
                assert slow() >= 1
                gate.set()
                await wait_for(lambda: len(got) >= n, timeout=60)
            task.cancel()
            await sub.close()
            assert got == list(range(1, n + 1))
            assert sub.stats.disconnect_codes.get(4008, 0) >= 1
    run(go(), timeout=120)


def test_slow_consumer_drop_oldest_skip_reports_every_gap(secret, tokens):
    async def go():
        async with Broker(secret, config=BrokerConfig(max_queue=32)) as b:
            gate = asyncio.Event()
            got = []
            sub = Subscriber(b.url, tokens(), [TOPIC], buffer=8, backoff=fast(1),
                             slow_policy="drop_oldest", gap_mode="skip")
            task = asyncio.create_task(collect(sub, got, 6000, gate))
            async with Publisher(b.url, tokens("publisher")) as pub:
                await wait_for(lambda: sub.stats.connects >= 1)
                for k in range(0, 6000, 200):
                    await asyncio.gather(*[pub.publish(TOPIC, sig(i)) for i in range(k, k + 200)])
                await wait_for(lambda: counter_value(b.metrics.registry, "sigbroker_dropped_total") > 0)
                gate.set()
                await wait_for(lambda: sub.tracker.cursor(TOPIC).last_seq == 6000)
                task.cancel()
            await sub.close()
            gapped = [s for g in sub.gaps for s in range(g.from_seq, g.to_seq + 1)]
            assert gapped, "the stall should have produced drops"
            assert got == sorted(set(got)) and not set(got) & set(gapped)
            assert sorted(got + gapped) == list(range(1, 6001))
            assert {g.reason for g in sub.gaps} == {"slow_consumer"}
    run(go())


def test_slow_consumer_drop_oldest_resync_recovers_from_the_log(secret, tokens):
    async def go():
        async with Broker(secret, config=BrokerConfig(max_queue=32)) as b:
            gate = asyncio.Event()
            got = []
            sub = Subscriber(b.url, tokens(), [TOPIC], buffer=8, backoff=fast(1),
                             slow_policy="drop_oldest", gap_mode="resync")
            task = asyncio.create_task(collect(sub, got, 6000, gate))
            async with Publisher(b.url, tokens("publisher")) as pub:
                await wait_for(lambda: sub.stats.connects >= 1)
                for k in range(0, 6000, 200):
                    await asyncio.gather(*[pub.publish(TOPIC, sig(i)) for i in range(k, k + 200)])
                await wait_for(lambda: counter_value(b.metrics.registry, "sigbroker_dropped_total") > 0)
                gate.set()
                await task
            await sub.close()
            assert got == list(range(1, 6001)) and sub.stats.resyncs >= 1 and sub.gaps == []
    run(go())


def test_lost_ack_retry_does_not_duplicate(secret, tokens):
    """The connection dies after the broker appended but before the ack arrived: the
    publisher re-sends with the same msg_id and gets the original seq back."""
    async def go():
        async with Broker(secret) as b:
            proxy = await ChaosProxy(b.port).start()
            async with Publisher(proxy.url, tokens("publisher"), backoff=fast(1)) as pub:
                original = b.publish
                cut = []

                async def append_then_cut(*a, **kw):
                    res = await original(*a, **kw)
                    if a[1] == "m-cut" and not cut:
                        cut.append(1)
                        proxy.cut_all()  # appended, but the ack is never delivered
                    return res
                b.publish = append_then_cut
                ack = await pub.publish(TOPIC, sig(1), msg_id="m-cut", timeout_s=10)
                b.publish = original
                assert ack.duplicate and ack.seq == 1 and pub.resent >= 1
                ack2 = await pub.publish(TOPIC, sig(2), msg_id="m-next")
                assert ack2.seq == 2
            await proxy.stop()
            assert [e.msg_id for e in await b.backend.read(TOPIC, 0, 10)] == ["m-cut", "m-next"]
    run(go())


def test_cursor_file_resume_after_application_restart(secret, tokens, tmp_path):
    async def go():
        from sigbroker.client import FileCursorStore
        path = str(tmp_path / "cursor.json")
        async with Broker(secret) as b:
            async with Publisher(b.url, tokens("publisher")) as pub:
                for i in range(50):
                    await pub.publish(TOPIC, sig(i))
            first = []
            sub = Subscriber(b.url, tokens(), [TOPIC], cursor_store=FileCursorStore(path))
            async for d in sub:
                first.append(d.seq)
                if d.seq == 20:
                    break
            await sub.close()       # commits seq 20
            second = []
            sub = Subscriber(b.url, tokens(), [TOPIC], cursor_store=FileCursorStore(path))
            async for d in sub:
                second.append(d.seq)
                if d.seq == 50:
                    break
            await sub.close()
            assert first == list(range(1, 21)) and second == list(range(21, 51))
    run(go())


def test_one_full_scenario_passes(tmp_path):
    r = run(run_scenario(3, tmp_path, ScenarioConfig(n_messages=300, stall_burst=1500, n_faults=4)), timeout=120)
    assert r["passed"], r


# ---------------------------------------------------------------------------
# property: random fault schedules
# ---------------------------------------------------------------------------

faults = st.lists(st.tuples(st.integers(1, 119), st.sampled_from(["cut", "crash", "restart", "kill_sub"])),
                  min_size=1, max_size=5)


@settings(max_examples=12, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
@given(schedule=faults, n_subs=st.integers(1, 3), seed=st.integers(0, 10_000))
def test_property_streams_gap_free_and_duplicate_free_after_resume(tmp_path_factory, schedule, n_subs, seed):
    from sigbroker.auth import mint_token, new_secret
    secret = new_secret()
    db = str(tmp_path_factory.mktemp("prop") / "log.sqlite")
    n = 120
    sched = {}
    for i, f in schedule:
        sched.setdefault(i, f)

    async def go():
        b = await Broker(secret, SQLiteBackend(db), BrokerConfig(drain_timeout_s=1)).start()
        proxy = await ChaosProxy(b.port).start()
        st_tok = mint_token(secret, "s", "subscriber", ["signals.*"])
        subs = [Subscriber(proxy.url, st_tok, [TOPIC], backoff=fast(seed + i)) for i in range(n_subs)]
        got = [[] for _ in subs]
        tasks = [asyncio.create_task(collect(s, got[i], n)) for i, s in enumerate(subs)]
        pub = Publisher(proxy.url, mint_token(secret, "p", "publisher", ["signals.*"]), backoff=fast(seed))
        await pub.start()
        for i in range(n):
            f = sched.get(i)
            if f == "cut":
                proxy.cut_all()
            elif f in ("crash", "restart"):
                await (b.abort() if f == "crash" else b.stop(drain_timeout_s=1))
                b = await Broker(secret, SQLiteBackend(db), BrokerConfig(drain_timeout_s=1)).start()
                proxy.upstream_port = b.port
            elif f == "kill_sub":
                subs[i % n_subs].kill_connection()
            await pub.publish(TOPIC, sig(i), msg_id=f"p{i}")
        await asyncio.gather(*tasks)
        for s in subs:
            await s.close()
        await pub.close()
        await b.stop()
        await proxy.stop()
        return got

    got = run(go(), timeout=60)
    for g in got:
        assert g == list(range(1, n + 1))


def test_close_while_the_app_is_consuming_keeps_the_cursor_exact(secret, tokens, tmp_path):
    """Regression (found by a held-out scenario seed): close() raced with a consumer that
    kept pulling buffered messages after the cursor was flushed, so a restarted app got
    those messages again.

    The consumer pauses at seq 500, close() starts, then the consumer resumes pulling while
    close() runs. More messages are published after the close, so the restarted subscriber
    always has a known last seq to wait for, however far the first consumer got."""
    async def go():
        from sigbroker.client import FileCursorStore
        path = str(tmp_path / "cursor.json")
        async with Broker(secret) as b:
            async with Publisher(b.url, tokens("publisher")) as pub:
                for k in range(0, 3000, 500):
                    await asyncio.gather(*[pub.publish(TOPIC, sig(i)) for i in range(k, k + 500)])
            got = []
            at_500, resume = asyncio.Event(), asyncio.Event()
            sub = Subscriber(b.url, tokens(), [TOPIC], cursor_store=FileCursorStore(path, min_interval_s=3600))

            async def consume():
                async for d in sub:
                    got.append(d.seq)
                    if d.seq == 500:
                        at_500.set()
                        await resume.wait()
                    await asyncio.sleep(0)
            task = asyncio.create_task(consume())
            await wait_for(at_500.is_set)
            closing = asyncio.create_task(sub.close())   # close() starts ...
            resume.set()                                 # ... while the consumer keeps pulling
            await closing
            await wait_for(task.done)                    # it must stop by itself, not be cancelled
            assert 500 <= len(got) < 3000
            saved = FileCursorStore(path).load(TOPIC)[1]
            assert saved == got[-1] and got == list(range(1, len(got) + 1))
            async with Publisher(b.url, tokens("publisher")) as pub:
                for i in range(3000, 3100):
                    await pub.publish(TOPIC, sig(i))
            again = []
            sub = Subscriber(b.url, tokens(), [TOPIC], cursor_store=FileCursorStore(path))
            async for d in sub:
                again.append(d.seq)
                if d.seq == 3100:
                    break
            await sub.close()
            assert again == list(range(got[-1] + 1, 3101))
    run(go())


def test_cursor_saved_after_a_log_reset_keeps_the_delivery_epoch(secret, tokens, tmp_path):
    """Regression (found in review): after an epoch change the SDK saved (new epoch, old seq)
    for a buffered old-epoch message, so a restarted app skipped new-epoch seqs 1..old_seq
    without any gap report."""
    async def go():
        from sigbroker.client import FileCursorStore
        path = str(tmp_path / "cursor.json")
        b = await Broker(secret, config=BrokerConfig(port=0)).start()
        port = b.port
        async with Publisher(b.url, tokens("publisher")) as pub:
            for i in range(50):
                await pub.publish(TOPIC, sig(i))
        sub = Subscriber(b.url, tokens(), [TOPIC], cursor_store=FileCursorStore(path, min_interval_s=0),
                         backoff=fast(1))
        it = sub.__aiter__()
        got = [(await it.__anext__()).seq for _ in range(5)]
        await wait_for(lambda: sub._queue.qsize() >= 40)   # 6..50 buffered, old epoch
        old_epoch = b.backend.epoch
        await b.abort()
        b = await Broker(secret, config=BrokerConfig(port=port)).start()  # memory log: new epoch
        await wait_for(lambda: sub.tracker.cursor(TOPIC).epoch == b.backend.epoch)
        d = await it.__anext__()                           # still an old-epoch message
        got.append(d.seq)
        assert d.epoch == old_epoch and got == [1, 2, 3, 4, 5, 6]
        await sub.close()
        assert FileCursorStore(path).load(TOPIC) == (old_epoch, 6)
        async with Publisher(b.url, tokens("publisher")) as pub:
            for i in range(20):
                await pub.publish(TOPIC, sig(100 + i))
        gaps = []
        sub2 = Subscriber(b.url, tokens(), [TOPIC], cursor_store=FileCursorStore(path), on_gap=gaps.append)
        seen = []
        async for d in sub2:
            seen.append((d.seq, d.epoch))
            if d.seq == 20:
                break
        await sub2.close()
        await b.stop()
        assert [s for s, _ in seen] == list(range(1, 21))
        assert {e for _, e in seen} == {b.backend.epoch}
        assert [g.reason for g in gaps] == ["epoch_reset"]
    run(go())


async def _inflight_case(secret, tokens, tmp_path, how):
    db = str(tmp_path / f"log-{how}.sqlite")
    b = await Broker(secret, SQLiteBackend(db), BrokerConfig(drain_timeout_s=2)).start()
    proxy = await ChaosProxy(b.port).start()
    subs = [Subscriber(proxy.url, tokens(), [TOPIC], backoff=fast(i)) for i in range(3)]
    got = [[] for _ in subs]
    tasks = [asyncio.create_task(collect(s, got[i], 600)) for i, s in enumerate(subs)]
    pub = Publisher(proxy.url, tokens("publisher"), backoff=fast(7))
    await pub.start()
    await asyncio.gather(*[pub.publish(TOPIC, sig(i), msg_id=f"a{i}") for i in range(200)])
    # 200 publishes in flight when the broker goes away
    inflight = [asyncio.ensure_future(pub.publish(TOPIC, sig(i), msg_id=f"b{i}", timeout_s=30))
                for i in range(200)]
    await asyncio.sleep(0)
    assert sum(f.done() for f in inflight) < 200  # really in flight when the broker goes
    if how == "crash":
        await b.abort()
    else:
        await b.stop(drain_timeout_s=2)
    proxy.cut_all()
    b = await Broker(secret, SQLiteBackend(db), BrokerConfig(drain_timeout_s=2)).start()
    proxy.upstream_port = b.port
    acks = await asyncio.gather(*inflight)
    assert pub.connects >= 2
    await asyncio.gather(*[pub.publish(TOPIC, sig(i), msg_id=f"c{i}") for i in range(200)])
    await asyncio.gather(*tasks)
    for s in subs:
        await s.close()
    await pub.close()
    stored = await b.backend.read(TOPIC, 0, 10_000)
    await b.stop()
    await proxy.stop()
    ids = [e.msg_id for e in stored]
    assert len(ids) == len(set(ids)) == 600
    assert set(ids) == {f"{p}{i}" for p in "abc" for i in range(200)}
    assert sorted(a.seq for a in acks) == sorted({a.seq for a in acks})  # 200 distinct seqs
    for g in got:
        assert g == list(range(1, 601))


def test_broker_crash_with_200_publishes_in_flight(secret, tokens, tmp_path):
    run(_inflight_case(secret, tokens, tmp_path, "crash"), timeout=90)


def test_graceful_stop_with_200_publishes_in_flight(secret, tokens, tmp_path):
    run(_inflight_case(secret, tokens, tmp_path, "stop"), timeout=90)

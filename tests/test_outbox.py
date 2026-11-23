import asyncio

import pytest

from conftest import run
from sigbroker.outbox import Item, Outbox, SlowConsumer


def ev(seq, topic="t"):
    return Item(frame=f"e{seq}", topic=topic, seq=seq)


def test_control_frames_do_not_count_against_the_bound():
    async def go():
        o = Outbox(max_events=2)
        for i in range(10):
            o.put_control(Item(frame=f"c{i}"))
        await o.put_event(ev(1))
        await o.put_event(ev(2))
        assert o.events == 2 and len(o) == 12
    run(go())


def test_disconnect_policy_raises_after_grace():
    async def go():
        o = Outbox(max_events=2, policy="disconnect", slow_grace_s=0.05)
        await o.put_event(ev(1))
        await o.put_event(ev(2))
        p = asyncio.create_task(o.put_event(ev(3)))
        for _ in range(3):           # it waits rather than raising at once ...
            await asyncio.sleep(0)
        assert not p.done()
        with pytest.raises(SlowConsumer):  # ... and gives up once the grace has passed
            await p
    run(go())


def test_disconnect_policy_waits_while_the_consumer_keeps_up():
    async def go():
        o = Outbox(max_events=2, policy="disconnect", slow_grace_s=0.5)
        await o.put_event(ev(1))
        await o.put_event(ev(2))

        async def reader():
            await asyncio.sleep(0.02)
            return [(await o.get()).frame for _ in range(3)]
        r = asyncio.create_task(reader())
        await o.put_event(ev(3))  # waits for the reader, does not raise
        assert await r == ["e1", "e2", "e3"]
    run(go())


def test_disconnect_policy_counts_time_full_not_time_per_message():
    """A consumer that frees one slot at a time but never drains is still disconnected."""
    async def go():
        o = Outbox(max_events=4, policy="disconnect", slow_grace_s=0.1)
        for i in range(4):
            await o.put_event(ev(i))

        async def trickle():
            while True:
                await asyncio.sleep(0.01)
                await o.get()
        t = asyncio.create_task(trickle())
        i = 4
        with pytest.raises(SlowConsumer):
            while True:  # bounded by run()'s timeout
                await o.put_event(ev(i))
                i += 1
        t.cancel()
    run(go())


def test_drop_oldest_merges_gaps_per_topic():
    async def go():
        o = Outbox(max_events=3, policy="drop_oldest")
        o.put_control(Item(frame="hello"))
        dropped = []
        for s in range(1, 7):
            dropped += await o.put_event(ev(s))
        assert dropped == [("t", 1), ("t", 2), ("t", 3)]
        frames = [(await o.get()).render() for _ in range(len(o))]
        assert frames[0] == "hello"
        assert '"from_seq":1' in frames[1] and '"to_seq":3' in frames[1] and "slow_consumer" in frames[1]
        assert frames[2:] == ["e4", "e5", "e6"]
    run(go())


def test_drop_oldest_keeps_topics_apart():
    async def go():
        o = Outbox(max_events=2, policy="drop_oldest")
        await o.put_event(ev(1, "a"))
        await o.put_event(ev(1, "b"))
        await o.put_event(ev(2, "a"))  # drops a:1
        await o.put_event(ev(2, "b"))  # drops b:1
        frames = [(await o.get()).render() for _ in range(len(o))]
        gaps = [f for f in frames if "gap" in f]
        assert len(gaps) == 2 and '"topic":"a"' in gaps[0] and '"topic":"b"' in gaps[1]
    run(go())


def test_paced_put_waits_instead_of_dropping():
    async def go():
        o = Outbox(max_events=1, policy="drop_oldest")
        await o.put_event(ev(1), paced=True)
        p = asyncio.create_task(o.put_event(ev(2), paced=True))
        for _ in range(10):  # however many loop turns pass, a paced put keeps waiting
            await asyncio.sleep(0)
        assert not p.done() and o.events == 1
        assert (await o.get()).seq == 1
        assert await p == []
        assert (await o.get()).seq == 2
    run(go())


def test_purge_topic_and_close():
    async def go():
        o = Outbox(max_events=10)
        await o.put_event(ev(1, "a"))
        await o.put_event(ev(1, "b"))
        o.put_control(Item(frame="ctl"))
        o.purge_topic("a")
        assert o.events == 1
        o.close()
        assert (await o.get()).topic == "b"
        assert (await o.get()).frame == "ctl"
        assert await o.get() is None
    run(go())


def test_unknown_policy():
    with pytest.raises(ValueError):
        Outbox(max_events=1, policy="spill")

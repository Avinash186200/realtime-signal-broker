"""`sigbroker demo`: a two-minute tour in one process.

A broker on a temporary SQLite log, a publisher, a trading-desk subscriber and a
dashboard subscriber, all through a fault-injection proxy. Midway the desk's link is cut
and later the broker crashes and restarts; at the end each subscriber's stream is checked.
"""
from __future__ import annotations

import asyncio
import random
import tempfile
from pathlib import Path

from .auth import mint_token
from .backends import SQLiteBackend
from .broker import Broker, BrokerConfig
from .chaos import ChaosProxy
from .client import Backoff, Subscriber
from .metrics import counter_value
from .publisher import Publisher
from .synthetic import SignalGenerator

TOPIC = "signals.rates"


async def run_demo(n: int, secret: str) -> dict:
    tmp = tempfile.TemporaryDirectory()
    db = str(Path(tmp.name) / "demo.sqlite")
    cfg = BrokerConfig(heartbeat_s=1.0, drain_timeout_s=1.0)
    broker = await Broker(secret, SQLiteBackend(db), cfg).start()
    proxy = await ChaosProxy(broker.port).start()
    print(f"broker on port {broker.port}, clients connect through a fault proxy at {proxy.url}")
    sub_tok = mint_token(secret, "desk", "subscriber", ["signals.*"])
    pub_tok = mint_token(secret, "research", "publisher", ["signals.*"])
    got: dict[str, list[int]] = {"desk": [], "dashboard": []}

    async def consume(name: str, sub: Subscriber):
        async for d in sub:
            got[name].append(d.seq)
            s = d.signal
            if name == "desk" and (d.seq % 10 == 0 or s.kind == "alert"):
                detail = f"{s.value:+.3f}" if s.kind == "score" else f"ALERT {s.severity}: {s.message}"
                print(f"  desk      seq {d.seq:>3}  {s.model:<8} {s.instrument:<7} {detail}")

    mk = lambda seed: Backoff(base_s=0.02, cap_s=0.5, rng=random.Random(seed))  # noqa: E731
    desk = Subscriber(proxy.url, sub_tok, [TOPIC], backoff=mk(1))
    dash = Subscriber(proxy.url, sub_tok, [TOPIC], backoff=mk(2), slow_policy="drop_oldest", gap_mode="skip")
    tasks = [asyncio.create_task(consume("desk", desk)), asyncio.create_task(consume("dashboard", dash))]
    gen = SignalGenerator(7)
    async with Publisher(proxy.url, pub_tok, backoff=mk(3)) as pub:
        for i in range(1, n + 1):
            if i == n // 3:
                print(f"-- cutting every connection after {i - 1} messages")
                proxy.cut_all()
            if i == 2 * n // 3:
                print(f"-- broker crash after {i - 1} messages; a new broker opens the same log")
                await broker.abort()
                broker = await Broker(secret, SQLiteBackend(db), cfg).start()
                proxy.upstream_port = broker.port
            await pub.publish(TOPIC, gen.next(), msg_id=f"demo-{i}")
            await asyncio.sleep(0.02)
    for _ in range(200):
        if all(len(v) >= n for v in got.values()):
            break
        await asyncio.sleep(0.05)
    for s in (desk, dash):
        await s.close()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    out = {}
    for name, sub in (("desk", desk), ("dashboard", dash)):
        ok = got[name] == list(range(1, n + 1))
        out[name] = {"received": len(got[name]), "exact": ok, "reconnects": sub.stats.reconnects,
                     "duplicates_dropped": sub.stats.duplicates_dropped, "gaps": len(sub.gaps)}
        print(f"{name:<9}: received {len(got[name])}/{n}, in order with no gap or duplicate: {ok}, "
              f"reconnects {sub.stats.reconnects}, duplicates dropped by the SDK {sub.stats.duplicates_dropped}")
    reg = broker.metrics.registry
    print(f"broker metrics (after restart): resumes {counter_value(reg, 'sigbroker_resumes_total'):.0f}, "
          f"replayed {counter_value(reg, 'sigbroker_replayed_total'):.0f}")
    await broker.stop()
    await proxy.stop()
    tmp.cleanup()
    return out

"""Fault-injection scenarios: the correctness suite behind results/correctness.json.

One scenario = one broker (SQLite or Redis-protocol log) behind a ChaosProxy, one
publisher and six subscribers, a seeded stream of messages on two topics, and a seeded
schedule of faults injected between messages:

- ``cut``       every TCP connection through the proxy is dropped mid-stream;
- ``partition`` connections are dropped and new ones refused for 150-400 ms;
- ``crash``     the broker process state is thrown away without a close handshake
                (``Broker.abort``) and a new broker opens the same log;
- ``restart``   graceful broker stop (drain, bye, close 1001) and a new broker;
- ``app_restart`` one subscriber's application is restarted: the Subscriber object is
                closed and a new one resumes from its file cursor store.

Every scenario also has one ``stall``: the three slow subscribers stop handling messages
while a burst of ``stall_burst`` messages is published, so their queues fill and the
slow-consumer policies act.

Subscribers:

- ``desk_fast``     default settings;
- ``desk_slow``     stalls; the broker's ``disconnect`` policy closes it when its
                    queue stays full, and it resumes from its last seq;
- ``desk_app``      restarted from its cursor file (see ``app_restart``);
- ``desk_late``     joins after a third of the stream, from the earliest message;
- ``dash_resync``   stalls; ``drop_oldest`` policy, re-fetches every gap from the log;
- ``dash_skip``     stalls; ``drop_oldest`` policy, skips gaps but must report them.

Checks, per subscriber and topic, after the stream ends:

- strict subscribers received exactly seq 1..head in order: no gap, no duplicate;
- ``dash_skip``: every seq was either delivered once or inside a reported gap, never both;
- the log holds each published msg_id exactly once (idempotent publishing).
"""
from __future__ import annotations

import asyncio
import random
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path

from .auth import mint_token, new_secret
from .backends import SQLiteBackend
from .broker import Broker, BrokerConfig
from .chaos import ChaosProxy
from .client import Backoff, FileCursorStore, Subscriber
from .metrics import counter_value
from .publisher import Publisher
from .synthetic import SignalGenerator

TOPICS = ("signals.rates", "signals.fx")
STRICT = ("desk_fast", "desk_slow", "desk_app", "desk_late", "dash_resync")
FAULTS = ("cut", "partition", "crash", "restart", "app_restart")


@dataclass
class ScenarioConfig:
    n_messages: int = 600      # the base stream; the stall burst comes on top
    n_faults: int = 6
    burst: int = 25            # messages published concurrently per burst
    stall_burst: int = 5000    # published while the slow subscribers are stalled
    stall_burst_size: int = 100
    backend: str = "sqlite"    # sqlite | fakeredis | redis://...
    max_queue: int = 64        # small, so slow consumers hit the policy
    slow_grace_s: float = 0.15
    slow_buffer: int = 16      # SDK-side buffer for the slow subscribers
    settle_timeout_s: float = 30.0


@dataclass
class _Consumer:
    name: str
    seqs: dict[str, list[int]] = field(default_factory=lambda: {t: [] for t in TOPICS})
    sub: Subscriber | None = None
    task: asyncio.Task | None = None
    reconnects: int = 0
    resyncs: int = 0
    dups_dropped: int = 0
    stale_dropped: int = 0
    gaps: list = field(default_factory=list)

    def absorb_stats(self) -> None:
        if self.sub is not None:
            s = self.sub.stats
            self.reconnects += s.reconnects
            self.resyncs += s.resyncs
            self.dups_dropped += s.duplicates_dropped
            self.stale_dropped += s.stale_dropped
            self.gaps.extend(self.sub.gaps)


def fault_schedule(seed: int, cfg: ScenarioConfig) -> tuple[list[tuple[int, str]], int]:
    """Seeded (message index, fault) pairs and the index where the stall starts.

    The first and last 5% of the stream stay clean, and no fault lands inside the stall."""
    rng = random.Random(seed * 7919 + 1)
    total = cfg.n_messages + cfg.stall_burst
    stall_at = rng.randrange(int(cfg.n_messages * 0.3), int(cfg.n_messages * 0.6))
    lo, hi = int(total * 0.05), int(total * 0.95)
    allowed = [i for i in range(lo, hi) if not stall_at <= i <= stall_at + cfg.stall_burst]
    idx = sorted(rng.sample(allowed, cfg.n_faults))
    return [(i, rng.choice(FAULTS)) for i in idx], stall_at


class _Harness:
    def __init__(self, seed: int, workdir: Path, cfg: ScenarioConfig):
        self.seed, self.workdir, self.cfg = seed, workdir, cfg
        self.secret = new_secret()
        self.broker: Broker | None = None
        self.proxy: ChaosProxy | None = None
        self.fakeredis_server = None
        self.metric_totals = {"slow_disconnects": 0.0, "dropped": 0.0, "publish_duplicates": 0.0}

    def _backend(self):
        b = self.cfg.backend
        if b == "sqlite":
            return SQLiteBackend(str(self.workdir / f"log-{self.seed}.sqlite"))
        from .backends.redis import RedisBackend
        if b == "fakeredis":
            import fakeredis
            if self.fakeredis_server is None:
                self.fakeredis_server = fakeredis.FakeServer()
            client = fakeredis.FakeAsyncRedis(server=self.fakeredis_server, decode_responses=True)
            return RedisBackend("redis://fake", prefix=f"t{self.seed}:", client=client)
        return RedisBackend(b, prefix=f"scenario{self.seed}-{time.time_ns()}:")

    async def start_broker(self) -> None:
        cfg = BrokerConfig(max_queue=self.cfg.max_queue, slow_grace_s=self.cfg.slow_grace_s, heartbeat_s=1.0,
                           drain_timeout_s=2.0, reconnect_hint_ms=50)
        if self.broker is not None and self.cfg.backend.startswith("redis://"):
            prefix = self.broker.backend.prefix
            backend = self._backend()
            backend.prefix = prefix
        else:
            backend = self._backend()
        self.broker = await Broker(self.secret, backend, cfg).start()
        if self.proxy is None:
            self.proxy = await ChaosProxy(self.broker.port, buffer_bytes=None).start()
        else:
            self.proxy.upstream_port = self.broker.port

    def _bank_metrics(self) -> None:
        r = self.broker.metrics.registry
        self.metric_totals["slow_disconnects"] += counter_value(r, "sigbroker_slow_disconnects_total")
        self.metric_totals["dropped"] += counter_value(r, "sigbroker_dropped_total")
        self.metric_totals["publish_duplicates"] += counter_value(r, "sigbroker_publish_duplicates_total")


async def _consume(c: _Consumer, gate: asyncio.Event | None) -> None:
    async for d in c.sub:
        c.seqs[d.topic].append(d.seq)
        if gate is not None and not gate.is_set():
            await gate.wait()  # stalled application


def _check_strict(seqs: list[int], head: int) -> dict:
    expected = list(range(1, head + 1))
    s = set(seqs)
    return {"delivered": len(seqs), "missing": len(set(expected) - s),
            "duplicates": len(seqs) - len(s),
            "out_of_order": sum(1 for a, b in zip(seqs, seqs[1:]) if b <= a),
            "exact": seqs == expected}


def _check_skip(seqs: list[int], gaps: list, topic: str, head: int) -> dict:
    covered: list[int] = []
    for g in gaps:
        if g.topic == topic and g.to_seq >= g.from_seq:
            covered.extend(range(g.from_seq, g.to_seq + 1))
    delivered, gapped = set(seqs), set(covered)
    return {"delivered": len(seqs), "in_reported_gaps": len(gapped),
            "both": len(delivered & gapped),
            "unaccounted": len(set(range(1, head + 1)) - delivered - gapped),
            "duplicates": len(seqs) - len(delivered) + len(covered) - len(gapped),
            "out_of_order": sum(1 for a, b in zip(seqs, seqs[1:]) if b <= a)}


async def run_scenario(seed: int, workdir: Path, cfg: ScenarioConfig | None = None) -> dict:
    cfg = cfg or ScenarioConfig()
    workdir = Path(workdir)
    workdir.mkdir(parents=True, exist_ok=True)
    for stale in workdir.glob(f"*-{seed}.*"):  # a fresh log and cursor file per run
        stale.unlink()
    h = _Harness(seed, workdir, cfg)
    rng = random.Random(seed)
    rng_backoff = random.Random(seed + 1)
    t0 = time.perf_counter()
    await h.start_broker()
    url = h.proxy.url
    sub_token = mint_token(h.secret, f"sub-{seed}", "subscriber", ["signals.*"])
    pub_token = mint_token(h.secret, f"pub-{seed}", "publisher", ["signals.*"])
    fast_backoff = lambda: Backoff(base_s=0.01, cap_s=0.25, rng=random.Random(rng_backoff.random()))  # noqa: E731

    consumers: dict[str, _Consumer] = {n: _Consumer(n) for n in STRICT + ("dash_skip",)}
    settings = {
        "desk_fast": dict(), "desk_slow": dict(), "desk_late": dict(),
        "desk_app": dict(cursor_store=FileCursorStore(str(workdir / f"cursor-{seed}.json"))),
        "dash_resync": dict(slow_policy="drop_oldest", gap_mode="resync"),
        "dash_skip": dict(slow_policy="drop_oldest", gap_mode="skip"),
    }
    slow = ("desk_slow", "dash_resync", "dash_skip")
    gate = asyncio.Event()
    gate.set()

    def launch(name: str) -> None:
        c = consumers[name]
        kw = dict(settings[name])
        if name == "desk_app":  # a fresh store object reads the file, like a new process would
            kw["cursor_store"] = FileCursorStore(str(workdir / f"cursor-{seed}.json"))
        buf = cfg.slow_buffer if name in slow else 256
        c.sub = Subscriber(url, sub_token, TOPICS, backoff=fast_backoff(), heartbeat_misses=3, buffer=buf, **kw)
        c.task = asyncio.create_task(_consume(c, gate if name in slow else None))

    for n in consumers:
        if n != "desk_late":
            launch(n)

    sched, stall_at = fault_schedule(seed, cfg)
    schedule = dict(sched)
    total = cfg.n_messages + cfg.stall_burst
    stall_end = None
    faults_done = {f: 0 for f in FAULTS}
    gen = SignalGenerator(seed)
    published = {t: [] for t in TOPICS}
    pub = Publisher(url, pub_token, backoff=fast_backoff())
    await pub.start()
    i = 0
    try:
        while i < total:
            batch = []
            if i == stall_at:
                gate.clear()
                stall_end = i + cfg.stall_burst
            size = cfg.stall_burst_size if stall_end is not None else cfg.burst
            for _ in range(size):
                if i >= total or (stall_end is not None and i >= stall_end):
                    break
                if i in schedule and batch:
                    break
                if i in schedule:
                    await _inject(h, schedule[i], consumers, launch)
                    faults_done[schedule[i]] += 1
                topic = TOPICS[rng.random() < 0.4]
                msg_id = f"s{seed}-m{i}"
                published[topic].append(msg_id)
                batch.append(pub.publish(topic, gen.next(), msg_id=msg_id, timeout_s=30))
                i += 1
                if i == cfg.n_messages // 3:
                    launch("desk_late")
                if i == stall_at:
                    break
            await asyncio.gather(*batch)
            if stall_end is not None and i >= stall_end:
                await asyncio.sleep(cfg.slow_grace_s * 4)  # long enough for the policy to act
                gate.set()
                stall_end = None
        heads = {t: len(published[t]) for t in TOPICS}
        await _settle(consumers, heads, cfg.settle_timeout_s)
    finally:
        gate.set()
        await pub.close()
        for c in consumers.values():
            c.absorb_stats()
            if c.sub is not None:
                await c.sub.close()
            if c.task is not None:
                c.task.cancel()
                await asyncio.gather(c.task, return_exceptions=True)
        h._bank_metrics()
        log_check = await _log_check(h, published)
        await h.broker.stop(drain_timeout_s=1.0)
        await h.proxy.stop()

    result = {"seed": seed, "backend": "redis" if cfg.backend.startswith("redis://") else cfg.backend,
              "published": {t: len(v) for t, v in published.items()},
              "faults": faults_done, "log": log_check, "subscribers": {}}
    ok = log_check["duplicate_msg_ids"] == 0 and log_check["missing_msg_ids"] == 0
    for name, c in consumers.items():
        per_topic = {}
        for t in TOPICS:
            if name == "dash_skip":
                r = _check_skip(c.seqs[t], c.gaps, t, heads[t])
                ok &= r["both"] == 0 and r["unaccounted"] == 0 and r["duplicates"] == 0 and r["out_of_order"] == 0
                r = {"unaccounted": r["unaccounted"], "both": r["both"], "duplicates": r["duplicates"],
                     "out_of_order": r["out_of_order"], "delivered_observed": r["delivered"],
                     "in_reported_gaps_observed": r["in_reported_gaps"]}
            else:
                r = _check_strict(c.seqs[t], heads[t])
                ok &= r["exact"]
            per_topic[t] = r
        result["subscribers"][name] = {
            "topics": per_topic, "reconnects_observed": c.reconnects, "resyncs_observed": c.resyncs,
            "duplicates_dropped_by_sdk_observed": c.dups_dropped, "stale_dropped_by_sdk_observed": c.stale_dropped,
            "gaps_reported_observed": len(c.gaps)}
    result["broker_observed"] = {k: int(v) for k, v in h.metric_totals.items()}
    result["publisher_resent_observed"] = pub.resent
    result["passed"] = bool(ok)
    result["wall_s_observed"] = round(time.perf_counter() - t0, 2)
    return result


async def _inject(h: _Harness, fault: str, consumers: dict, launch) -> None:
    if fault == "cut":
        h.proxy.cut_all()
    elif fault == "partition":
        h.proxy.refuse = True
        h.proxy.cut_all()
        await asyncio.sleep(random.Random(h.seed).uniform(0.15, 0.4))
        h.proxy.refuse = False
    elif fault in ("crash", "restart"):
        h._bank_metrics()
        if fault == "crash":
            await h.broker.abort()
        else:
            await h.broker.stop(drain_timeout_s=1.0)
        h.proxy.cut_all()
        await h.start_broker()
    elif fault == "app_restart":
        c = consumers["desk_app"]
        c.absorb_stats()
        await c.sub.close()
        c.task.cancel()
        await asyncio.gather(c.task, return_exceptions=True)
        launch("desk_app")


async def _settle(consumers: dict, heads: dict, timeout_s: float) -> None:
    """Wait until every subscriber has seen the head of every topic (or reported past it)."""
    deadline = time.perf_counter() + timeout_s
    while time.perf_counter() < deadline:
        done = True
        for c in consumers.values():
            for t, head in heads.items():
                if head == 0:
                    continue
                last = c.sub.tracker.cursor(t).last_seq if c.sub is not None else None
                if last is None or last < head:
                    done = False
                elif c.name != "dash_skip" and (not c.seqs[t] or c.seqs[t][-1] < head):
                    done = False
        if done:
            return
        await asyncio.sleep(0.05)


async def _log_check(h: _Harness, published: dict) -> dict:
    """Every published msg_id is in the log exactly once, and seqs are 1..n without holes."""
    dup = missing = holes = 0
    for t, ids in published.items():
        events = []
        after = 0
        while True:
            batch = await h.broker.backend.read(t, after, 1000)
            if not batch:
                break
            events.extend(batch)
            after = batch[-1].seq
        seqs = [e.seq for e in events]
        holes += int(seqs != list(range(1, len(seqs) + 1)))
        got = [e.msg_id for e in events]
        dup += len(got) - len(set(got))
        missing += len(set(ids) - set(got))
    return {"duplicate_msg_ids": dup, "missing_msg_ids": missing, "seq_holes": holes}


def summarize(results: list[dict]) -> dict:
    out = {"scenarios": len(results), "passed": sum(r["passed"] for r in results),
           "messages_published": sum(sum(r["published"].values()) for r in results),
           "faults": {f: sum(r["faults"][f] for r in results) for f in FAULTS},
           "log_duplicate_msg_ids": sum(r["log"]["duplicate_msg_ids"] for r in results),
           "log_missing_msg_ids": sum(r["log"]["missing_msg_ids"] for r in results),
           "subscribers": {}}
    for name in STRICT:
        out["subscribers"][name] = {
            "missing": sum(r["subscribers"][name]["topics"][t]["missing"] for r in results for t in TOPICS),
            "duplicates": sum(r["subscribers"][name]["topics"][t]["duplicates"] for r in results for t in TOPICS),
            "out_of_order": sum(r["subscribers"][name]["topics"][t]["out_of_order"] for r in results for t in TOPICS),
            "exact_streams": sum(r["subscribers"][name]["topics"][t]["exact"] for r in results for t in TOPICS),
            "reconnects_observed": sum(r["subscribers"][name]["reconnects_observed"] for r in results),
            "resyncs_observed": sum(r["subscribers"][name]["resyncs_observed"] for r in results),
            "duplicates_dropped_by_sdk_observed": sum(
                r["subscribers"][name]["duplicates_dropped_by_sdk_observed"] for r in results),
        }
    sk = "dash_skip"
    out["subscribers"][sk] = {
        k: sum(r["subscribers"][sk]["topics"][t][k] for r in results for t in TOPICS)
        for k in ("unaccounted", "both", "duplicates", "out_of_order", "delivered_observed",
                  "in_reported_gaps_observed")}
    out["subscribers"][sk]["reconnects_observed"] = sum(r["subscribers"][sk]["reconnects_observed"] for r in results)
    out["broker_observed"] = {k: sum(r["broker_observed"][k] for r in results) for k in results[0]["broker_observed"]}
    out["publisher_resent_observed"] = sum(r["publisher_resent_observed"] for r in results)
    out["wall_s_observed"] = round(sum(r["wall_s_observed"] for r in results), 1)
    return out


def run_many(seeds, workdir: Path, cfg: ScenarioConfig | None = None) -> list[dict]:
    return [asyncio.run(run_scenario(s, workdir, cfg)) for s in seeds]


__all__ = ["ScenarioConfig", "run_scenario", "run_many", "summarize", "fault_schedule", "asdict"]

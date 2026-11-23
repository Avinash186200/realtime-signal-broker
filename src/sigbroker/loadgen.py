"""Load generator: end-to-end latency and throughput vs subscribers and message rate.

Process layout, all on one machine:

- the broker runs in its own process (``sigbroker serve``);
- subscribers run in ``sub_procs`` worker processes, many asyncio subscribers per process;
- the publisher runs in this process and publishes open-loop at a fixed rate (each
  message is scheduled at ``t0 + i / rate`` whether or not earlier acks have arrived).

Latency = subscriber receive time - publisher send time, both from
``time.perf_counter_ns()``. That clock is system-wide on Windows (QueryPerformanceCounter)
and Linux (CLOCK_MONOTONIC), so the two stamps compare across processes on the same
machine; it means nothing across machines. The first ``warmup_s`` of messages are left out.
"""
from __future__ import annotations

import asyncio
import json
import multiprocessing as mp
import os
import platform
import subprocess
import sys
import time
import urllib.request
from dataclasses import dataclass

from .auth import mint_token, new_secret
from .client import Backoff, Subscriber
from .publisher import Publisher
from .synthetic import SignalGenerator

TOPIC = "signals.load"


@dataclass
class Cell:
    subscribers: int
    rate: float
    duration_s: float = 5.0
    warmup_s: float = 1.0
    backend: str = "sqlite"  # memory | sqlite | redis://...
    sub_procs: int = 4


def machine_info() -> dict:
    cpu = platform.processor() or platform.machine()
    if sys.platform == "win32":
        try:
            cpu = subprocess.run(["powershell", "-NoProfile", "-Command", "(Get-CimInstance Win32_Processor).Name"],
                                 capture_output=True, text=True, timeout=20).stdout.strip() or cpu
        except Exception:
            pass
    elif os.path.exists("/proc/cpuinfo"):
        for line in open("/proc/cpuinfo", encoding="utf-8"):
            if line.startswith("model name"):
                cpu = line.split(":", 1)[1].strip()
                break
    return {"cpu": cpu, "logical_cpus": os.cpu_count(), "os": f"{platform.system()} {platform.version()}",
            "python": platform.python_version()}


def _percentile(sorted_vals: list[float], q: float) -> float:
    if not sorted_vals:
        return float("nan")
    k = (len(sorted_vals) - 1) * q
    lo = int(k)
    hi = min(lo + 1, len(sorted_vals) - 1)
    return sorted_vals[lo] + (sorted_vals[hi] - sorted_vals[lo]) * (k - lo)


# ---------------------------------------------------------------------------
# subscriber worker process
# ---------------------------------------------------------------------------

def _sub_worker(url: str, token: str, n: int, worker: int, ready, stop, out_q) -> None:
    asyncio.run(_sub_main(url, token, n, worker, ready, stop, out_q))


async def _sub_main(url, token, n, worker, ready, stop, out_q) -> None:
    import random
    subs = [Subscriber(url, token, [TOPIC], start="latest", buffer=10_000,
                       backoff=Backoff(0.05, 2.0, random.Random(worker * 1000 + i))) for i in range(n)]
    recs = [{"lat_ns": [], "seqs": 0, "first": None, "last": 0, "dups": 0, "holes": 0} for _ in range(n)]

    async def consume(i, s):
        r = recs[i]
        async for d in s:
            r["lat_ns"].append((d.received_pc_ns - d.sent_ns, d.seq))
            # Independent check of what the application saw (the SDK should make both 0).
            if r["first"] is None:
                r["first"] = d.seq
            elif d.seq <= r["last"]:
                r["dups"] += 1
            elif d.seq != r["last"] + 1:
                r["holes"] += 1
            r["seqs"] += 1
            r["last"] = max(r["last"], d.seq)

    tasks = [asyncio.create_task(consume(i, s)) for i, s in enumerate(subs)]
    while not all(s.tracker.cursor(TOPIC).last_seq is not None for s in subs):
        await asyncio.sleep(0.02)
    ready.set()
    while not stop.is_set():
        await asyncio.sleep(0.05)
    for s in subs:
        await s.close()
    for t in tasks:
        t.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    out_q.put([{"lat": r["lat_ns"], "count": r["seqs"], "first": r["first"], "last": r["last"],
                "dups": r["dups"], "holes": r["holes"],
                "reconnects": s.stats.reconnects, "dups_dropped": s.stats.duplicates_dropped,
                "gaps": len(s.gaps)} for r, s in zip(recs, subs)])


# ---------------------------------------------------------------------------
# one cell
# ---------------------------------------------------------------------------

def _start_broker(secret: str, backend: str, workdir: str) -> tuple[subprocess.Popen, int]:
    spec = backend
    if backend == "sqlite":
        path = os.path.join(workdir, f"load-{time.time_ns()}.sqlite")
        spec = f"sqlite:{path}"
    env = dict(os.environ, SIGBROKER_SECRET=secret)
    proc = subprocess.Popen([sys.executable, "-m", "sigbroker.cli", "serve", "--port", "0", "--backend", spec,
                             "--max-queue", "10000", "--slow-grace", "2.0"],
                            env=env, stdout=subprocess.PIPE, text=True)
    line = proc.stdout.readline()
    if not line.startswith("PORT "):
        proc.kill()
        raise RuntimeError(f"broker did not start: {line!r}")
    return proc, int(line.split()[1])


def _scrape(port: int) -> dict[str, float]:
    text = urllib.request.urlopen(f"http://127.0.0.1:{port}/metrics", timeout=5).read().decode()
    out: dict[str, float] = {}
    for line in text.splitlines():
        if line.startswith("#") or not line.strip():
            continue
        name_labels, _, value = line.rpartition(" ")
        name = name_labels.split("{", 1)[0]
        try:
            out[name] = out.get(name, 0.0) + float(value)
        except ValueError:
            pass
    return out


async def _publish_open_loop(url: str, token: str, rate: float, total: int) -> dict:
    gen = SignalGenerator(0)
    payloads = [gen.next().model_dump(mode="json") for _ in range(min(total, 5000))]
    async with Publisher(url, token, open_timeout_s=10) as pub:
        t0 = time.perf_counter()
        pending = []
        for i in range(total):
            target = t0 + i / rate
            delay = target - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            pending.append(asyncio.ensure_future(pub.publish(TOPIC, payloads[i % len(payloads)], timeout_s=120)))
        send_s = time.perf_counter() - t0
        acks = await asyncio.gather(*pending)
        ack_s = time.perf_counter() - t0
    return {"send_s": send_s, "ack_s": ack_s, "last_seq": max(a.seq for a in acks)}


def run_cell(cell: Cell, workdir: str) -> dict:
    secret = new_secret()
    proc, port = _start_broker(secret, cell.backend, workdir)
    url = f"ws://127.0.0.1:{port}/ws"
    sub_tok = mint_token(secret, "load-sub", "subscriber", [TOPIC])
    pub_tok = mint_token(secret, "load-pub", "publisher", [TOPIC])
    ctx = mp.get_context("spawn")
    procs_n = max(1, min(cell.sub_procs, cell.subscribers))
    per = [cell.subscribers // procs_n + (1 if i < cell.subscribers % procs_n else 0) for i in range(procs_n)]
    stop = ctx.Event()
    out_q = ctx.Queue()
    readies, workers = [], []
    try:
        for w, n in enumerate(per):
            ready = ctx.Event()
            p = ctx.Process(target=_sub_worker, args=(url, sub_tok, n, w, ready, stop, out_q), daemon=True)
            p.start()
            readies.append(ready)
            workers.append(p)
        for r in readies:
            if not r.wait(120):
                raise RuntimeError("subscribers did not connect")
        total = int(cell.rate * (cell.duration_s + cell.warmup_s))
        pubres = asyncio.run(_publish_open_loop(url, pub_tok, cell.rate, total))
        # Let subscribers finish what is in flight (bounded).
        deadline = time.perf_counter() + 30
        while time.perf_counter() < deadline:
            m = _scrape(port)
            if m.get("sigbroker_delivered_total", 0) >= total * cell.subscribers:
                break
            time.sleep(0.1)
        metrics = _scrape(port)
        stop.set()
        results = []
        for _ in workers:
            results.extend(out_q.get(timeout=120))
        for p in workers:
            p.join(30)
    finally:
        stop.set()
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()
    warm_seq = int(cell.rate * cell.warmup_s)
    lat = sorted(l / 1e6 for r in results for l, seq in r["lat"] if seq > warm_seq)
    delivered = sum(r["count"] for r in results)
    lost = sum(max(0, pubres["last_seq"] - (r["first"] or 1) + 1 - (r["count"] - r["dups"])) for r in results)
    measured = cell.duration_s * cell.subscribers * cell.rate
    return {
        "subscribers": cell.subscribers, "rate": cell.rate, "backend": cell.backend.split(":")[0],
        "duration_s": cell.duration_s, "published": pubres["last_seq"],
        "achieved_rate": round(total / pubres["send_s"], 1),
        "deliveries": delivered, "deliveries_per_s_offered": round(cell.subscribers * cell.rate, 1),
        "latency_samples": len(lat), "expected_samples": int(measured),
        "p50_ms": round(_percentile(lat, 0.50), 3), "p95_ms": round(_percentile(lat, 0.95), 3),
        "p99_ms": round(_percentile(lat, 0.99), 3), "max_ms": round(lat[-1], 3) if lat else None,
        "lost": lost, "dups_delivered": sum(r["dups"] for r in results),
        "holes_seen": sum(r["holes"] for r in results),
        "sdk_dups_dropped": sum(r["dups_dropped"] for r in results),
        "reconnects": sum(r["reconnects"] for r in results),
        "gaps_reported": sum(r["gaps"] for r in results),
        "slow_disconnects": int(metrics.get("sigbroker_slow_disconnects_total", 0)),
    }


def main_cli() -> None:  # pragma: no cover - convenience
    import argparse
    p = argparse.ArgumentParser()
    p.add_argument("--subs", type=int, default=10)
    p.add_argument("--rate", type=float, default=1000)
    p.add_argument("--duration", type=float, default=5)
    p.add_argument("--backend", default="sqlite")
    a = p.parse_args()
    print(json.dumps(run_cell(Cell(a.subs, a.rate, a.duration, backend=a.backend), "."), indent=1))


if __name__ == "__main__":  # pragma: no cover
    main_cli()


# ---------------------------------------------------------------------------
# catch-up after an outage
# ---------------------------------------------------------------------------

def run_catchup(workdir: str, rate: float = 1000.0, offline_s: float = 5.0, before_s: float = 2.0,
                after_s: float = 3.0, backend: str = "sqlite") -> dict:
    """A subscriber drops for ``offline_s`` while messages keep coming, then resumes."""
    secret = new_secret()
    proc, port = _start_broker(secret, backend, workdir)
    url = f"ws://127.0.0.1:{port}/ws"
    try:
        return asyncio.run(_catchup(url, secret, rate, offline_s, before_s, after_s, backend))
    finally:
        proc.terminate()
        try:
            proc.wait(10)
        except subprocess.TimeoutExpired:
            proc.kill()


async def _catchup(url, secret, rate, offline_s, before_s, after_s, backend) -> dict:
    from .client import MemoryCursorStore
    sub_tok = mint_token(secret, "desk", "subscriber", [TOPIC])
    pub_tok = mint_token(secret, "research", "publisher", [TOPIC])
    store = MemoryCursorStore()  # survives the "outage" like a cursor file would
    seqs: list[int] = []

    async def consume(sub):
        async for d in sub:
            seqs.append(d.seq)

    total = int(rate * (before_s + offline_s + after_s))
    gen = SignalGenerator(1)
    payload = gen.next().model_dump(mode="json")
    sub = Subscriber(url, sub_tok, [TOPIC], cursor_store=store)
    task = asyncio.create_task(consume(sub))
    while sub.tracker.cursor(TOPIC).last_seq is None:
        await asyncio.sleep(0.01)
    async with Publisher(url, pub_tok) as pub:
        t0 = time.perf_counter()
        pending, missed_from, catch = [], None, {}
        for i in range(total):
            now = time.perf_counter() - t0
            if missed_from is None and now >= before_s:
                await sub.close()
                task.cancel()
                missed_from = len(seqs)
            if missed_from is not None and "reconnect_at" not in catch and now >= before_s + offline_s:
                catch["head_at_reconnect"] = i
                catch["reconnect_at"] = time.perf_counter()
                sub = Subscriber(url, sub_tok, [TOPIC], cursor_store=store)
                task = asyncio.create_task(consume(sub))
            delay = t0 + i / rate - time.perf_counter()
            if delay > 0:
                await asyncio.sleep(delay)
            pending.append(asyncio.ensure_future(pub.publish(TOPIC, payload, timeout_s=60)))
            if "reconnect_at" in catch and "caught_up_at" not in catch and seqs and \
                    seqs[-1] >= catch["head_at_reconnect"]:
                catch["caught_up_at"] = time.perf_counter()
        await asyncio.gather(*pending)
    deadline = time.perf_counter() + 30
    while time.perf_counter() < deadline and (not seqs or seqs[-1] < total):
        if "caught_up_at" not in catch and seqs and seqs[-1] >= catch["head_at_reconnect"]:
            catch["caught_up_at"] = time.perf_counter()
        await asyncio.sleep(0.01)
    if "caught_up_at" not in catch and seqs and seqs[-1] >= catch["head_at_reconnect"]:
        catch["caught_up_at"] = time.perf_counter()
    await sub.close()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    missed = catch["head_at_reconnect"] - (seqs[missed_from - 1] if missed_from else 0)
    return {"backend": backend, "rate": rate, "offline_s": offline_s, "published": total,
            "received": len(seqs), "missed": missed,
            "catchup_ms": round((catch["caught_up_at"] - catch["reconnect_at"]) * 1000, 1),
            "lost": len(set(range(1, total + 1)) - set(seqs)), "duplicates": len(seqs) - len(set(seqs))}

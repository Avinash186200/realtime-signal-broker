"""Benchmarks: the correctness suite and the load benchmark, and the RESULTS.md renderer.

- ``run_correctness`` -> results/correctness.json. Fault-injection scenarios on development
  seeds (used while building) and held-out seeds (run once, at the end). Every field
  except those ending in ``_observed`` is deterministic and checked by a test.
- ``run_load_bench`` -> results/load.json. Latency and throughput on this machine; the
  numbers depend on the hardware and are not checked by tests (only the zero-loss fields).
"""
from __future__ import annotations

import asyncio
import json
import tempfile
import time
from dataclasses import asdict
from pathlib import Path

from .scenarios import ScenarioConfig, run_scenario, summarize

DEV_SEEDS = tuple(range(0, 10))
DEV_SEEDS_REDIS = tuple(range(0, 5))
HOLDOUT_SEEDS = tuple(range(200, 220))  # 100-119 were the first held-out set; see docs/methodology.md


def _write_json(path: Path, obj) -> None:
    path.write_text(json.dumps(obj, indent=1, sort_keys=False) + "\n", encoding="utf-8", newline="\n")


def _suite(seeds, backend: str, workdir: Path) -> dict:
    cfg = ScenarioConfig(backend=backend)
    per_seed = []
    for s in seeds:
        r = asyncio.run(run_scenario(s, workdir, cfg))
        status = "ok" if r["passed"] else "FAILED"
        print(f"  {backend:<9} seed {s:>3}: {status} ({r['wall_s_observed']} s)", flush=True)
        per_seed.append(r)
    return {"backend": backend, "seeds": list(seeds), "summary": summarize(per_seed), "per_seed": per_seed}


def run_correctness(out: Path, quick: bool = False, redis_url: str | None = None) -> dict:
    """``redis_url``: instead, run the development seeds against a real Redis and write
    results/correctness_redis.json (needs a Redis server, so not part of CI)."""
    out.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory() as tmp:
        work = Path(tmp)
        if redis_url:
            res = {"config": asdict(ScenarioConfig()), "dev_redis_real": _suite(DEV_SEEDS, redis_url, work)}
            res["dev_redis_real"]["backend"] = "redis"
            _write_json(out / "correctness_redis.json", res)
            write_report(out)
            return res
        if quick:
            res = {"config": asdict(ScenarioConfig()), "dev_sqlite": _suite((0, 1), "sqlite", work)}
            _write_json(out / "correctness.json", res)
            return res
        res = {"config": asdict(ScenarioConfig()),
               "dev_sqlite": _suite(DEV_SEEDS, "sqlite", work),
               "dev_redis_protocol": _suite(DEV_SEEDS_REDIS, "fakeredis", work),
               "holdout_sqlite": _suite(HOLDOUT_SEEDS, "sqlite", work)}
    _write_json(out / "correctness.json", res)
    write_report(out)
    return res


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------

def load_grid(duration_s: float, redis_url: str | None):
    from .loadgen import Cell
    cells = []
    for subs in (1, 10, 100):
        for rate in (100, 1000):
            cells.append(Cell(subs, rate, duration_s, backend="sqlite"))
    cells += [Cell(500, 100, duration_s, backend="sqlite", sub_procs=8),
              Cell(1, 5000, duration_s, backend="sqlite"), Cell(10, 5000, duration_s, backend="sqlite"),
              Cell(100, 2000, duration_s, backend="sqlite", sub_procs=8)]
    for subs in (1, 10, 100):
        cells.append(Cell(subs, 1000, duration_s, backend="memory"))
    if redis_url:
        for subs in (1, 10, 100):
            cells.append(Cell(subs, 1000, duration_s, backend=redis_url))
    return cells


def run_load_bench(out: Path, smoke: bool = False, duration_s: float = 5.0, redis_url: str | None = None,
                   redis_label: str | None = None, add_redis_rows: bool = False) -> dict:
    """``add_redis_rows``: run only the Redis rows against ``redis_url`` and append them, labelled
    ``redis_label``, to the existing results/load.json (e.g. the same Redis with another
    persistence setting)."""
    from .loadgen import Cell, machine_info, run_catchup, run_cell
    out.mkdir(parents=True, exist_ok=True)
    started = time.strftime("%Y-%m-%d")
    with tempfile.TemporaryDirectory() as tmp:
        if smoke:
            cell = Cell(2, 200, 1.0, warmup_s=0.5, backend="memory", sub_procs=1)
            r = run_cell(cell, tmp)
            print(json.dumps(r, indent=1))
            if r["lost"] or r["dups_delivered"] or r["holes_seen"]:
                raise SystemExit("load smoke: messages lost or duplicated")
            return r
        if add_redis_rows:
            from .loadgen import Cell as _Cell
            res = json.loads((out / "load.json").read_text(encoding="utf-8"))
            for subs in (1, 10, 100):
                r = run_cell(_Cell(subs, 1000, duration_s, backend=redis_url), tmp)
                r["backend"] = redis_label or "redis"
                print(f"  {r['backend']} subs {subs}: p50 {r['p50_ms']} ms, p99 {r['p99_ms']} ms", flush=True)
                res["rows"].append(r)
            _write_json(out / "load.json", res)
            write_report(out)
            return res
        rows = []
        for cell in load_grid(duration_s, redis_url):
            r = run_cell(cell, tmp)
            if redis_label and cell.backend.startswith("redis"):
                r["backend"] = redis_label
            print(f"  {r['backend']:<6} subs {r['subscribers']:>4} rate {r['rate']:>6.0f}/s: "
                  f"p50 {r['p50_ms']} ms, p99 {r['p99_ms']} ms, lost {r['lost']}", flush=True)
            rows.append(r)
        catchup = run_catchup(tmp)
    res = {"date": started, "machine": machine_info(), "duration_s": duration_s, "rows": rows,
           "catchup": catchup}
    _write_json(out / "load.json", res)
    write_report(out)
    return res


# ---------------------------------------------------------------------------
# RESULTS.md
# ---------------------------------------------------------------------------

def _md_table(header: list[str], rows: list[list]) -> list[str]:
    lines = ["| " + " | ".join(header) + " |", "|" + "|".join("---" for _ in header) + "|"]
    lines += ["| " + " | ".join(str(c) for c in r) + " |" for r in rows]
    return lines


def render(out: Path) -> str:
    L = ["# Results", "",
         "Generated by `sigbroker bench` (correctness) and `sigbroker load` (latency). Synthetic signals only.", ""]
    cpath = out / "correctness.json"
    if cpath.exists():
        c = json.loads(cpath.read_text(encoding="utf-8"))
        cfg = c["config"]
        L += ["## Correctness under faults", "",
              f"Each scenario publishes {cfg['n_messages'] + cfg['stall_burst']:,} messages on two topics "
              f"(a {cfg['n_messages']}-message stream plus a {cfg['stall_burst']:,}-message burst during a "
              f"subscriber stall) with {cfg['n_faults']} seeded faults, to 6 subscribers.", ""]
        rows = []
        rpath = out / "correctness_redis.json"
        if rpath.exists():
            c = dict(c, **{k: v for k, v in json.loads(rpath.read_text(encoding="utf-8")).items() if k != "config"})
        suites = (("dev_sqlite", "development seeds, SQLite log"),
                  ("dev_redis_protocol", "development seeds, Redis Streams log (fakeredis, in CI)"),
                  ("dev_redis_real", "development seeds, Redis Streams log (Redis 7 in Docker, local only)"),
                  ("holdout_sqlite", "held-out seeds, SQLite log"))
        for key, label in suites:
            if key not in c:
                continue
            s = c[key]["summary"]
            strict = s["subscribers"]
            miss = sum(v["missing"] for k, v in strict.items() if k != "dash_skip")
            dup = sum(v["duplicates"] for k, v in strict.items() if k != "dash_skip")
            ooo = sum(v["out_of_order"] for k, v in strict.items() if k != "dash_skip")
            exact = sum(v["exact_streams"] for k, v in strict.items() if k != "dash_skip")
            n_streams = s["scenarios"] * 2 * 5
            sk = strict["dash_skip"]
            rows.append([label, f"{s['passed']}/{s['scenarios']}", f"{s['messages_published']:,}",
                         " ".join(f"{k} {v}" for k, v in s["faults"].items()),
                         f"{exact}/{n_streams}", miss, dup, ooo,
                         f"{sk['unaccounted']} / {sk['both']}", s["log_duplicate_msg_ids"]])
        L += _md_table(["seeds", "scenarios passed", "messages", "faults injected", "exact streams",
                        "missing", "duplicates", "out of order", "skip mode: unaccounted / both",
                        "duplicate msg_ids in log"], rows)
        L += ["", "Observed (timing-dependent, not checked by tests):", ""]
        orows = []
        for key, _ in suites:
            if key not in c:
                continue
            s = c[key]["summary"]
            sub = s["subscribers"]
            orows.append([key, sum(v["reconnects_observed"] for v in sub.values()),
                          sub["dash_resync"]["resyncs_observed"], s["broker_observed"]["slow_disconnects"],
                          s["broker_observed"]["dropped"], sub["dash_skip"]["in_reported_gaps_observed"],
                          sum(v.get("duplicates_dropped_by_sdk_observed", 0) for v in sub.values()),
                          s["broker_observed"]["publish_duplicates"], s["publisher_resent_observed"],
                          s["wall_s_observed"]])
        L += _md_table(["suite", "reconnects", "resyncs (dash_resync)", "slow-consumer disconnects",
                        "events dropped (drop_oldest)", "seqs in reported gaps (dash_skip)",
                        "duplicates dropped by SDK", "idempotent publish duplicates", "publisher re-sends",
                        "wall s"], orows)
        L.append("")
    lpath = out / "load.json"
    if lpath.exists():
        lo = json.loads(lpath.read_text(encoding="utf-8"))
        m = lo["machine"]
        L += ["## Load (this machine only)", "",
              f"{lo['date']}, {m['cpu']}, {m['logical_cpus']} logical CPUs, {m['os']}, Python {m['python']}. "
              f"Broker, publisher and subscriber processes share the machine. {lo['duration_s']:.0f} s measured "
              "per row after 1 s of warm-up.", ""]
        rows = [[r["backend"], r["subscribers"], f"{r['rate']:.0f}", f"{r['achieved_rate']:.0f}",
                 f"{r['deliveries_per_s_offered']:,.0f}", r["p50_ms"], r["p95_ms"], r["p99_ms"], r["max_ms"],
                 r["lost"], r["dups_delivered"], r["reconnects"], r["slow_disconnects"]] for r in lo["rows"]]
        L += _md_table(["log", "subscribers", "target msg/s", "sent msg/s", "deliveries/s", "p50 ms", "p95 ms",
                        "p99 ms", "max ms", "lost", "duplicates", "reconnects", "slow disconnects"], rows)
        cu = lo.get("catchup")
        if cu:
            L += ["", "## Catch-up after an outage", "",
                  f"One subscriber offline for {cu['offline_s']:.0f} s while {cu['rate']:.0f} msg/s are published "
                  f"({cu['backend']} log): it missed {cu['missed']:,} messages, replayed them in "
                  f"{cu['catchup_ms']:.0f} ms after reconnecting, and ended with {cu['lost']} lost and "
                  f"{cu['duplicates']} duplicates over {cu['received']:,} messages."]
        L.append("")
    return "\n".join(L)


def write_report(out: Path) -> None:
    (out / "RESULTS.md").write_text(render(out), encoding="utf-8", newline="\n")

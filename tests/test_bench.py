"""The committed results: correctness reruns and must match; load numbers are machine-specific,
so only their zero-loss fields are checked, plus a tiny live load run."""
import json
import math
from pathlib import Path

import pytest

from sigbroker.bench import render, run_correctness, run_load_bench

ROOT = Path(__file__).resolve().parents[1]
RESULTS = ROOT / "results"


def _diff(a, b, path="$"):
    """Differences between two JSON trees. Timing-dependent fields (keys ending in
    ``_observed``) are skipped."""
    if isinstance(a, dict) and isinstance(b, dict):
        out = []
        keys = {k for k in a.keys() | b.keys() if not k.endswith("_observed")}
        for k in sorted(keys):
            if k not in a or k not in b:
                out.append(f"{path}.{k}: missing")
            else:
                out += _diff(a[k], b[k], f"{path}.{k}")
        return out
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return [f"{path}: length {len(a)} != {len(b)}"]
        return [d for i, (x, y) in enumerate(zip(a, b)) for d in _diff(x, y, f"{path}[{i}]")]
    if isinstance(a, float) or isinstance(b, float):
        ok = isinstance(a, (int, float)) and isinstance(b, (int, float)) and math.isclose(a, b, rel_tol=1e-9)
        return [] if ok else [f"{path}: {a} != {b}"]
    return [] if a == b else [f"{path}: {a!r} != {b!r}"]


def test_committed_correctness_has_no_loss_or_duplicates():
    c = json.loads((RESULTS / "correctness.json").read_text(encoding="utf-8"))
    for suite in ("dev_sqlite", "dev_redis_protocol", "holdout_sqlite"):
        s = c[suite]["summary"]
        assert s["passed"] == s["scenarios"] == len(c[suite]["seeds"])
        assert s["log_duplicate_msg_ids"] == 0 and s["log_missing_msg_ids"] == 0
        for name, v in s["subscribers"].items():
            if name == "dash_skip":
                assert v["unaccounted"] == v["both"] == v["duplicates"] == v["out_of_order"] == 0
            else:
                assert v["missing"] == v["duplicates"] == v["out_of_order"] == 0
    assert set(c["holdout_sqlite"]["seeds"]).isdisjoint(c["dev_sqlite"]["seeds"])


def test_committed_real_redis_run_has_no_loss_or_duplicates():
    path = RESULTS / "correctness_redis.json"
    if not path.exists():
        pytest.skip("no real-Redis run committed")
    s = json.loads(path.read_text(encoding="utf-8"))["dev_redis_real"]["summary"]
    assert s["passed"] == s["scenarios"] and s["log_duplicate_msg_ids"] == 0


def test_committed_load_has_no_loss_or_duplicates():
    lo = json.loads((RESULTS / "load.json").read_text(encoding="utf-8"))
    assert lo["rows"]
    for r in lo["rows"]:
        assert r["lost"] == 0 and r["dups_delivered"] == 0 and r["holes_seen"] == 0, r
    assert lo["catchup"]["lost"] == 0 and lo["catchup"]["duplicates"] == 0


def test_results_md_is_rendered_from_the_committed_json():
    assert (RESULTS / "RESULTS.md").read_text(encoding="utf-8") == render(RESULTS)


def test_load_smoke(tmp_path):
    r = run_load_bench(tmp_path, smoke=True)
    assert r["published"] > 0 and r["lost"] == 0 and r["dups_delivered"] == 0
    assert r["latency_samples"] > 0 and r["p50_ms"] > 0


@pytest.fixture(scope="module")
def full_run(tmp_path_factory):
    out = tmp_path_factory.mktemp("full")
    run_correctness(out)
    return out


@pytest.mark.slow
def test_committed_correctness_reproduces(full_run):
    fresh = json.loads((full_run / "correctness.json").read_text(encoding="utf-8"))
    committed = json.loads((RESULTS / "correctness.json").read_text(encoding="utf-8"))
    diffs = _diff(fresh, committed)
    assert not diffs, "\n".join(diffs[:20])

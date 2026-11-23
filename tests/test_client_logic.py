"""Pure client-side logic: back-off, cursor stores and the sequence tracker.

The property tests drive SeqTracker with a model of an unreliable connection: each
session starts a replay from wherever the server thinks the client is (sometimes too
early, which causes duplicates), and loses, repeats and drops events at random. The
application must still see 1..N exactly once, in order (resync mode), or see every seq
either delivered once or reported in a gap, never both (skip mode).
"""
import json
import random

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from sigbroker.client import (DELIVER, DUPLICATE, GAP, STALE, Backoff, FileCursorStore, MemoryCursorStore,
                              SeqTracker)


def test_backoff_full_jitter_bounds_and_cap():
    b = Backoff(base_s=0.1, cap_s=2.0, rng=random.Random(0))
    for attempt in range(12):
        ceiling = min(2.0, 0.1 * 2 ** attempt)
        assert b.ceiling(attempt) == pytest.approx(ceiling)
        draws = [b.delay(attempt) for _ in range(500)]
        assert 0 <= min(draws) and max(draws) <= ceiling
    assert b.ceiling(10_000) == 2.0  # no overflow


def test_backoff_spreads_a_crowd():
    """500 clients reconnecting after a restart, attempt 4: full jitter spreads them out."""
    rngs = [random.Random(i) for i in range(500)]
    delays = sorted(Backoff(0.1, 5.0, r).delay(4) for r in rngs)
    buckets = [sum(1 for d in delays if k * 0.16 <= d < (k + 1) * 0.16) for k in range(10)]
    assert max(buckets) < 90  # roughly uniform over [0, 1.6): about 50 per bucket


def test_backoff_is_reproducible_with_a_seed():
    a = [Backoff(rng=random.Random(5)).delay(i) for i in range(6)]
    b = [Backoff(rng=random.Random(5)).delay(i) for i in range(6)]
    assert a == b


def test_file_cursor_store_round_trip_and_rate_limit(tmp_path):
    p = tmp_path / "cursor.json"
    s = FileCursorStore(str(p), min_interval_s=3600)
    s.save("a", "ep1", 1)          # first save writes
    s.save("a", "ep1", 2)          # rate-limited, kept in memory
    assert json.loads(p.read_text())["a"] == ["ep1", 1]
    s.flush()
    assert json.loads(p.read_text())["a"] == ["ep1", 2]
    assert FileCursorStore(str(p)).load("a") == ("ep1", 2)
    assert FileCursorStore(str(tmp_path / "none.json")).load("a") is None
    m = MemoryCursorStore()
    m.save("t", None, 3)
    assert m.load("t") == (None, 3)


def test_tracker_basic_cases():
    t = SeqTracker("resync")
    assert t.on_subscribed("x", "ep", resume_from=1) == []
    assert t.on_event("x", 1) == (DELIVER, None)
    assert t.on_event("x", 1) == (DUPLICATE, None)
    action, gap = t.on_event("x", 4)
    assert action == GAP and (gap.from_seq, gap.to_seq) == (2, 3)
    assert t.on_event("x", 5) == (STALE, None)  # waiting for the replay
    t.on_subscribed("x", "ep", resume_from=2)
    assert t.on_event("x", 2) == (DELIVER, None)


def test_tracker_live_start_takes_position_from_subscribed():
    t = SeqTracker()
    t.on_subscribed("x", "ep", resume_from=41)
    assert t.on_event("x", 41)[0] == DELIVER


def test_tracker_reports_epoch_reset_and_retention():
    t = SeqTracker()
    t.on_subscribed("x", "ep1", 1)
    for s in (1, 2, 3):
        t.on_event("x", s)
    gaps = t.on_subscribed("x", "ep2", resume_from=1)
    assert [g.reason for g in gaps] == ["epoch_reset"]
    assert t.on_event("x", 1)[0] == DELIVER
    resync, rec = t.on_gap_frame("x", 2, 9, "retention")
    assert not resync and (rec.from_seq, rec.to_seq, rec.reason) == (2, 9, "retention")
    assert t.on_event("x", 10)[0] == DELIVER


def test_tracker_slow_consumer_gap_frame_modes():
    r = SeqTracker("resync")
    r.on_subscribed("x", "e", 1)
    r.on_event("x", 1)
    assert r.on_gap_frame("x", 2, 5, "slow_consumer") == (True, None)
    assert r.on_gap_frame("x", 6, 7, "slow_consumer") == (False, None)  # already re-syncing
    s = SeqTracker("skip")
    s.on_subscribed("x", "e", 1)
    s.on_event("x", 1)
    resync, rec = s.on_gap_frame("x", 2, 5, "slow_consumer")
    assert not resync and (rec.from_seq, rec.to_seq) == (2, 5)
    assert s.on_event("x", 6)[0] == DELIVER


# ---------------------------------------------------------------------------
# property tests
# ---------------------------------------------------------------------------

session_faults = st.lists(
    st.tuples(st.integers(0, 5),                 # rewind: server replays from last_seq - rewind
              st.floats(0, 0.3),                 # probability an event is lost on the wire
              st.floats(0, 0.2),                 # probability an event is sent twice
              st.integers(1, 40)),               # events before the connection drops
    min_size=1, max_size=40)


def _simulate(n, sessions, mode):
    """Run the tracker against a lossy model of the broker; return (delivered, gaps)."""
    t = SeqTracker(mode)
    delivered, gaps = [], []
    for rewind, p_loss, p_dup, length in sessions + [(0, 0.0, 0.0, 10 ** 6)]:  # last session is clean
        last = t.cursor("x").last_seq or 0
        start = max(0, last - rewind)
        t.on_subscribed("x", "ep", resume_from=start + 1)
        rng = random.Random(hash((rewind, p_loss, p_dup, length, last)))
        sent = 0
        seq = start
        while seq < n and sent < length:
            seq += 1
            if rng.random() < p_loss:
                continue
            copies = 2 if rng.random() < p_dup else 1
            stop = False
            for _ in range(copies):
                action, gap = t.on_event("x", seq)
                sent += 1
                if action == DELIVER:
                    delivered.append(seq)
                    if gap is not None:
                        gaps.append(gap)
                elif action == GAP:
                    stop = True  # SDK sends a re-subscribe; model it as a new session
                    break
            if stop:
                break
        if (t.cursor("x").last_seq or 0) >= n:
            break
    return delivered, gaps


@settings(max_examples=300, deadline=None)
@given(n=st.integers(1, 300), sessions=session_faults)
def test_property_resync_mode_delivers_exactly_once_in_order(n, sessions):
    delivered, gaps = _simulate(n, sessions, "resync")
    assert delivered == list(range(1, n + 1))
    assert gaps == []


@settings(max_examples=300, deadline=None)
@given(n=st.integers(1, 300), sessions=session_faults)
def test_property_skip_mode_accounts_for_every_seq(n, sessions):
    delivered, gaps = _simulate(n, sessions, "skip")
    covered = [s for g in gaps for s in range(g.from_seq, g.to_seq + 1)]
    assert delivered == sorted(set(delivered))                  # in order, no duplicate
    assert not set(delivered) & set(covered)                     # never both
    assert sorted(delivered + covered) == list(range(1, n + 1))  # nothing unaccounted


def test_corrupt_cursor_file_is_a_clear_error(tmp_path):
    p = tmp_path / "cursor.json"
    for text in ("{not json", '{"a": "x"}', '{"a": ["ep"]}', '[1, 2]'):
        p.write_text(text)
        with pytest.raises(ValueError, match="corrupt"):
            FileCursorStore(str(p))

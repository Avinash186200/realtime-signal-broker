"""Synthetic model signals: AR(1) scores per (model, instrument), alerts on large moves."""
from __future__ import annotations

import random
from datetime import datetime, timedelta, timezone

from .schema import AlertSignal, ScoreSignal

MODELS = ("carry", "momentum", "value")
INSTRUMENTS = ("UST2Y", "UST10Y", "BUND10Y", "EURUSD", "USDJPY", "SPX", "WTI", "GOLD")


class SignalGenerator:
    """Deterministic for a given seed. Scores follow z_t = 0.95 z_{t-1} + 0.31 e_t (unit
    variance); an alert is emitted when |z| first crosses 2."""

    def __init__(self, seed: int = 0, start: datetime | None = None, step_ms: int = 100):
        self.rng = random.Random(seed)
        self.t = start or datetime(2026, 1, 5, 14, 30, tzinfo=timezone.utc)
        self.step = timedelta(milliseconds=step_ms)
        self.state = {(m, i): 0.0 for m in MODELS for i in INSTRUMENTS}
        self.keys = list(self.state)

    def next(self):
        self.t += self.step
        key = self.keys[self.rng.randrange(len(self.keys))]
        prev = self.state[key]
        z = 0.95 * prev + 0.31 * self.rng.gauss(0.0, 1.0)
        self.state[key] = z
        model, inst = key
        if abs(z) >= 2.0 > abs(prev):
            return AlertSignal(model=model, instrument=inst, severity="warning" if abs(z) < 3 else "critical",
                               message=f"{model} score for {inst} crossed {z:+.2f}", as_of=self.t)
        return ScoreSignal(model=model, instrument=inst, value=round(z, 6), as_of=self.t, horizon="1d")

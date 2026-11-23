"""Prometheus metrics, one registry per broker (so tests can run several brokers)."""
from __future__ import annotations

from typing import TYPE_CHECKING

from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, generate_latest
from prometheus_client.core import GaugeMetricFamily

if TYPE_CHECKING:  # pragma: no cover
    from .broker import Broker

LATENCY_BUCKETS = (0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1,
                   0.25, 0.5, 1.0, 2.5)


class _LagCollector:
    """Per-topic subscriber lag and queue depth, computed at scrape time."""

    def __init__(self, broker: "Broker"):
        self.broker = broker

    def collect(self):
        lag_max = GaugeMetricFamily("sigbroker_subscriber_lag_max",
                                    "Largest (head seq - last seq written to the socket) over subscribers",
                                    labels=["topic"])
        queue_max = GaugeMetricFamily("sigbroker_outbox_events_max",
                                      "Largest number of queued events over subscribers", labels=["topic"])
        subs = GaugeMetricFamily("sigbroker_subscriptions", "Live subscriptions", labels=["topic"])
        for topic, rows in self.broker.lag_snapshot().items():
            lag_max.add_metric([topic], max((r[0] for r in rows), default=0))
            queue_max.add_metric([topic], max((r[1] for r in rows), default=0))
            subs.add_metric([topic], len(rows))
        yield lag_max
        yield queue_max
        yield subs


class BrokerMetrics:
    def __init__(self, broker: "Broker | None" = None):
        r = self.registry = CollectorRegistry()
        self.connections = Gauge("sigbroker_connections", "Open authenticated connections", ["role"], registry=r)
        self.connects = Counter("sigbroker_connects", "Authenticated connections", ["role"], registry=r)
        self.auth_failures = Counter("sigbroker_auth_failures", "Rejected hello frames", registry=r)
        self.resumes = Counter("sigbroker_resumes", "Subscriptions that resumed from a last_seq", ["topic"],
                               registry=r)
        self.published = Counter("sigbroker_published", "Messages appended to the log", ["topic"], registry=r)
        self.publish_duplicates = Counter("sigbroker_publish_duplicates",
                                          "Publishes ignored because the msg_id was already in the log",
                                          ["topic"], registry=r)
        self.delivered = Counter("sigbroker_delivered", "Event frames written to sockets", ["topic"], registry=r)
        self.replayed = Counter("sigbroker_replayed", "Event frames read from the log for a catch-up",
                                ["topic"], registry=r)
        self.dropped = Counter("sigbroker_dropped", "Events dropped from a subscriber queue", ["topic", "reason"],
                               registry=r)
        self.slow_disconnects = Counter("sigbroker_slow_disconnects",
                                        "Connections closed by the disconnect slow-consumer policy", registry=r)
        self.protocol_errors = Counter("sigbroker_protocol_errors", "Rejected frames", ["code"], registry=r)
        self.append_to_send = Histogram("sigbroker_append_to_send_seconds",
                                        "Time from log append to socket write, live events", buckets=LATENCY_BUCKETS,
                                        registry=r)
        if broker is not None:
            r.register(_LagCollector(broker))

    def render(self) -> bytes:
        return generate_latest(self.registry)


def counter_value(registry: CollectorRegistry, name: str, labels: dict | None = None) -> float:
    """Read one sample (summing over labels not given). Handy in tests and the benchmark."""
    total = 0.0
    for metric in registry.collect():
        for s in metric.samples:
            if s.name != name:
                continue
            if labels and any(s.labels.get(k) != v for k, v in labels.items()):
                continue
            total += s.value
    return total

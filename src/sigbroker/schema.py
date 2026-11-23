"""Wire protocol: typed messages, signal payloads and schema versioning.

Every frame is one JSON object with a ``type`` field and a protocol version ``v``.
Rules (see docs/protocol.md):

- ``v`` is the major protocol version. A peer rejects a frame whose major version it does
  not support (error ``unsupported_version``).
- Within a major version, changes are additive only. Unknown fields are ignored, so an
  older client keeps working when the broker adds a field.
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Annotated, Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, TypeAdapter, field_validator

PROTOCOL_VERSION = 1
SUPPORTED_VERSIONS = frozenset({1})

TOPIC_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9_.\-]{0,127}$"


class _Model(BaseModel):
    # Additive changes within a major version: ignore fields we do not know.
    model_config = ConfigDict(extra="ignore", frozen=True)


# ---------------------------------------------------------------------------
# Signal payloads (what the research team publishes)
# ---------------------------------------------------------------------------

class ScoreSignal(_Model):
    kind: Literal["score"] = "score"
    model: str = Field(min_length=1, max_length=64)
    instrument: str = Field(min_length=1, max_length=32)
    value: float
    as_of: datetime
    horizon: str | None = None
    meta: dict[str, Any] = Field(default_factory=dict)

    @field_validator("value")
    @classmethod
    def _finite(cls, v: float) -> float:
        if v != v or v in (float("inf"), float("-inf")):
            raise ValueError("value must be finite")
        return v


class AlertSignal(_Model):
    kind: Literal["alert"] = "alert"
    model: str = Field(min_length=1, max_length=64)
    instrument: str = Field(min_length=1, max_length=32)
    severity: Literal["info", "warning", "critical"]
    message: str = Field(max_length=1024)
    as_of: datetime
    meta: dict[str, Any] = Field(default_factory=dict)


Signal = Annotated[Union[ScoreSignal, AlertSignal], Field(discriminator="kind")]
SIGNAL_ADAPTER: TypeAdapter = TypeAdapter(Signal)


# ---------------------------------------------------------------------------
# Client -> broker
# ---------------------------------------------------------------------------

class Hello(_Model):
    type: Literal["hello"] = "hello"
    v: int = PROTOCOL_VERSION
    token: str
    client_id: str | None = Field(default=None, max_length=64)
    # Optional request for a slow-consumer policy; the broker may refuse it.
    slow_policy: Literal["disconnect", "drop_oldest"] | None = None
    # The client understands "batch" frames (added in 1.x; old clients never get them).
    batch: bool = False


class Subscribe(_Model):
    """Subscribe to a topic, or re-sync an existing subscription.

    ``last_seq``: the last sequence the client has. ``None`` means "only new messages",
    ``0`` means "from the oldest retained message". The broker replays every retained
    message with ``seq > last_seq`` and then streams live ones.
    """
    type: Literal["subscribe"] = "subscribe"
    v: int = PROTOCOL_VERSION
    topic: str = Field(pattern=TOPIC_PATTERN)
    last_seq: int | None = Field(default=None, ge=0)
    epoch: str | None = None


class Unsubscribe(_Model):
    type: Literal["unsubscribe"] = "unsubscribe"
    v: int = PROTOCOL_VERSION
    topic: str = Field(pattern=TOPIC_PATTERN)


class Publish(_Model):
    type: Literal["publish"] = "publish"
    v: int = PROTOCOL_VERSION
    topic: str = Field(pattern=TOPIC_PATTERN)
    msg_id: str = Field(min_length=1, max_length=128)  # idempotency key
    payload: dict[str, Any]
    sent_ns: int | None = None  # optional publisher clock stamp, echoed to subscribers


ClientFrame = Annotated[Union[Hello, Subscribe, Unsubscribe, Publish], Field(discriminator="type")]
CLIENT_ADAPTER: TypeAdapter = TypeAdapter(ClientFrame)


# ---------------------------------------------------------------------------
# Broker -> client
# ---------------------------------------------------------------------------

class Welcome(_Model):
    type: Literal["welcome"] = "welcome"
    v: int = PROTOCOL_VERSION
    session: str
    role: Literal["subscriber", "publisher"]
    heartbeat_s: float
    slow_policy: Literal["disconnect", "drop_oldest"]
    max_queue: int


class Subscribed(_Model):
    type: Literal["subscribed"] = "subscribed"
    v: int = PROTOCOL_VERSION
    topic: str
    epoch: str
    head_seq: int
    first_seq: int
    resume_from: int  # first seq the broker will send


class Event(_Model):
    type: Literal["event"] = "event"
    v: int = PROTOCOL_VERSION
    topic: str
    seq: int
    msg_id: str
    published_ns: int  # broker wall clock at append (time.time_ns)
    sent_ns: int | None = None
    payload: dict[str, Any]


class Batch(_Model):
    """Several consecutive events in one WebSocket frame. Sent only to clients that said
    ``batch: true`` in hello, and only when more than one event is already queued, so it
    adds no waiting: it just saves a socket write per event under load."""
    type: Literal["batch"] = "batch"
    v: int = PROTOCOL_VERSION
    events: list[Event]


class Gap(_Model):
    type: Literal["gap"] = "gap"
    v: int = PROTOCOL_VERSION
    topic: str
    from_seq: int
    to_seq: int
    reason: Literal["slow_consumer", "retention"]


class Heartbeat(_Model):
    type: Literal["heartbeat"] = "heartbeat"
    v: int = PROTOCOL_VERSION
    ts_ns: int
    heads: dict[str, int] = Field(default_factory=dict)


class Ack(_Model):
    type: Literal["ack"] = "ack"
    v: int = PROTOCOL_VERSION
    topic: str
    msg_id: str
    seq: int
    duplicate: bool


class Error(_Model):
    type: Literal["error"] = "error"
    v: int = PROTOCOL_VERSION
    code: str
    message: str
    topic: str | None = None
    msg_id: str | None = None


class Bye(_Model):
    type: Literal["bye"] = "bye"
    v: int = PROTOCOL_VERSION
    reason: str
    reconnect_after_ms: int = 0


ServerFrame = Annotated[
    Union[Welcome, Subscribed, Event, Batch, Gap, Heartbeat, Ack, Error, Bye], Field(discriminator="type")
]
SERVER_ADAPTER: TypeAdapter = TypeAdapter(ServerFrame)


class ProtocolError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


def _check_version(raw: Any) -> None:
    if not isinstance(raw, dict):
        raise ProtocolError("bad_frame", "frame must be a JSON object")
    v = raw.get("v", PROTOCOL_VERSION)
    if v not in SUPPORTED_VERSIONS:
        raise ProtocolError("unsupported_version", f"protocol version {v!r} not supported")


def decode_client(text: str | bytes):
    try:
        raw = json.loads(text)
    except (ValueError, TypeError, RecursionError) as e:  # RecursionError: absurdly nested JSON
        raise ProtocolError("bad_frame", f"not JSON: {type(e).__name__}") from None
    _check_version(raw)
    try:
        return CLIENT_ADAPTER.validate_python(raw)
    except (Exception, RecursionError) as e:  # pydantic.ValidationError
        raise ProtocolError("bad_frame", str(e).splitlines()[0][:200]) from None


def decode_server(text: str | bytes):
    try:
        raw = json.loads(text)
    except (ValueError, TypeError, RecursionError) as e:
        raise ProtocolError("bad_frame", f"not JSON: {type(e).__name__}") from None
    _check_version(raw)
    try:
        return SERVER_ADAPTER.validate_python(raw)
    except (Exception, RecursionError) as e:
        raise ProtocolError("bad_frame", str(e).splitlines()[0][:200]) from None


def encode(frame: BaseModel) -> str:
    return frame.model_dump_json(exclude_none=True)


def validate_signal(payload: dict[str, Any]):
    """Validate a published payload; returns the typed signal or raises ProtocolError."""
    try:
        return SIGNAL_ADAPTER.validate_python(payload)
    except Exception as e:
        raise ProtocolError("invalid_payload", str(e).splitlines()[0]) from None

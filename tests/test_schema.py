import json
from datetime import datetime, timezone

import pytest

from sigbroker.schema import (PROTOCOL_VERSION, Event, ProtocolError, Publish, ScoreSignal, decode_client,
                              decode_server, encode, validate_signal)

T = datetime(2026, 1, 5, tzinfo=timezone.utc)


def test_client_frames_round_trip():
    f = Publish(topic="signals.rates", msg_id="m1", payload={"kind": "score"})
    back = decode_client(encode(f))
    assert back == f and back.v == PROTOCOL_VERSION


def test_unknown_fields_are_ignored_within_a_major_version():
    raw = {"type": "event", "v": 1, "topic": "a", "seq": 3, "msg_id": "x", "published_ns": 1,
           "payload": {}, "added_in_1_1": "ignored"}
    ev = decode_server(json.dumps(raw))
    assert isinstance(ev, Event) and ev.seq == 3


@pytest.mark.parametrize("v", [0, 2, "1"])
def test_unsupported_major_version_is_rejected(v):
    with pytest.raises(ProtocolError) as e:
        decode_client(json.dumps({"type": "subscribe", "v": v, "topic": "a"}))
    assert e.value.code == "unsupported_version"


@pytest.mark.parametrize("text", ["not json", "[]", '{"type": "nope"}', '{"type": "subscribe", "topic": "bad topic"}',
                                  '{"type": "subscribe", "topic": "a", "last_seq": -1}'])
def test_bad_frames(text):
    with pytest.raises(ProtocolError) as e:
        decode_client(text)
    assert e.value.code == "bad_frame"


def test_signal_validation():
    s = ScoreSignal(model="carry", instrument="UST10Y", value=1.5, as_of=T)
    assert validate_signal(s.model_dump(mode="json")).value == 1.5
    alert = {"kind": "alert", "model": "m", "instrument": "X", "severity": "critical", "message": "hi",
             "as_of": T.isoformat()}
    assert validate_signal(alert).severity == "critical"
    for bad in ({"kind": "score", "model": "m", "instrument": "X", "value": float("nan"), "as_of": T.isoformat()},
                {"kind": "weather"}, {"kind": "alert", "model": "m", "instrument": "X", "severity": "panic",
                                      "message": "", "as_of": T.isoformat()}):
        with pytest.raises(ProtocolError) as e:
            validate_signal(bad)
        assert e.value.code == "invalid_payload"


def test_deeply_nested_json_is_a_bad_frame_not_a_crash():
    deep = "[" * 100_000 + "]" * 100_000
    with pytest.raises(ProtocolError) as e:
        decode_client(deep)
    assert e.value.code == "bad_frame"
    with pytest.raises(ProtocolError):
        decode_server(deep)
    with pytest.raises(ProtocolError):
        decode_server('{"type": "event", "v": 1}')

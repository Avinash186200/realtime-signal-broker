"""Broker behaviour over real localhost WebSockets: auth, publish, replay, heartbeats,
metrics, versioning, retention, epochs, shutdown and TLS."""
import asyncio
import json
import ssl
import urllib.request
from datetime import datetime, timezone

import pytest
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed

from conftest import run
from sigbroker import Broker, BrokerConfig, Publisher, Subscriber, SubscriberError
from sigbroker.backends import MemoryBackend
from sigbroker.metrics import counter_value
from sigbroker.publisher import PublishError
from sigbroker.schema import ScoreSignal

T = datetime(2026, 1, 5, tzinfo=timezone.utc)


def sig(i=0):
    return ScoreSignal(model="carry", instrument="UST10Y", value=float(i), as_of=T).model_dump(mode="json")


async def raw_session(url, token, **hello):
    ws = await connect(url, compression=None)
    await ws.send(json.dumps({"type": "hello", "token": token, **hello}))
    return ws, json.loads(await ws.recv())


async def frames_until(ws, kind, n=1, timeout=5):
    out = []
    async with asyncio.timeout(timeout):
        while len([f for f in out if f["type"] == kind]) < n:
            out.append(json.loads(await ws.recv()))
    return out


def test_bad_token_is_closed_with_4001(secret, tokens):
    async def go():
        async with Broker(secret) as b:
            ws = await connect(b.url)
            await ws.send(json.dumps({"type": "hello", "token": "not-a-token"}))
            err = json.loads(await ws.recv())
            assert err["type"] == "error" and err["code"] == "unauthorized"
            with pytest.raises(ConnectionClosed) as e:
                await ws.recv()
            assert e.value.rcvd.code == 4001
            # first frame must be hello
            ws = await connect(b.url)
            await ws.send(json.dumps({"type": "subscribe", "topic": "signals.a"}))
            assert json.loads(await ws.recv())["code"] == "unauthorized"
            assert counter_value(b.metrics.registry, "sigbroker_auth_failures_total") == 2
            # the SDK treats it as fatal instead of retrying forever
            sub = Subscriber(b.url, "bad", ["signals.a"])
            with pytest.raises(SubscriberError):
                async for _ in sub:
                    pass
            await sub.close()
    run(go())


def test_roles_and_topic_acl(secret, tokens):
    async def go():
        async with Broker(secret) as b:
            ws, welcome = await raw_session(b.url, tokens("subscriber", ["signals.rates"]))
            assert welcome["type"] == "welcome" and welcome["role"] == "subscriber"
            await ws.send(json.dumps({"type": "subscribe", "topic": "signals.fx", "last_seq": 0}))
            await ws.send(json.dumps({"type": "publish", "topic": "signals.rates", "msg_id": "x", "payload": sig()}))
            f = await frames_until(ws, "error", 2)
            assert [x["code"] for x in f if x["type"] == "error"] == ["forbidden", "forbidden"]
            pub, _ = await raw_session(b.url, tokens("publisher", ["signals.rates"]))
            await pub.send(json.dumps({"type": "subscribe", "topic": "signals.rates"}))
            await pub.send(json.dumps({"type": "publish", "topic": "signals.fx", "msg_id": "x", "payload": sig()}))
            f = await frames_until(pub, "error", 2)
            assert [x["code"] for x in f if x["type"] == "error"] == ["forbidden", "forbidden"]
            await ws.close()
            await pub.close()
    run(go())


def test_publish_is_idempotent_and_validated(secret, tokens):
    async def go():
        async with Broker(secret) as b:
            async with Publisher(b.url, tokens("publisher")) as pub:
                a1 = await pub.publish("signals.rates", sig(1), msg_id="k1")
                a2 = await pub.publish("signals.rates", sig(2), msg_id="k2")
                again = await pub.publish("signals.rates", sig(99), msg_id="k1")
                assert (a1.seq, a2.seq) == (1, 2) and not a1.duplicate
                assert again.duplicate and again.seq == 1
                with pytest.raises(PublishError) as e:
                    await pub.publish("signals.rates", {"kind": "score", "value": "high"})
                assert e.value.code == "invalid_payload"
            reg = b.metrics.registry
            assert counter_value(reg, "sigbroker_published_total") == 2
            assert counter_value(reg, "sigbroker_publish_duplicates_total") == 1
            stored = await b.backend.read("signals.rates", 0, 10)
            assert [e.payload["value"] for e in stored] == [1.0, 2.0]
    run(go())


def test_reconnect_with_last_seq_gets_exactly_the_missed_messages(secret, tokens):
    async def go():
        async with Broker(secret) as b:
            async with Publisher(b.url, tokens("publisher")) as pub:
                for i in range(10):
                    await pub.publish("signals.rates", sig(i))
            ws, _ = await raw_session(b.url, tokens())
            await ws.send(json.dumps({"type": "subscribe", "topic": "signals.rates", "last_seq": 6,
                                      "epoch": b.backend.epoch}))
            f = await frames_until(ws, "event", 4)
            sub = [x for x in f if x["type"] == "subscribed"][0]
            assert sub["resume_from"] == 7 and sub["head_seq"] == 10
            assert [x["seq"] for x in f if x["type"] == "event"] == [7, 8, 9, 10]
            # live-only subscription sees only new messages
            ws2, _ = await raw_session(b.url, tokens())
            await ws2.send(json.dumps({"type": "subscribe", "topic": "signals.rates"}))
            await frames_until(ws2, "subscribed")
            async with Publisher(b.url, tokens("publisher")) as pub:
                await pub.publish("signals.rates", sig(10))
            f = await frames_until(ws2, "event")
            assert [x["seq"] for x in f if x["type"] == "event"] == [11]
            assert counter_value(b.metrics.registry, "sigbroker_resumes_total") == 1
            await ws.close()
            await ws2.close()
    run(go())


def test_seq_ahead_without_epoch_is_an_error_not_silence(secret, tokens):
    async def go():
        async with Broker(secret) as b:
            ws, _ = await raw_session(b.url, tokens())
            await ws.send(json.dumps({"type": "subscribe", "topic": "signals.rates", "last_seq": 50}))
            f = await frames_until(ws, "error")
            assert f[-1]["code"] == "seq_ahead"
            await ws.close()
    run(go())


def test_heartbeats_carry_heads(secret, tokens):
    async def go():
        async with Broker(secret, config=BrokerConfig(heartbeat_s=0.05)) as b:
            async with Publisher(b.url, tokens("publisher")) as pub:
                await pub.publish("signals.rates", sig())
            ws, welcome = await raw_session(b.url, tokens())
            assert welcome["heartbeat_s"] == 0.05
            await ws.send(json.dumps({"type": "subscribe", "topic": "signals.rates", "last_seq": 0}))
            f = await frames_until(ws, "heartbeat", 2)
            assert [x for x in f if x["type"] == "heartbeat"][-1]["heads"] == {"signals.rates": 1}
            await ws.close()
    run(go())


def test_client_detects_a_silent_connection(secret, tokens):
    """No frames for heartbeat_misses x heartbeat interval -> the SDK drops and reconnects."""
    async def go():
        async with Broker(secret, config=BrokerConfig(heartbeat_s=0.1)) as b:
            sub = Subscriber(b.url, tokens(), ["signals.rates"], heartbeat_misses=2)
            sub.start_background()
            async with asyncio.timeout(5):
                while not (b._sessions and sub.tracker.cursor("signals.rates").last_seq is not None):
                    await asyncio.sleep(0.01)
            for s in list(b._sessions):  # freeze the broker side: no more heartbeats
                for t in asyncio.all_tasks():
                    if t.get_name() == f"hb-{s.id}":
                        t.cancel()
            async with asyncio.timeout(5):
                while sub.stats.heartbeat_timeouts == 0 or sub.stats.connects < 2:
                    await asyncio.sleep(0.02)
            await sub.close()
    run(go())


def test_metrics_and_health_endpoints(secret, tokens):
    async def go():
        async with Broker(secret) as b:
            async with Publisher(b.url, tokens("publisher")) as pub:
                await pub.publish("signals.rates", sig())
            sub = Subscriber(b.url, tokens(), ["signals.rates"])
            async for _ in sub:
                break
            port = b.port

            def get(path):
                try:
                    r = urllib.request.urlopen(f"http://127.0.0.1:{port}{path}", timeout=5)
                    return r.status, r.read().decode()
                except urllib.error.HTTPError as e:
                    return e.code, ""
            status, text = await asyncio.to_thread(get, "/metrics")
            assert status == 200
            for name in ("sigbroker_published_total", "sigbroker_delivered_total", "sigbroker_dropped_total",
                         "sigbroker_subscriber_lag_max", "sigbroker_connections", "sigbroker_resumes_total",
                         "sigbroker_append_to_send_seconds_bucket"):
                assert name in text, name
            assert 'sigbroker_subscriber_lag_max{topic="signals.rates"} 0.0' in text
            assert (await asyncio.to_thread(get, "/healthz"))[0] == 200
            assert (await asyncio.to_thread(get, "/nope"))[0] == 404
            b.closing = True
            assert (await asyncio.to_thread(get, "/healthz"))[0] == 503
            b.closing = False
            await sub.close()
    run(go())


def test_unsupported_version_and_bad_frames_get_errors(secret, tokens):
    async def go():
        async with Broker(secret) as b:
            ws, _ = await raw_session(b.url, tokens())
            await ws.send(json.dumps({"type": "subscribe", "v": 2, "topic": "signals.rates"}))
            await ws.send("{not json")
            f = await frames_until(ws, "error", 2)
            assert [x["code"] for x in f if x["type"] == "error"] == ["unsupported_version", "bad_frame"]
            await ws.close()
    run(go())


def test_token_expiry_closes_the_session(secret, tokens):
    async def go():
        async with Broker(secret, config=BrokerConfig(heartbeat_s=0.05)) as b:
            ws, _ = await raw_session(b.url, tokens(ttl_s=-2))  # inside the 5 s leeway at connect
            with pytest.raises(ConnectionClosed) as e:
                async with asyncio.timeout(5):
                    while True:
                        await ws.recv()
            assert e.value.rcvd.code == 4003
    run(go())


def test_retention_gap_is_reported_to_the_application(secret, tokens):
    async def go():
        async with Broker(secret, backend=MemoryBackend(retention=5)) as b:
            async with Publisher(b.url, tokens("publisher")) as pub:
                for i in range(3):
                    await pub.publish("signals.rates", sig(i))
                gaps = []
                from sigbroker.client import MemoryCursorStore
                store = MemoryCursorStore()
                store.save("signals.rates", b.backend.epoch, 2)
                sub = Subscriber(b.url, tokens(), ["signals.rates"], cursor_store=store, on_gap=gaps.append)
                for i in range(3, 12):  # seqs 4..12; the log keeps the last 5 (8..12)
                    await pub.publish("signals.rates", sig(i))
            got = []
            async for d in sub:
                got.append(d.seq)
                if d.seq == 12:
                    break
            await sub.close()
            assert got == [8, 9, 10, 11, 12]
            assert [(g.from_seq, g.to_seq, g.reason) for g in gaps] == [(3, 7, "retention")]
    run(go())


def test_memory_broker_restart_is_reported_as_epoch_reset(secret, tokens):
    async def go():
        b = await Broker(secret, config=BrokerConfig(port=0)).start()
        port = b.port
        async with Publisher(b.url, tokens("publisher")) as pub:
            for i in range(3):
                await pub.publish("signals.rates", sig(i))
        gaps = []
        sub = Subscriber(b.url, tokens(), ["signals.rates"], on_gap=gaps.append)
        got = []
        async for d in sub:
            got.append(d.seq)
            if d.seq == 3:
                break
        await b.abort()
        b2 = await Broker(secret, config=BrokerConfig(port=port)).start()  # empty log, new epoch
        async with Publisher(b2.url, tokens("publisher")) as pub:
            await pub.publish("signals.rates", sig(9))
        async for d in sub:
            got.append(d.seq)
            break
        await sub.close()
        await b2.stop()
        assert got == [1, 2, 3, 1]
        assert [g.reason for g in gaps] == ["epoch_reset"] and sub.stats.epoch_resets == 1
    run(go())


def test_graceful_shutdown_says_bye_and_refuses_new_work(secret, tokens):
    async def go():
        b = await Broker(secret).start()
        ws, _ = await raw_session(b.url, tokens())
        await ws.send(json.dumps({"type": "subscribe", "topic": "signals.rates", "last_seq": 0}))
        await frames_until(ws, "subscribed")
        stop = asyncio.create_task(b.stop(drain_timeout_s=2))
        f = await frames_until(ws, "bye")
        assert f[-1]["reason"] == "shutdown" and f[-1]["reconnect_after_ms"] > 0
        with pytest.raises(ConnectionClosed) as e:
            await ws.recv()
        assert e.value.rcvd.code == 1001
        await stop
        with pytest.raises(OSError):
            await connect(f"ws://127.0.0.1:{b.port}/ws", open_timeout=1)
    run(go())


def test_wss_with_a_runtime_certificate(secret, tokens):
    trustme = pytest.importorskip("trustme")

    async def go():
        ca = trustme.CA()
        server_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ca.issue_cert("127.0.0.1").configure_cert(server_ctx)
        client_ctx = ssl.create_default_context()
        ca.configure_trust(client_ctx)
        async with Broker(secret, config=BrokerConfig(ssl=server_ctx)) as b:
            assert b.url.startswith("wss://")
            async with Publisher(b.url, tokens("publisher"), ssl=client_ctx) as pub:
                await pub.publish("signals.rates", sig(1))
            sub = Subscriber(b.url, tokens(), ["signals.rates"], ssl=client_ctx)
            async for d in sub:
                assert d.seq == 1 and d.signal.value == 1.0
                break
            await sub.close()
    run(go())


def test_too_many_topics_is_refused_and_idle_names_are_evicted(secret, tokens):
    async def go():
        async with Broker(secret, config=BrokerConfig(max_topics=3)) as b:
            ws, _ = await raw_session(b.url, tokens())
            for t in ("signals.a", "signals.b", "signals.c"):
                await ws.send(json.dumps({"type": "subscribe", "topic": t}))
            await frames_until(ws, "subscribed", 3)
            await ws.send(json.dumps({"type": "subscribe", "topic": "signals.d"}))
            f = await frames_until(ws, "error")
            assert f[-1]["code"] == "too_many_topics"
            await ws.send(json.dumps({"type": "unsubscribe", "topic": "signals.a"}))
            await ws.send(json.dumps({"type": "subscribe", "topic": "signals.e"}))  # a was idle: evicted
            f = await frames_until(ws, "subscribed")
            assert f[-1]["topic"] == "signals.e" and "signals.a" not in b._topics
            await ws.close()
    run(go())


def test_duplicate_append_ahead_of_head_resyncs_the_broker(secret, tokens):
    """Another writer appended to the shared log; a duplicate publish reveals the newer head."""
    async def go():
        async with Broker(secret) as b:
            sub = Subscriber(b.url, tokens(), ["signals.rates"])
            async with Publisher(b.url, tokens("publisher")) as pub:
                await pub.publish("signals.rates", sig(1), msg_id="m1")
                await b.backend.append("signals.rates", "other", sig(2), 0)  # behind the broker's back
                ack = await pub.publish("signals.rates", sig(2), msg_id="other")
                assert ack.duplicate and ack.seq == 2
                assert b._topics["signals.rates"].head == 2
            got = []
            async for d in sub:
                got.append(d.seq)
                if d.seq == 2:
                    break
            await sub.close()
            assert got == [1, 2]
    run(go())


def test_shutdown_refuses_new_subscriptions(secret, tokens):
    async def go():
        async with Broker(secret) as b:
            ws, _ = await raw_session(b.url, tokens())
            b.closing = True
            await ws.send(json.dumps({"type": "subscribe", "topic": "signals.rates"}))
            f = await frames_until(ws, "error")
            assert f[-1]["code"] == "shutting_down"
            b.closing = False
            await ws.close()
    run(go())

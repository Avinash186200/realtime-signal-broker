"""The WebSocket broker.

One asyncio process. Publishers append to the replay log (one lock per topic, so sequence
numbers are assigned in order); every subscription is a cursor over the log that pushes
into the subscriber's bounded outbox. Replay and live delivery are the same code path:
the cursor reads "everything after my last seq", from a small in-memory cache of recent
encoded frames when it can and from the backend when it has fallen further behind. The
publisher never waits for a subscriber.
"""
from __future__ import annotations

import asyncio
import logging
import socket
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from http import HTTPStatus

from websockets.asyncio.server import Server, ServerConnection, serve
from websockets.exceptions import ConnectionClosed

from .auth import AuthError, Claims, verify_token
from .backends import LogBackend, MemoryBackend
from .metrics import BrokerMetrics
from .outbox import Item, Outbox, SlowConsumer
from .schema import (Ack, Bye, Error, Event, Heartbeat, Hello, ProtocolError, Publish, Subscribe,
                     Subscribed, Unsubscribe, Welcome, decode_client, encode, validate_signal)

log = logging.getLogger("sigbroker.broker")

CLOSE_UNAUTHORIZED = 4001
CLOSE_TOKEN_EXPIRED = 4003
CLOSE_SLOW_CONSUMER = 4008
CLOSE_GOING_AWAY = 1001


@dataclass
class BrokerConfig:
    host: str = "127.0.0.1"
    port: int = 0
    heartbeat_s: float = 5.0
    max_queue: int = 1000            # events per subscriber outbox
    slow_policy: str = "disconnect"  # default policy; "drop_oldest" for latest-value consumers
    allow_policy_override: bool = True
    slow_grace_s: float = 1.0
    auth_timeout_s: float = 5.0
    read_batch: int = 500
    cache_size: int = 10_000         # recent encoded events kept per topic
    ping_interval_s: float | None = 20.0
    ping_timeout_s: float | None = 20.0
    max_frame_bytes: int = 64 * 1024
    max_subscriptions: int = 100
    max_batch: int = 256             # events per batch frame (clients that opt in)
    # Topics this process will track. Token patterns such as "signals.*" let a client name
    # any topic; without a cap, made-up names would grow state and metric labels forever.
    max_topics: int = 10_000
    drain_timeout_s: float = 5.0
    reconnect_hint_ms: int = 500
    ssl: object | None = None        # ssl.SSLContext for wss://
    # Per-connection kernel send buffer. None keeps the OS default, which on loopback can
    # hold over a thousand small frames before a stalled consumer backs up into the outbox.
    socket_send_buffer: int | None = None


class _TopicState:
    __slots__ = ("head", "loaded", "lock", "changed", "cache")

    def __init__(self):
        self.head = 0
        self.loaded = False
        self.lock = asyncio.Lock()
        self.changed = asyncio.Event()
        self.cache: deque[tuple[int, str, int]] = deque()  # (seq, frame, appended perf_counter_ns)

    def notify(self) -> None:
        old, self.changed = self.changed, asyncio.Event()
        old.set()


@dataclass(eq=False)
class _Subscription:
    topic: str
    cursor: int                 # last seq queued
    sent_seq: int = 0           # last seq written to the socket
    task: asyncio.Task | None = None


@dataclass(eq=False)
class _Session:
    broker: "Broker"
    ws: ServerConnection
    claims: Claims
    outbox: Outbox
    id: str = field(default_factory=lambda: uuid.uuid4().hex[:12])
    subs: dict[str, _Subscription] = field(default_factory=dict)
    closing: bool = False
    draining: bool = False
    batch: bool = False
    _close_task: asyncio.Task | None = None

    # -- frames from the client -------------------------------------------------------
    async def on_frame(self, raw) -> None:
        b = self.broker
        frame = None
        try:
            frame = decode_client(raw)
            if isinstance(frame, Publish):
                await self._on_publish(frame)
            elif isinstance(frame, Subscribe):
                await self._on_subscribe(frame)
            elif isinstance(frame, Unsubscribe):
                await self._stop_sub(frame.topic)
            elif isinstance(frame, Hello):
                raise ProtocolError("bad_frame", "already authenticated")
        except ProtocolError as e:
            b.metrics.protocol_errors.labels(e.code).inc()
            topic = getattr(frame, "topic", None)
            msg_id = getattr(frame, "msg_id", None)
            self.control(Error(code=e.code, message=str(e), topic=topic, msg_id=msg_id))

    async def _on_publish(self, f: Publish) -> None:
        if self.claims.role != "publisher":
            raise ProtocolError("forbidden", "token role cannot publish")
        if not self.claims.may_access(f.topic):
            raise ProtocolError("forbidden", f"token may not publish to {f.topic}")
        if self.broker.closing:
            raise ProtocolError("shutting_down", "broker is shutting down; retry after reconnect")
        seq, dup = await self.broker.publish(f.topic, f.msg_id, f.payload, sent_ns=f.sent_ns)
        self.control(Ack(topic=f.topic, msg_id=f.msg_id, seq=seq, duplicate=dup))

    async def _on_subscribe(self, f: Subscribe) -> None:
        b = self.broker
        if self.claims.role != "subscriber":
            raise ProtocolError("forbidden", "token role cannot subscribe")
        if self.closing or b.closing:
            raise ProtocolError("shutting_down", "broker is shutting down")
        if not self.claims.may_access(f.topic):
            raise ProtocolError("forbidden", f"token may not subscribe to {f.topic}")
        if f.topic not in self.subs and len(self.subs) >= b.config.max_subscriptions:
            raise ProtocolError("too_many_subscriptions", "subscription limit reached")
        st = await b.topic_state(f.topic)
        if f.last_seq is None:
            cursor = st.head                       # live only
        elif f.epoch is not None and f.epoch != b.backend.epoch:
            cursor = 0                             # the client's seqs belong to another log
        elif f.last_seq > st.head:
            raise ProtocolError("seq_ahead", f"last_seq {f.last_seq} is ahead of head {st.head}; "
                                             "send the epoch you resumed from")
        else:
            cursor = f.last_seq
            b.metrics.resumes.labels(f.topic).inc()
        await self._stop_sub(f.topic)              # re-sync replaces the old cursor
        sub = _Subscription(topic=f.topic, cursor=cursor, sent_seq=cursor)
        self.subs[f.topic] = sub
        sub.task = asyncio.create_task(self._feed(sub), name=f"feed-{self.id}-{f.topic}")

    async def _stop_sub(self, topic: str) -> None:
        sub = self.subs.pop(topic, None)
        if sub is not None and sub.task is not None:
            sub.task.cancel()
            try:
                await sub.task
            except (asyncio.CancelledError, Exception):
                pass
        self.outbox.purge_topic(topic)

    # -- the per-subscription cursor -------------------------------------------------
    async def _feed(self, sub: _Subscription) -> None:
        b = self.broker
        topic = sub.topic
        st = await b.topic_state(topic)
        first = await b.backend.first_seq(topic)
        resume_from = max(sub.cursor + 1, first)
        self.control(Subscribed(topic=topic, epoch=b.backend.epoch, head_seq=st.head, first_seq=first,
                                resume_from=resume_from))
        # Catch-up up to the head at subscribe time is paced by the consumer; the
        # slow-consumer policy applies to live traffic after it.
        replay_until = st.head
        if sub.cursor + 1 < first and sub.cursor < st.head:
            self.outbox.put_control(Item(frame=None, topic=topic, gap_from=sub.cursor + 1, gap_to=first - 1,
                                         gap_reason="retention"))
            sub.cursor = first - 1
        try:
            while True:
                changed = st.changed
                batch = await b.read_frames(topic, sub.cursor, b.config.read_batch)
                if not batch:
                    await changed.wait()
                    continue
                replayed = 0
                for seq, frame, pc in batch:
                    dropped = await self.outbox.put_event(Item(frame=frame, topic=topic, seq=seq,
                                                               appended_pc_ns=pc), paced=seq <= replay_until)
                    for d_topic, _ in dropped:
                        b.metrics.dropped.labels(d_topic, "slow_consumer").inc()
                    sub.cursor = seq
                    replayed += pc is None
                if replayed:
                    b.metrics.replayed.labels(topic).inc(replayed)
        except SlowConsumer:
            b.metrics.slow_disconnects.inc()
            log.info("session %s: slow consumer on %s, closing", self.id, topic)
            self.kill(CLOSE_SLOW_CONSUMER, "slow consumer")

    # -- output ------------------------------------------------------------------------
    def control(self, frame) -> None:
        self.outbox.put_control(Item(frame=encode(frame)))

    async def writer(self) -> None:
        b = self.broker
        try:
            while True:
                item = await self.outbox.get()
                if item is None:
                    break
                if not item.is_event:
                    await self.ws.send(item.render())
                    continue
                items = [item]
                if self.batch:
                    items += self.outbox.take_events(b.config.max_batch - 1)
                if len(items) == 1:
                    await self.ws.send(item.frame)
                else:  # events are pre-encoded once per message; join them, no re-encoding
                    await self.ws.send('{"type":"batch","v":1,"events":[' + ",".join(i.frame for i in items) + "]}")
                now = time.perf_counter_ns()
                for it in items:
                    b.metrics.delivered.labels(it.topic).inc()
                    sub = self.subs.get(it.topic)
                    if sub is not None:
                        sub.sent_seq = it.seq
                    if it.appended_pc_ns is not None:
                        b.metrics.append_to_send.observe((now - it.appended_pc_ns) / 1e9)
            if self.draining:  # graceful drain finished
                await self.ws.close(CLOSE_GOING_AWAY, "broker shutting down")
        except ConnectionClosed:
            pass

    async def heartbeats(self) -> None:
        b = self.broker
        while True:
            await asyncio.sleep(b.config.heartbeat_s)
            if time.time() > self.claims.exp:
                self.kill(CLOSE_TOKEN_EXPIRED, "token expired")
                return
            heads = {}
            for t in self.subs:
                st = b._topics.get(t)
                heads[t] = st.head if st else 0
            self.control(Heartbeat(ts_ns=time.time_ns(), heads=heads))

    def cancel_feeds(self) -> None:
        for sub in self.subs.values():
            if sub.task is not None:
                sub.task.cancel()

    def kill(self, code: int, reason: str) -> None:
        """Close now, discarding anything queued."""
        if self.closing:
            return
        self.closing = True
        self.cancel_feeds()
        self.outbox.clear()
        self.outbox.close()
        # Keep a reference: the event loop holds tasks only weakly.
        self._close_task = asyncio.create_task(self.ws.close(code, reason))

    def drain(self) -> None:
        """Graceful: stop the cursors, flush what is queued, say bye, then close 1001."""
        if self.closing:
            return
        self.closing = True
        self.draining = True
        self.cancel_feeds()
        self.control(Bye(reason="shutdown", reconnect_after_ms=self.broker.config.reconnect_hint_ms))
        self.outbox.close()


class Broker:
    def __init__(self, secret: str, backend: LogBackend | None = None, config: BrokerConfig | None = None):
        self.secret = secret
        self.backend = backend if backend is not None else MemoryBackend()
        self.config = config or BrokerConfig()
        self.metrics = BrokerMetrics(self)
        self.closing = False
        self._topics: dict[str, _TopicState] = {}
        self._sessions: set[_Session] = set()
        self._handlers: set[asyncio.Task] = set()
        self._server: Server | None = None
        self.port: int | None = None

    # -- lifecycle -----------------------------------------------------------------------
    async def start(self) -> "Broker":
        await self.backend.open()
        c = self.config
        self._server = await serve(
            self._handle, c.host, c.port, process_request=self._http, ssl=c.ssl,
            ping_interval=c.ping_interval_s, ping_timeout=c.ping_timeout_s, max_size=c.max_frame_bytes,
            compression=None)  # small frames; deflate per connection would cost CPU on every fan-out
        self.port = self._server.sockets[0].getsockname()[1]
        log.info("broker listening on %s", self.url)
        return self

    @property
    def url(self) -> str:
        scheme = "wss" if self.config.ssl else "ws"
        return f"{scheme}://{self.config.host}:{self.port}/ws"

    async def __aenter__(self) -> "Broker":
        return await self.start()

    async def __aexit__(self, *exc) -> None:
        await self.stop()

    async def stop(self, drain_timeout_s: float | None = None) -> None:
        """Graceful shutdown: refuse new work, flush queues, send bye, close, close the log."""
        if self._server is None:
            return
        self.closing = True
        self._server.server.close()  # stop accepting; clients reconnect to the next broker
        for s in list(self._sessions):
            s.drain()
        handlers = [t for t in self._handlers if not t.done()]
        if handlers:
            timeout = self.config.drain_timeout_s if drain_timeout_s is None else drain_timeout_s
            done, pending = await asyncio.wait(handlers, timeout=timeout)
            for t in pending:
                t.cancel()
        await self._close_server()

    async def abort(self) -> None:
        """Simulate a crash: drop every connection without a close handshake."""
        if self._server is None:
            return
        self.closing = True
        for s in list(self._sessions):
            s.closing = True
            s.cancel_feeds()
            s.ws.transport.abort()
        await asyncio.sleep(0)
        await self._close_server()

    async def _close_server(self) -> None:
        server, self._server = self._server, None
        server.close(close_connections=False)
        for t in list(self._handlers):
            t.cancel()
        if self._handlers:
            await asyncio.wait(list(self._handlers), timeout=2)
        await server.wait_closed()
        await self.backend.close()

    # -- HTTP side door: /metrics and /healthz on the same port --------------------------
    def _http(self, connection: ServerConnection, request):
        path = request.path.split("?", 1)[0]
        if path == "/metrics":
            return connection.respond(HTTPStatus.OK, self.metrics.render().decode())
        if path == "/healthz":
            if self.closing:
                return connection.respond(HTTPStatus.SERVICE_UNAVAILABLE, "draining\n")
            return connection.respond(HTTPStatus.OK, "ok\n")
        if path not in ("/", "/ws"):
            return connection.respond(HTTPStatus.NOT_FOUND, "not found\n")
        return None

    # -- the log -------------------------------------------------------------------------
    async def topic_state(self, topic: str) -> _TopicState:
        st = self._topics.get(topic)
        if st is None:
            if len(self._topics) >= self.config.max_topics:
                self._evict_idle_topics()
                if len(self._topics) >= self.config.max_topics:
                    raise ProtocolError("too_many_topics", f"broker tracks at most {self.config.max_topics} topics")
            st = self._topics[topic] = _TopicState()
        if not st.loaded:
            async with st.lock:
                if not st.loaded:
                    st.head = await self.backend.head(topic)
                    st.loaded = True
        return st

    async def publish(self, topic: str, msg_id: str, payload: dict, sent_ns: int | None = None) -> tuple[int, bool]:
        """Validate and append; returns (seq, duplicate). Idempotent on (topic, msg_id).

        A session's reader is never cancelled mid-frame (see ``_serve_connection``), so an
        append that reached the backend also reaches the cache and the head. If that ever
        failed, ``_append`` refuses to serve a cache with a hole.
        """
        validate_signal(payload)
        st = await self.topic_state(topic)
        res = await self._append(st, topic, msg_id, payload, sent_ns)
        if res.created:
            self.metrics.published.labels(topic).inc()
        else:
            self.metrics.publish_duplicates.labels(topic).inc()
        return res.seq, not res.created

    def _evict_idle_topics(self) -> None:
        """Forget topics that never received a message and have no subscriber (made-up names)."""
        live = {t for s in self._sessions for t in s.subs}
        for t, st in list(self._topics.items()):
            if st.head == 0 and t not in live and not st.lock.locked():
                del self._topics[t]
                for m in (self.metrics.resumes, self.metrics.published, self.metrics.publish_duplicates,
                          self.metrics.delivered, self.metrics.replayed):
                    try:
                        m.remove(t)
                    except KeyError:
                        pass

    async def _append(self, st: _TopicState, topic: str, msg_id: str, payload: dict, sent_ns: int | None):
        async with st.lock:
            res = await self.backend.append(topic, msg_id, payload, time.time_ns(), sent_ns)
            if not res.created and res.seq > st.head:
                # The log is ahead of us (another writer appended): our cache cannot be trusted
                # to be contiguous any more, so drop it and let cursors read from the log.
                st.cache.clear()
                st.head = res.seq
                st.notify()
            if res.created:
                ev = res.event
                frame = encode(Event(topic=topic, seq=ev.seq, msg_id=msg_id, published_ns=ev.published_ns,
                                     sent_ns=sent_ns, payload=payload))
                if st.cache and st.cache[-1][0] != ev.seq - 1:
                    st.cache.clear()  # never serve a cache with a hole; reads fall back to the log
                st.cache.append((ev.seq, frame, time.perf_counter_ns()))
                if len(st.cache) > self.config.cache_size:
                    st.cache.popleft()
                st.head = max(st.head, ev.seq)
                st.notify()
            return res

    async def read_frames(self, topic: str, after: int, limit: int) -> list[tuple[int, str, int | None]]:
        """Encoded events with seq > after: from the cache if it reaches back far enough."""
        st = await self.topic_state(topic)
        if after >= st.head:
            return []
        cache = st.cache
        if cache and cache[0][0] <= after + 1:
            start = after + 1 - cache[0][0]
            end = min(len(cache), start + limit)
            return [cache[i] for i in range(start, end)]
        out = []
        for ev in await self.backend.read(topic, after, limit):
            out.append((ev.seq, encode(Event(topic=topic, seq=ev.seq, msg_id=ev.msg_id,
                                             published_ns=ev.published_ns, sent_ns=ev.sent_ns,
                                             payload=ev.payload)), None))
        return out

    def lag_snapshot(self) -> dict[str, list[tuple[int, int]]]:
        """Per topic: (lag, queued events) for each live subscription."""
        out: dict[str, list[tuple[int, int]]] = {}
        for s in self._sessions:
            for t, sub in s.subs.items():
                st = self._topics.get(t)
                head = st.head if st else 0
                out.setdefault(t, []).append((max(0, head - sub.sent_seq), s.outbox.events))
        return out

    # -- connections -----------------------------------------------------------------------
    async def _handle(self, ws: ServerConnection) -> None:
        task = asyncio.current_task()
        self._handlers.add(task)
        try:
            await self._serve_connection(ws)
        finally:
            self._handlers.discard(task)

    async def _serve_connection(self, ws: ServerConnection) -> None:
        c = self.config
        if self.closing:
            await ws.close(CLOSE_GOING_AWAY, "broker shutting down")
            return
        if c.socket_send_buffer:
            sock = ws.transport.get_extra_info("socket")
            if sock is not None:
                try:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, c.socket_send_buffer)
                except OSError:
                    pass
        try:
            async with asyncio.timeout(c.auth_timeout_s):
                raw = await ws.recv()
            hello = decode_client(raw)
            if not isinstance(hello, Hello):
                raise ProtocolError("unauthorized", "first frame must be hello")
            claims = verify_token(self.secret, hello.token)
        except (asyncio.TimeoutError, ProtocolError, AuthError) as e:
            self.metrics.auth_failures.inc()
            code = e.code if isinstance(e, ProtocolError) else "unauthorized"
            try:
                await ws.send(encode(Error(code=code, message="authentication failed")))
                await ws.close(CLOSE_UNAUTHORIZED, "unauthorized")
            except ConnectionClosed:
                pass
            return
        except ConnectionClosed:
            return
        if self.closing:  # shutdown started while this client was authenticating
            await ws.close(CLOSE_GOING_AWAY, "broker shutting down")
            return
        policy = c.slow_policy
        if hello.slow_policy and c.allow_policy_override:
            policy = hello.slow_policy
        sess = _Session(self, ws, claims, Outbox(max_events=c.max_queue, policy=policy,
                                                 slow_grace_s=c.slow_grace_s))
        sess.batch = hello.batch
        self._sessions.add(sess)
        self.metrics.connects.labels(claims.role).inc()
        self.metrics.connections.labels(claims.role).inc()
        sess.control(Welcome(session=sess.id, role=claims.role, heartbeat_s=c.heartbeat_s,
                             slow_policy=policy, max_queue=c.max_queue))
        writer = asyncio.create_task(sess.writer(), name=f"writer-{sess.id}")
        beats = asyncio.create_task(sess.heartbeats(), name=f"hb-{sess.id}")
        try:
            reader = asyncio.ensure_future(self._read_loop(sess))
            done, _ = await asyncio.wait({reader, writer}, return_when=asyncio.FIRST_COMPLETED)
            if writer in done and not reader.done():
                # Writer finished (drain or kill): wait for the close handshake, then stop reading.
                try:
                    async with asyncio.timeout(5):
                        await ws.wait_closed()
                except asyncio.TimeoutError:
                    ws.transport.abort()
            # Let the reader finish on its own (the closed socket ends it) instead of
            # cancelling it: a cancel could land in the middle of a publish's append.
            try:
                async with asyncio.timeout(5):
                    await asyncio.gather(reader, return_exceptions=True)
            except asyncio.TimeoutError:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
        finally:
            sess.closing = True
            sess.cancel_feeds()
            for t in list(sess.subs):
                sub = sess.subs[t]
                if sub.task is not None:
                    try:
                        await sub.task
                    except (asyncio.CancelledError, Exception):
                        pass
            sess.outbox.close()
            beats.cancel()
            if not writer.done():
                writer.cancel()
            await asyncio.gather(writer, beats, return_exceptions=True)
            self._sessions.discard(sess)
            self.metrics.connections.labels(claims.role).dec()

    async def _read_loop(self, sess: _Session) -> None:
        try:
            async for raw in sess.ws:
                if sess.closing:
                    if not self.closing:
                        break
                    # Draining: still answer publishes with shutting_down so publishers retry.
                await sess.on_frame(raw)
        except ConnectionClosed:
            pass
        except Exception:
            # e.g. the log backend is unreachable: drop the connection; publishers re-send
            # their unacked messages (same msg_id) after reconnecting.
            log.exception("session %s failed; closing", sess.id)

"""Publisher SDK: idempotent publishing over the WebSocket publish role.

Each message carries an idempotency key (``msg_id``). Until the broker acks it, the
message stays pending and is re-sent with the same key after a reconnect. If the first
attempt was in fact appended (the connection died before the ack came back), the broker
answers ``duplicate=True`` with the original sequence number, so a retry never creates a
second copy in the log.
"""
from __future__ import annotations

import asyncio
import logging
import time
import uuid
from collections import OrderedDict
from typing import Any

from pydantic import BaseModel
from websockets.asyncio.client import connect
from websockets.exceptions import ConnectionClosed, InvalidHandshake

from .client import Backoff, SubscriberError
from .schema import Ack, Bye, Error, Hello, ProtocolError, Publish, Welcome, decode_server, encode

log = logging.getLogger("sigbroker.publisher")

RETRYABLE_ERRORS = {"shutting_down"}


class PublishError(Exception):
    def __init__(self, code: str, message: str, msg_id: str):
        super().__init__(f"{code}: {message} (msg_id={msg_id})")
        self.code = code
        self.msg_id = msg_id


class Publisher:
    def __init__(self, url: str, token: str, *, backoff: Backoff | None = None, open_timeout_s: float = 5.0,
                 stamp_sent: bool = True, ssl=None):
        self.url = url
        self.token = token
        self.backoff = backoff or Backoff()
        self.open_timeout_s = open_timeout_s
        self.stamp_sent = stamp_sent  # add perf_counter_ns at send, for latency measurement
        self.ssl = ssl
        self._pending: OrderedDict[str, tuple[str, asyncio.Future]] = OrderedDict()
        self._ws = None
        self._connected = asyncio.Event()
        self._send_lock = asyncio.Lock()  # keeps re-sends and new sends in publish order
        self._task: asyncio.Task | None = None
        self._closed = False
        self._fatal: BaseException | None = None
        self.connects = 0
        self.resent = 0       # frames sent again after a reconnect (true re-sends only)
        self._sent: set[str] = set()  # msg_ids written to some connection at least once
        self.duplicates = 0

    async def __aenter__(self) -> "Publisher":
        await self.start()
        return self

    async def __aexit__(self, *exc) -> None:
        await self.close()

    async def start(self, wait: bool = True) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="sigbroker-publisher")
        if wait:
            async with asyncio.timeout(self.open_timeout_s * 4):
                await self._wait_connected()

    async def _wait_connected(self) -> None:
        while not self._connected.is_set():
            if self._fatal is not None:
                raise self._fatal
            await asyncio.sleep(0.01)

    async def close(self) -> None:
        self._closed = True
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None
        for _, fut in self._pending.values():
            if not fut.done():
                fut.cancel()

    def kill_connection(self) -> None:
        """Test hook: drop the current connection abruptly."""
        if self._ws is not None:
            self._ws.transport.abort()

    async def publish(self, topic: str, payload: dict[str, Any] | BaseModel, msg_id: str | None = None,
                      timeout_s: float = 30.0) -> Ack:
        if self._fatal is not None:
            raise self._fatal
        if isinstance(payload, BaseModel):
            payload = payload.model_dump(mode="json")
        msg_id = msg_id or uuid.uuid4().hex
        existing = self._pending.get(msg_id)
        if existing is not None:
            fut = existing[1]
        else:
            sent_ns = time.perf_counter_ns() if self.stamp_sent else None
            frame = encode(Publish(topic=topic, msg_id=msg_id, payload=payload, sent_ns=sent_ns))
            fut = asyncio.get_running_loop().create_future()
            self._pending[msg_id] = (frame, fut)
            async with self._send_lock:
                ws = self._ws
                # The reconnect loop may already have sent this frame (it snapshots pending
                # messages while this call waited for the lock); sending it twice would be
                # harmless (the broker answers duplicate=True) but is skipped.
                if ws is not None and self._connected.is_set() and msg_id not in self._sent:
                    try:
                        await ws.send(frame)
                        self._sent.add(msg_id)
                    except ConnectionClosed:
                        pass  # the reconnect loop re-sends everything pending
        try:
            async with asyncio.timeout(timeout_s):
                return await asyncio.shield(fut)
        finally:
            if fut.done():
                self._pending.pop(msg_id, None)
                self._sent.discard(msg_id)

    async def _run(self) -> None:
        attempt = 0
        while not self._closed:
            hint_ms = 0
            try:
                async with connect(self.url, open_timeout=self.open_timeout_s, ssl=self.ssl,
                                   close_timeout=2, compression=None) as ws:
                    await ws.send(encode(Hello(token=self.token)))
                    async with asyncio.timeout(self.open_timeout_s):
                        first = decode_server(await ws.recv())
                    if isinstance(first, Error) or not isinstance(first, Welcome):
                        raise SubscriberError(f"publisher rejected: {getattr(first, 'code', first.type)}")
                    self._ws = ws
                    self.connects += 1
                    attempt = 0
                    async with self._send_lock:
                        for mid, (frame, fut) in list(self._pending.items()):  # original order
                            if not fut.done():
                                await ws.send(frame)
                                if mid in self._sent:
                                    self.resent += 1  # it went out on an earlier connection
                                self._sent.add(mid)
                        self._connected.set()
                    async for raw in ws:
                        f = decode_server(raw)
                        if isinstance(f, Ack):
                            entry = self._pending.get(f.msg_id)
                            if entry is not None and not entry[1].done():
                                self.duplicates += f.duplicate
                                entry[1].set_result(f)
                        elif isinstance(f, Error):
                            entry = self._pending.get(f.msg_id or "")
                            if entry is None:
                                log.warning("broker error %s: %s", f.code, f.message)
                            elif f.code not in RETRYABLE_ERRORS and not entry[1].done():
                                entry[1].set_exception(PublishError(f.code, f.message, f.msg_id))
                        elif isinstance(f, Bye):
                            hint_ms = f.reconnect_after_ms
                            await ws.close()
            except SubscriberError as e:
                self._fatal = e
                for _, fut in self._pending.values():
                    if not fut.done():
                        fut.set_exception(e)
                return
            except asyncio.CancelledError:
                raise
            except (OSError, ConnectionClosed, asyncio.TimeoutError, InvalidHandshake, EOFError, ProtocolError) as e:
                log.debug("publisher connection lost: %r", e)
            finally:
                self._ws = None
                self._connected.clear()
            if self._closed:
                return
            delay = self.backoff.delay(attempt)
            if hint_ms:
                delay = max(delay, hint_ms / 1000 * self.backoff.rng.uniform(0.5, 1.5))
            attempt += 1
            await asyncio.sleep(delay)

"""A TCP proxy for fault injection: cut live connections, refuse new ones, retarget.

Sits between clients and the broker on localhost. Cutting a connection closes both TCP
sides at once, in the middle of whatever frame was in flight, which is what a dropped
network link looks like to both peers.
"""
from __future__ import annotations

import asyncio
import contextlib
import socket


class ChaosProxy:
    def __init__(self, upstream_port: int, host: str = "127.0.0.1", buffer_bytes: int | None = 8192):
        """``buffer_bytes`` caps the proxy's socket and write buffers, so a stalled consumer
        backs up into the broker within hundreds of messages rather than thousands (OS
        loopback buffers otherwise hold about a megabyte)."""
        self.host = host
        self.buffer_bytes = buffer_bytes
        self.upstream_port = upstream_port
        self.port: int | None = None
        self.refuse = False
        self.cuts = 0
        self._server: asyncio.base_events.Server | None = None
        self._pairs: set[tuple[asyncio.StreamWriter, asyncio.StreamWriter]] = set()
        self._tasks: set[asyncio.Task] = set()

    @property
    def url(self) -> str:
        return f"ws://{self.host}:{self.port}/ws"

    async def start(self) -> "ChaosProxy":
        self._server = await asyncio.start_server(self._accept, self.host, 0)
        self.port = self._server.sockets[0].getsockname()[1]
        return self

    async def stop(self) -> None:
        self.cut_all(count=False)
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
        for t in list(self._tasks):
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    def cut_all(self, count: bool = True) -> int:
        n = 0
        for cw, uw in list(self._pairs):
            for w in (cw, uw):
                with contextlib.suppress(Exception):
                    w.transport.abort()
            n += 1
        self._pairs.clear()
        if count:
            self.cuts += n
        return n

    @property
    def open_connections(self) -> int:
        return len(self._pairs)

    async def _accept(self, cr: asyncio.StreamReader, cw: asyncio.StreamWriter) -> None:
        if self.refuse:
            cw.transport.abort()
            return
        try:
            ur, uw = await asyncio.open_connection(self.host, self.upstream_port)
        except OSError:
            cw.transport.abort()
            return
        if self.buffer_bytes:
            for w in (cw, uw):
                sock = w.get_extra_info("socket")
                with contextlib.suppress(OSError):
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, self.buffer_bytes)
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, self.buffer_bytes)
                w.transport.set_write_buffer_limits(high=self.buffer_bytes)
        pair = (cw, uw)
        self._pairs.add(pair)
        t1 = asyncio.create_task(self._pipe(cr, uw))
        t2 = asyncio.create_task(self._pipe(ur, cw))
        self._tasks.update((t1, t2))
        try:
            await asyncio.wait({t1, t2}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for t in (t1, t2):
                t.cancel()
            for w in (cw, uw):
                with contextlib.suppress(Exception):
                    w.transport.abort()
            self._pairs.discard(pair)
            self._tasks.difference_update((t1, t2))

    @staticmethod
    async def _pipe(r: asyncio.StreamReader, w: asyncio.StreamWriter) -> None:
        try:
            while True:
                data = await r.read(16384)
                if not data:
                    break
                w.write(data)
                await w.drain()
        except (ConnectionError, OSError, asyncio.CancelledError):
            pass

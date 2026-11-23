"""Command line: serve | token | publish | subscribe | demo | bench | load | report."""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
import ssl as ssl_mod
import sys
from pathlib import Path

from .auth import AuthError, mint_token, new_secret, secret_from_env


def _secret() -> str:
    try:
        return secret_from_env()
    except AuthError as e:
        sys.exit(f"error: {e}. Generate one with: python -c \"import secrets; print(secrets.token_hex(32))\"")


async def _serve(args) -> None:
    from .backends import make_backend
    from .broker import Broker, BrokerConfig
    cfg = BrokerConfig(host=args.host, port=args.port, heartbeat_s=args.heartbeat, max_queue=args.max_queue,
                       slow_policy=args.policy, slow_grace_s=args.slow_grace, drain_timeout_s=args.drain_timeout)
    if args.tls_cert:
        ctx = ssl_mod.SSLContext(ssl_mod.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(args.tls_cert, args.tls_key)
        cfg.ssl = ctx
    broker = await Broker(_secret(), make_backend(args.backend, retention=args.retention), cfg).start()
    print(f"PORT {broker.port}", flush=True)
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop.set)
        except (NotImplementedError, RuntimeError):  # Windows: Ctrl+C raises KeyboardInterrupt
            pass
    try:
        await stop.wait()
    finally:
        logging.getLogger("sigbroker").info("shutting down (draining up to %.1fs)", cfg.drain_timeout_s)
        await broker.stop()


async def _publish(args) -> None:
    from .publisher import Publisher
    from .synthetic import SignalGenerator
    gen = SignalGenerator(args.seed)
    token = mint_token(_secret(), args.sub, "publisher", [args.topic])
    async with Publisher(args.url, token) as pub:
        interval = 1.0 / args.rate
        n = 0
        while args.count == 0 or n < args.count:
            ack = await pub.publish(args.topic, gen.next())
            n += 1
            if n % max(1, int(args.rate)) == 0:
                print(f"published {n}, last seq {ack.seq}", flush=True)
            await asyncio.sleep(interval)


async def _subscribe(args) -> None:
    from .client import FileCursorStore, Subscriber
    token = mint_token(_secret(), args.sub, "subscriber", args.topic)
    store = FileCursorStore(args.cursor_file) if args.cursor_file else None
    async with Subscriber(args.url, token, args.topic, cursor_store=store, start=args.start,
                          on_gap=lambda g: print(f"GAP {g}", flush=True)) as sub:
        n = 0
        async for d in sub:
            print(json.dumps({"topic": d.topic, "seq": d.seq, **d.payload}), flush=True)
            n += 1
            if args.count and n >= args.count:
                break


def main(argv: list[str] | None = None) -> None:
    p = argparse.ArgumentParser(prog="sigbroker", description=__doc__)
    sp = p.add_subparsers(dest="cmd", required=True)

    s = sp.add_parser("serve", help="run the broker (secret from SIGBROKER_SECRET)")
    s.add_argument("--host", default="127.0.0.1")
    s.add_argument("--port", type=int, default=8765)
    s.add_argument("--backend", default="sqlite:sigbroker.sqlite",
                   help="memory | sqlite:PATH | redis://HOST:PORT/DB")
    s.add_argument("--retention", type=int, default=1_000_000, help="messages kept per topic")
    s.add_argument("--policy", choices=["disconnect", "drop_oldest"], default="disconnect")
    s.add_argument("--max-queue", type=int, default=1000)
    s.add_argument("--slow-grace", type=float, default=1.0)
    s.add_argument("--heartbeat", type=float, default=5.0)
    s.add_argument("--drain-timeout", type=float, default=5.0)
    s.add_argument("--tls-cert")
    s.add_argument("--tls-key")

    t = sp.add_parser("token", help="mint a token (secret from SIGBROKER_SECRET)")
    t.add_argument("--sub", required=True)
    t.add_argument("--role", choices=["subscriber", "publisher"], required=True)
    t.add_argument("--topics", nargs="+", required=True, help="fnmatch patterns, e.g. 'signals.*'")
    t.add_argument("--ttl", type=int, default=3600)

    pu = sp.add_parser("publish", help="publish synthetic signals")
    pu.add_argument("--url", default="ws://127.0.0.1:8765/ws")
    pu.add_argument("--topic", default="signals.rates")
    pu.add_argument("--rate", type=float, default=5.0)
    pu.add_argument("--count", type=int, default=0, help="0 = forever")
    pu.add_argument("--seed", type=int, default=0)
    pu.add_argument("--sub", default="research")

    su = sp.add_parser("subscribe", help="print signals as JSON lines")
    su.add_argument("--url", default="ws://127.0.0.1:8765/ws")
    su.add_argument("--topic", nargs="+", default=["signals.rates"])
    su.add_argument("--start", choices=["earliest", "latest"], default="earliest")
    su.add_argument("--cursor-file")
    su.add_argument("--count", type=int, default=0)
    su.add_argument("--sub", default="desk")

    d = sp.add_parser("demo", help="broker + publisher + subscribers in one process, with a dropped link")
    d.add_argument("--messages", type=int, default=40)

    b = sp.add_parser("bench", help="correctness suite -> results/correctness.json and RESULTS.md")
    b.add_argument("--out", default="results")
    b.add_argument("--quick", action="store_true", help="2 dev seeds only (CI smoke)")
    b.add_argument("--redis", help="run the development seeds against this Redis instead")

    lo = sp.add_parser("load", help="load benchmark on this machine -> results/load.json and RESULTS.md")
    lo.add_argument("--out", default="results")
    lo.add_argument("--smoke", action="store_true", help="one tiny cell (CI)")
    lo.add_argument("--duration", type=float, default=5.0)
    lo.add_argument("--redis", help="also run the Redis rows against this URL")
    lo.add_argument("--redis-label", help="name for the Redis rows, e.g. 'redis (fsync always)'")
    lo.add_argument("--add-redis-rows", action="store_true",
                    help="run only the Redis rows and append them to the existing load.json")

    r = sp.add_parser("report", help="re-render RESULTS.md from results/*.json")
    r.add_argument("--out", default="results")

    args = p.parse_args(argv)
    logging.basicConfig(level=os.environ.get("SIGBROKER_LOG", "WARNING"),
                        format="%(asctime)s %(name)s %(levelname)s %(message)s")
    if args.cmd == "serve":
        try:
            asyncio.run(_serve(args))
        except KeyboardInterrupt:
            pass
    elif args.cmd == "token":
        print(mint_token(_secret(), args.sub, args.role, args.topics, ttl_s=args.ttl))
    elif args.cmd == "publish":
        asyncio.run(_publish(args))
    elif args.cmd == "subscribe":
        asyncio.run(_subscribe(args))
    elif args.cmd == "demo":
        from .demo import run_demo
        asyncio.run(run_demo(args.messages, new_secret()))
    elif args.cmd == "bench":
        from .bench import run_correctness
        run_correctness(Path(args.out), quick=args.quick, redis_url=args.redis)
    elif args.cmd == "load":
        from .bench import run_load_bench
        run_load_bench(Path(args.out), smoke=args.smoke, duration_s=args.duration, redis_url=args.redis,
                       redis_label=args.redis_label, add_redis_rows=args.add_redis_rows)
    elif args.cmd == "report":
        from .bench import write_report
        write_report(Path(args.out))


if __name__ == "__main__":
    main()

# Contributing

Thanks for taking a look. Changes are judged on whether they keep the delivery guarantees, keep CI offline, and keep the numbers honest.

## Ground rules

1. **CI needs no network and no services.** Tests use localhost sockets, the in-memory and SQLite logs, and fakeredis. A real Redis is only used when `SIGBROKER_TEST_REDIS_URL` is set, never by default.
2. **No secrets in the repo.** Tests and the demo generate a random secret at runtime (`sigbroker.auth.new_secret`). The broker reads `SIGBROKER_SECRET` from the environment; `.env` is git-ignored and gitleaks runs in CI.
3. **Numbers come from results.** Any number in a doc must be in a `results/*.json` file written by `make bench` or `make load`. CI fails if a fresh correctness run does not match `results/correctness.json` (fields ending in `_observed` are timing-dependent and skipped) or if `RESULTS.md` is not rendered from the committed JSON.
4. **Do not tune on the held-out seeds.** Scenario seeds 0-9 are for development. Seeds 200-219 were the held-out set for 1.0.0; because CI reruns them on every change, they are regression seeds from now on (100-119 were retired after one of them guided a fix; see docs/methodology.md). To claim a held-out result for a new change, pick fresh seeds, run them once at the end, and say so.
5. **Every delivery change needs a fault test.** If you touch the broker cursor, the outbox, the SDK tracker or a backend, add or extend a test in `tests/test_faults.py` or `tests/test_client_logic.py` that checks the stream is exactly 1..N after the fault.
6. **Protocol changes follow docs/protocol.md.** Additive fields only within a major version; anything else bumps `PROTOCOL_VERSION`.

## Set up

```bash
python -m venv .venv && source .venv/bin/activate
pip install -c constraints.txt -e ".[dev]"
pytest -q -m "not slow"   # about 30 s
pytest -q                 # adds the full correctness rerun, several minutes
```

## Adding a backend

Implement the `LogBackend` protocol in `src/sigbroker/backends/`, add it to `make_backend`, and add it to the `KINDS` list in `tests/test_backends.py` so it runs the whole contract (gap-free concurrent appends, idempotency, retention, epoch). Then run the scenario suite against it (`ScenarioConfig(backend=...)`).

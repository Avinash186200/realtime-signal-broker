.PHONY: install test test-fast bench bench-redis load load-redis report demo up

install:
	pip install -c constraints.txt -e ".[dev]"

test:
	pytest -q

# Everything except the multi-minute correctness rerun.
test-fast:
	pytest -q -m "not slow"

# Fault-injection suite: development + held-out seeds -> results/correctness.json, RESULTS.md
bench:
	python -m sigbroker.cli bench --out results

# Same development seeds against a real Redis (docker run -p 6399:6379 redis:7-alpine)
bench-redis:
	python -m sigbroker.cli bench --out results --redis redis://127.0.0.1:6399/0

# Latency/throughput grid on this machine -> results/load.json, RESULTS.md (several minutes)
load:
	python -m sigbroker.cli load --out results

# The committed load.json also has Redis rows, made with two Redis 7 containers:
#   docker run -p 6399:6379 redis:7-alpine redis-server --appendonly yes --appendfsync always --maxmemory-policy noeviction
#   docker run -p 6398:6379 redis:7-alpine redis-server --appendonly yes --appendfsync everysec --maxmemory-policy noeviction
load-redis:
	python -m sigbroker.cli load --out results --redis redis://127.0.0.1:6399/0 --redis-label "redis (fsync always)"
	python -m sigbroker.cli load --out results --redis redis://127.0.0.1:6398/0 --redis-label "redis (fsync everysec)" --add-redis-rows

report:
	python -m sigbroker.cli report --out results

demo:
	python -m sigbroker.cli demo

up:
	docker compose up --build

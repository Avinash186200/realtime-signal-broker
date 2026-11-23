import asyncio
import os

import pytest

from sigbroker.auth import mint_token, new_secret


@pytest.fixture
def secret():
    return new_secret()  # generated per test, never stored


@pytest.fixture
def tokens(secret):
    def make(role="subscriber", topics=("signals.*",), sub="t", ttl_s=3600):
        return mint_token(secret, sub, role, list(topics), ttl_s=ttl_s)
    return make


def run(coro, timeout=60):
    async def guarded():
        async with asyncio.timeout(timeout):
            return await coro
    return asyncio.run(guarded())


REDIS_URL = os.environ.get("SIGBROKER_TEST_REDIS_URL")  # optional: a real Redis for the backend tests

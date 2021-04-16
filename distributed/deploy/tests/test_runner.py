import asyncio
from contextlib import suppress
from time import time

import pytest

from dask.distributed import Client

from distributed.deploy.runner import AsyncCommWorld, AsyncRunner
from distributed.utils_test import loop  # noqa: F401


@pytest.mark.asyncio
@pytest.mark.timeout(10)
async def test_runner(loop):
    commworld = AsyncCommWorld()

    async def run_code(commworld):
        with suppress(SystemExit):
            async with AsyncRunner(commworld, asynchronous=True) as runner:
                async with Client(runner, asynchronous=True) as c:

                    start = time()
                    while len(c.scheduler_info()["workers"]) != 2:
                        assert time() < start + 10
                        await asyncio.sleep(0.2)

                    assert await c.submit(lambda x: x + 1, 10).result() == 11
                    assert await c.submit(lambda x: x + 1, 20).result() == 21

    await asyncio.gather(*[run_code(commworld) for _ in range(4)])

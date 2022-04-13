import asyncio

import pytest

from distributed import Nanny
from distributed.chaos import KillWorker
from distributed.utils_test import gen_cluster


@pytest.mark.parametrize("mode", ["sys.exit", "graceful", "segfault"])
@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True, Worker=Nanny)
async def test_KillWorker(c, s, w, mode):
    plugin = KillWorker(delay="1ms", mode=mode)

    await c.register_worker_plugin(plugin)

    while s.workers:
        await asyncio.sleep(0.001)

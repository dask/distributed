import asyncio
import random
import sys
from contextlib import suppress
from operator import add
from time import sleep

import pytest
from tlz import concat, sliding_window

from dask import delayed

from distributed import Client, Nanny, wait
from distributed.client import wait
from distributed.config import config
from distributed.metrics import time
from distributed.utils import All, CancelledError
from distributed.utils_test import (  # noqa: F401
    bump_rlimit,
    cluster,
    gen_cluster,
    inc,
    loop,
    nodebug_setup_module,
    nodebug_teardown_module,
    slowadd,
    slowinc,
    slowsum,
)

# All tests here are slow in some way
setup_module = nodebug_setup_module
teardown_module = nodebug_teardown_module


@gen_cluster(client=True)
async def test_stress_1(c, s, a, b):
    n = 2 ** 6

    seq = c.map(inc, range(n))
    while len(seq) > 1:
        await asyncio.sleep(0.1)
        seq = [c.submit(add, seq[i], seq[i + 1]) for i in range(0, len(seq), 2)]
    result = await seq[0]
    assert result == sum(map(inc, range(n)))


@pytest.mark.parametrize(("func", "n"), [(slowinc, 100), (inc, 1000)])
def test_stress_gc(loop, func, n):
    with cluster() as (s, [a, b]):
        with Client(s["address"], loop=loop) as c:
            x = c.submit(func, 1)
            for i in range(n):
                x = c.submit(func, x)

            assert x.result() == n + 2


@pytest.mark.skipif(
    sys.platform.startswith("win"), reason="test can leave dangling RPC objects"
)
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 8, timeout=None)
async def test_cancel_stress(c, s, *workers):
    da = pytest.importorskip("dask.array")
    x = da.random.random((50, 50), chunks=(2, 2))
    x = c.persist(x)
    await wait([x])
    y = (x.sum(axis=0) + x.sum(axis=1) + 1).std()
    n_todo = len(y.dask) - len(x.dask)
    for i in range(5):
        f = c.compute(y)
        while len(s.waiting) > (random.random() + 1) * 0.5 * n_todo:
            await asyncio.sleep(0.01)
        await c._cancel(f)


def test_cancel_stress_sync(loop):
    da = pytest.importorskip("dask.array")
    x = da.random.random((50, 50), chunks=(2, 2))
    with cluster(active_rpc_timeout=10) as (s, [a, b]):
        with Client(s["address"], loop=loop) as c:
            x = c.persist(x)
            y = (x.sum(axis=0) + x.sum(axis=1) + 1).std()
            wait(x)
            for i in range(5):
                f = c.compute(y)
                sleep(random.random())
                c.cancel(f)


@gen_cluster(nthreads=[], client=True, timeout=None)
async def test_stress_creation_and_deletion(c, s):
    # Assertions are handled by the validate mechanism in the scheduler
    s.allowed_failures = 100000
    da = pytest.importorskip("dask.array")

    x = da.random.random(size=(2000, 2000), chunks=(100, 100))
    y = (x + 1).T + (x * 2) - x.mean(axis=1)

    z = c.persist(y)

    async def create_and_destroy_worker(delay):
        start = time()
        while time() < start + 5:
            n = await Nanny(s.address, nthreads=2, loop=s.loop)
            await asyncio.sleep(delay)
            await n.close()
            print("Killed nanny")

    await asyncio.wait_for(
        All([create_and_destroy_worker(0.1 * i) for i in range(20)]), 60
    )


@gen_cluster(nthreads=[("127.0.0.1", 1)] * 10, client=True, timeout=60)
async def test_stress_scatter_death(c, s, *workers):
    import random

    s.allowed_failures = 1000
    np = pytest.importorskip("numpy")
    L = await c.scatter([np.random.random(10000) for i in range(len(workers))])
    await c.replicate(L, n=2)

    adds = [
        delayed(slowadd, pure=True)(
            random.choice(L),
            random.choice(L),
            delay=0.05,
            dask_key_name="slowadd-1-%d" % i,
        )
        for i in range(50)
    ]

    adds = [
        delayed(slowadd, pure=True)(a, b, delay=0.02, dask_key_name="slowadd-2-%d" % i)
        for i, (a, b) in enumerate(sliding_window(2, adds))
    ]

    futures = c.compute(adds)
    L = adds = None

    alive = list(workers)

    from distributed.scheduler import logger

    for i in range(7):
        await asyncio.sleep(0.1)
        try:
            s.validate_state()
        except Exception as c:
            logger.exception(c)
            if config.get("log-on-err"):
                import pdb

                pdb.set_trace()
            else:
                raise
        w = random.choice(alive)
        await w.close()
        alive.remove(w)

    with suppress(CancelledError):
        await c.gather(futures)

    futures = None


def vsum(*args):
    return sum(args)


@pytest.mark.avoid_ci
@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 80, timeout=1000)
async def test_stress_communication(c, s, *workers):
    s.validate = False  # very slow otherwise
    da = pytest.importorskip("dask.array")
    # Test consumes many file descriptors and can hang if the limit is too low
    resource = pytest.importorskip("resource")
    bump_rlimit(resource.RLIMIT_NOFILE, 8192)

    n = 20
    xs = [da.random.random((100, 100), chunks=(5, 5)) for i in range(n)]
    ys = [x + x.T for x in xs]
    z = da.atop(vsum, "ij", *concat(zip(ys, ["ij"] * n)), dtype="float64")

    future = c.compute(z.sum())

    result = await future
    assert isinstance(result, float)


@pytest.mark.skip
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)] * 10, timeout=60)
async def test_stress_steal(c, s, *workers):
    s.validate = False
    for w in workers:
        w.validate = False

    dinc = delayed(slowinc)
    L = [delayed(slowinc)(i, delay=0.005) for i in range(100)]
    for i in range(5):
        L = [delayed(slowsum)(part, delay=0.005) for part in sliding_window(5, L)]

    total = delayed(sum)(L)
    future = c.compute(total)

    while future.status != "finished":
        await asyncio.sleep(0.1)
        for i in range(3):
            a = random.choice(workers)
            b = random.choice(workers)
            if a is not b:
                s.work_steal(a.address, b.address, 0.5)
        if not s.processing:
            break


@pytest.mark.slow
@gen_cluster(nthreads=[("127.0.0.1", 1)] * 10, client=True, timeout=120)
async def test_close_connections(c, s, *workers):
    da = pytest.importorskip("dask.array")
    x = da.random.random(size=(1000, 1000), chunks=(1000, 1))
    for i in range(3):
        x = x.rechunk((1, 1000))
        x = x.rechunk((1000, 1))

    future = c.compute(x.sum())
    while any(s.processing.values()):
        await asyncio.sleep(0.5)
        worker = random.choice(list(workers))
        for comm in worker._comms:
            comm.abort()
        # print(frequencies(ts.state for ts in s.tasks.values()))
        # for w in workers:
        #     print(w)

    await wait(future)


@pytest.mark.slow
@pytest.mark.xfail(
    reason="flaky and re-fails on rerun; "
    "IOStream._handle_write blocks on large write_buffer"
    " https://github.com/tornadoweb/tornado/issues/2110",
)
@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_no_delay_during_large_transfer(c, s, w):
    pytest.importorskip("crick")
    np = pytest.importorskip("numpy")
    x = np.random.random(100000000)
    x_nbytes = x.nbytes

    # Reset digests
    from collections import defaultdict
    from functools import partial

    from dask.diagnostics import ResourceProfiler

    from distributed.counter import Digest

    for server in [s, w]:
        server.digests = defaultdict(partial(Digest, loop=server.io_loop))
        server._last_tick = time()

    with ResourceProfiler(dt=0.01) as rprof:
        future = await c.scatter(x, direct=True, hash=False)
        await asyncio.sleep(0.5)

    rprof.close()
    x = None  # lose ref

    for server in [s, w]:
        assert server.digests["tick-duration"].components[0].max() < 0.5

    nbytes = np.array([t.mem for t in rprof.results])
    nbytes -= nbytes[0]
    assert nbytes.max() < (x_nbytes * 2) / 1e6
    assert nbytes[-1] < (x_nbytes * 1.2) / 1e6

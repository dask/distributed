from __future__ import print_function, division, absolute_import

from concurrent.futures import CancelledError
from datetime import timedelta
from operator import add
from time import sleep, time

from dask import delayed
import pytest
from toolz import concat, sliding_window

from distributed import Executor, wait, Nanny
from distributed.utils import All
from distributed.utils_test import (gen_cluster, cluster, inc, slowinc, loop,
        slowadd, slow)
from distributed.executor import _wait
from tornado import gen


@gen_cluster(executor=True)
def test_stress_1(e, s, a, b):
    n = 2**6

    seq = e.map(inc, range(n))
    while len(seq) > 1:
        yield gen.sleep(0.1)
        seq = [e.submit(add, seq[i], seq[i + 1])
                for i in range(0, len(seq), 2)]
    result = yield seq[0]._result()
    assert result == sum(map(inc, range(n)))


@pytest.mark.parametrize(('func', 'n'), [(slowinc, 100), (inc, 1000)])
def test_stress_gc(loop, func, n):
    with cluster() as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            x = e.submit(func, 1)
            for i in range(n):
                x = e.submit(func, x)

            assert x.result() == n + 2


@gen_cluster(executor=True, ncores=[('127.0.0.1', 1)] * 4, timeout=None)
def test_cancel_stress(e, s, *workers):
    da = pytest.importorskip('dask.array')
    x = da.random.random((40, 40), chunks=(1, 1))
    x = e.persist(x)
    yield _wait([x])
    y = (x.sum(axis=0) + x.sum(axis=1) + 1).std()
    for i in range(5):
        f = e.compute(y)
        while len(s.waiting) > (len(y.dask) - len(x.dask)) / 2:
            yield gen.sleep(0.01)
        yield e._cancel(f)


def test_cancel_stress_sync(loop):
    da = pytest.importorskip('dask.array')
    x = da.random.random((40, 40), chunks=(1, 1))
    with cluster() as (s, [a, b]):
        with Executor(('127.0.0.1', s['port']), loop=loop) as e:
            x = e.persist(x)
            y = (x.sum(axis=0) + x.sum(axis=1) + 1).std()
            wait(x)
            for i in range(5):
                f = e.compute(y)
                sleep(1)
                e.cancel(f)


@gen_cluster(ncores=[], executor=True, timeout=None)
def test_stress_creation_and_deletion(e, s):
    # Assertions are handled by the validate mechanism in the scheduler
    s.allowed_failures = 100000
    da = pytest.importorskip('dask.array')

    x = da.random.random(size=(2000, 2000), chunks=(100, 100))
    y = (x + 1).T + (x * 2) - x.mean(axis=1)

    z = e.persist(y)

    @gen.coroutine
    def create_and_destroy_worker(delay):
        start = time()
        while time() < start + 10:
            n = Nanny(s.ip, s.port, ncores=2, loop=s.loop)
            n.start(0)

            yield gen.sleep(delay)

            yield n._close()
            print("Killed nanny")

    yield gen.with_timeout(timedelta(minutes=1),
                          All([create_and_destroy_worker(0.1 * i) for i in
                              range(10)]))


@gen_cluster(ncores=[('127.0.0.1', 1)] * 10, executor=True, timeout=60)
def test_stress_scatter_death(e, s, *workers):
    import random
    np = pytest.importorskip('numpy')
    L = yield e._scatter([np.random.random(10000) for i in range(len(workers))])
    yield e._replicate(L, n=2)

    adds = [delayed(slowadd, pure=True)(random.choice(L),
                                        random.choice(L),
                                        delay=0.05)
            for i in range(50)]

    adds = [delayed(slowadd, pure=True)(a, b, delay=0.02)
            for a, b in sliding_window(2, adds)]

    futures = e.compute(adds)

    alive = list(workers)

    from distributed.scheduler import logger

    for i in range(7):
        yield gen.sleep(0.1)
        try:
            s.validate_state()
        except Exception as e:
            logger.exception(e)
            import pdb; pdb.set_trace()
        w = random.choice(alive)
        yield w._close()
        alive.remove(w)

    try:
        yield gen.with_timeout(timedelta(seconds=10), e._gather(futures))
    except gen.TimeoutError:
        import pdb; pdb.set_trace()
    except CancelledError:
        pass


def vsum(*args):
    return sum(args)


@slow
@gen_cluster(executor=True, ncores=[('127.0.0.1', 1)] * 80, timeout=1000)
def test_stress_communication(e, s, *workers):
    s.validate = False # very slow otherwise
    da = pytest.importorskip('dask.array')

    n = 40
    xs = [da.random.random((100, 100), chunks=(5, 5)) for i in range(n)]
    ys = [x + x.T for x in xs]
    z = da.atop(vsum, 'ij', *concat(zip(ys, ['ij'] * n)))

    future = e.compute(z.sum())

    result = yield future._result()
    assert isinstance(result, float)

from distributed import Worker
from distributed.client import _wait

from distributed.utils_test import (inc, ignoring, dec, gen_cluster, gen_test,
        loop, readone, slowinc, slowadd)


@gen_cluster(client=True, ncores=[])
def test_resources(c, s):
    assert not s.worker_resources
    assert not s.resources

    a = Worker(s.ip, s.port, loop=s.loop, resources={'GPU': 2})
    b = Worker(s.ip, s.port, loop=s.loop, resources={'GPU': 1, 'DB': 1})

    yield [a._start(), b._start()]

    assert s.resources == {'GPU': {a.address: 2, b.address: 1},
                           'DB': {b.address: 1}}
    assert s.worker_resources == {a.address: {'GPU': 2},
                                  b.address: {'GPU': 1, 'DB': 1}}

    yield b._close()

    assert s.resources == {'GPU': {a.address: 2}, 'DB': {}}
    assert s.worker_resources == {a.address: {'GPU': 2}}

    yield a._close()


@gen_cluster(client=True, ncores=[])
def test_resource_submit(c, s):
    a = Worker(s.ip, s.port, loop=s.loop, resources={'A': 5})
    b = Worker(s.ip, s.port, loop=s.loop, resources={'A': 1, 'B': 1})

    yield [a._start(), b._start()]

    x = c.submit(inc, 1, resources={'A': 3})
    y = c.submit(inc, 2, resources={'B': 1})
    z = c.submit(inc, 3, resources={'C': 2})

    yield _wait(x)
    assert x.key in a.data

    yield _wait(y)
    assert y.key in b.data

    assert z.key in s.unrunnable

    d = Worker(s.ip, s.port, loop=s.loop, resources={'C': 10})
    yield d._start()

    yield _wait(z)
    assert z.key in d.data

    yield [a._close(), b._close(), d._close()]


@gen_cluster(client=True, ncores=[])
def test_submit_many_non_overlapping(c, s):
    a = Worker(s.ip, s.port, loop=s.loop, resources={'A': 1})
    b = Worker(s.ip, s.port, loop=s.loop, resources={'B': 1})

    yield [a._start(), b._start()]

    futures = [c.submit(inc, i, resources={'A': 1}) for i in range(5)]
    yield _wait(futures)

    assert len(a.data) == 5
    assert len(b.data) == 0

    yield [a._close(), b._close()]


@gen_cluster(client=True, ncores=[])
def test_move(c, s):
    a = Worker(s.ip, s.port, loop=s.loop, resources={'A': 1})
    b = Worker(s.ip, s.port, loop=s.loop, resources={'B': 1})

    yield [a._start(), b._start()]

    [x] = yield c._scatter([1], workers=b.address)

    future = c.submit(inc, x, resources={'A': 1})

    yield _wait(future)
    assert a.data[future.key] == 2

    yield [a._close(), b._close()]


@gen_cluster(client=True, ncores=[])
def test_dont_work_steal(c, s):
    a = Worker(s.ip, s.port, loop=s.loop, resources={'A': 1}, ncores=1)
    b = Worker(s.ip, s.port, loop=s.loop, resources={'B': 1}, ncores=1)

    yield [a._start(), b._start()]

    [x] = yield c._scatter([1], workers=a.address)

    futures = [c.submit(slowadd, x, i, resources={'A': 1}, delay=0.05)
              for i in range(10)]

    yield _wait(futures)
    assert all(f.key in a.data for f in futures)

    yield [a._close(), b._close()]

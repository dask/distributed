from collections import Iterator
from operator import add
import random

import pytest

from distributed import Client
from distributed.client import (_as_completed, as_completed, _first_completed,
        AsCompleted)
from distributed.utils_test import gen_cluster, inc, dec, loop, cluster
from distributed.compatibility import Queue


@gen_cluster(client=True)
def test__as_completed(c, s, a, b):
    x = c.submit(inc, 1)
    y = c.submit(inc, 1)
    z = c.submit(inc, 2)

    queue = Queue()
    yield _as_completed([x, y, z], queue)

    assert queue.qsize() == 3
    assert {queue.get(), queue.get(), queue.get()} == {x, y, z}

    result = yield _first_completed([x, y, z])
    assert result in [x, y, z]


def test_as_completed(loop):
    with cluster() as (s, [a, b]):
        with Client(('127.0.0.1', s['port']), loop=loop) as c:
            x = c.submit(inc, 1)
            y = c.submit(inc, 2)
            z = c.submit(inc, 1)

            seq = as_completed([x, y, z])
            assert isinstance(seq, Iterator)
            assert set(seq) == {x, y, z}

            assert list(as_completed([])) == []


def test_as_completed_with_non_futures(loop):
    with cluster() as (s, [a, b]):
        with Client(('127.0.0.1', s['port']), loop=loop) as c:
            with pytest.raises(TypeError):
                list(as_completed([1, 2, 3]))


def test_as_completed_add(loop):
    with cluster() as (s, [a, b]):
        with Client(('127.0.0.1', s['port']), loop=loop) as c:
            total = 0
            futures = c.map(inc, range(20))
            ac = AsCompleted(futures)
            for future in ac:
                total += future.result()
                if random.random() < 0.5:
                    future = c.submit(add, future, 10)
                    ac.add(future)
            assert total > sum(map(inc, range(10)))


def test_as_completed_repeats(loop):
    with cluster() as (s, [a, b]):
        with Client(('127.0.0.1', s['port']), loop=loop) as c:
            ac = AsCompleted()
            x = c.submit(inc, 1)
            ac.add(x)
            ac.add(x)

            assert next(ac) is x
            assert next(ac) is x

            with pytest.raises(StopIteration):
                next(ac)

            ac.add(x)
            assert next(ac) is x

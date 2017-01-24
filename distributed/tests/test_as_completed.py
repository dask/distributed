from collections import Iterator

import pytest

from distributed import Client
from distributed.client import _as_completed, as_completed, _first_completed
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


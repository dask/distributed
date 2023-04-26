from __future__ import annotations

import pytest

pl = pytest.importorskip("polars")

import distributed
from distributed.protocol import deserialize, serialize, to_serialize
from distributed.utils_test import gen_cluster


def test_roundtrip():
    # Test that the serialize/deserialize functions actually
    # work independent of distributed
    obj = pl.DataFrame({"A": list("abc"), "B": [1, 2, 3]})
    header, frames = serialize(obj)
    new_obj = deserialize(header, frames)
    assert obj.frame_equal(new_obj)


def test_roundtrip_lazy():
    # Test that the serialize/deserialize functions actually
    # work independent of distributed
    obj = pl.DataFrame({"A": list("abc"), "B": [1, 2, 3]}).lazy()
    header, frames = serialize(obj)
    new_obj = deserialize(header, frames)
    assert obj.collect().frame_equal(new_obj.collect())


def echo(arg):
    return arg


def test_scatter():
    @gen_cluster(client=True)
    async def run_test(client, scheduler, worker1, worker2):
        obj = pl.DataFrame({"A": list("abc"), "B": [1, 2, 3]})
        obj_fut = await client.scatter(obj)
        fut = client.submit(echo, obj_fut)
        result = await fut
        assert obj.frame_equal(result)

    run_test()


def test_scatter_lazy():
    @gen_cluster(client=True)
    async def run_test(client, scheduler, worker1, worker2):
        obj = pl.DataFrame({"A": list("abc"), "B": [1, 2, 3]}).lazy()
        obj_fut = await client.scatter(obj)
        fut = client.submit(echo, obj_fut)
        result = await fut
        assert obj.collect().frame_equal(result.collect())

    run_test()


def test_dumps_compression():
    # https://github.com/dask/distributed/issues/2966
    # large enough to trigger compression
    t = pl.DataFrame({"A": [1] * 10000})
    msg = {"op": "update", "data": to_serialize(t)}
    result = distributed.protocol.loads(distributed.protocol.dumps(msg))
    assert result["data"].frame_equal(t)

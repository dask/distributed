from typing import Dict

import dask.config
from dask.highlevelgraph import HighLevelGraph, MaterializedLayer

from distributed.client import Client
from distributed.protocol import dumps, loads
from distributed.protocol.serialize import ToPickle
from distributed.utils_test import gen_cluster


def test_ToPickle():
    class Foo:
        def __init__(self, data):
            self.data = data

    msg = {"x": ToPickle(Foo(123))}
    frames = dumps(msg)
    out = loads(frames)
    assert out["x"].data == 123


class NonMsgPackSerializableLayer(MaterializedLayer):
    """Layer that uses non-msgpack-serializable data"""


@gen_cluster(client=True)
async def test_non_msgpack_serializable_layer(c: Client, s, w1, w2):
    with dask.config.set({"distributed.scheduler.allowed-imports": "test_to_pickle"}):
        a = NonMsgPackSerializableLayer({"x": 42})
        layers = {"a": a}
        dependencies: Dict[str, set] = {"a": set()}
        hg = HighLevelGraph(layers, dependencies)
        res = await c.get(hg, "x", sync=False)
        assert res == 42

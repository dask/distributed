from typing import Dict

import dask.config
from dask.highlevelgraph import HighLevelGraph, MaterializedLayer

from distributed.client import Client
from distributed.protocol.serialize import ToPickle
from distributed.utils_test import gen_cluster


class NonMsgPackSerializableLayer(MaterializedLayer):
    """Layer that uses non-msgpack-serializable data"""

    def __dask_distributed_pack__(self, *args, **kwargs):
        ret = super().__dask_distributed_pack__(*args, **kwargs)
        # Some info that contains a `list`, which msgpack will convert to
        # a tuple if getting the chance.
        ret["myinfo"] = ["myinfo"]
        return ToPickle(ret)

    @classmethod
    def __dask_distributed_unpack__(cls, state, *args, **kwargs):
        assert state["myinfo"] == ["myinfo"]
        return super().__dask_distributed_unpack__(state, *args, **kwargs)


@gen_cluster(client=True)
async def test_non_msgpack_serializable_layer(c: Client, s, w1, w2):
    with dask.config.set({"distributed.scheduler.allowed-imports": "test_to_pickle"}):
        a = NonMsgPackSerializableLayer({"x": 42})
        layers = {"a": a}
        dependencies: Dict[str, set] = {"a": set()}
        hg = HighLevelGraph(layers, dependencies)
        res = await c.get(hg, "x", sync=False)
        assert res == 42

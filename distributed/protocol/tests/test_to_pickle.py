import logging
from typing import Dict

import pytest

import dask.config
from dask.highlevelgraph import HighLevelGraph, MaterializedLayer

from distributed.client import Client
from distributed.protocol import dumps, loads
from distributed.protocol.serialize import ToPickle
from distributed.scheduler import Scheduler
from distributed.utils import CancelledError
from distributed.utils_test import captured_logger
from distributed.worker import Worker


@pytest.mark.parametrize("allow_pickle", [False, True])
def test_ToPickle(allow_pickle):
    class Foo:
        def __init__(self, data):
            self.data = data

    with dask.config.set(
        {
            "distributed.scheduler.pickle": allow_pickle,
        }
    ):
        msg = {"x": ToPickle(Foo(123))}
        frames = dumps(msg)
        if allow_pickle:
            out = loads(frames)
            assert out["x"].data == 123
        else:
            with pytest.raises(
                ValueError, match="Unpickle on the Scheduler isn't allowed"
            ):
                loads(frames)


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


@pytest.mark.parametrize("allow_pickle", [False, True])
@pytest.mark.parametrize("protocol", ["tcp", "inproc"])
@pytest.mark.asyncio
async def test_non_msgpack_serializable(allow_pickle, protocol):
    async def client_run(log, a, c):
        a = NonMsgPackSerializableLayer({"x": 42})
        layers = {"a": a}
        dependencies: Dict[str, set] = {"a": set()}
        hg = HighLevelGraph(layers, dependencies)
        if allow_pickle or protocol == "inproc":
            res = await c.get(hg, "x", sync=False)
            assert res == 42
        else:
            with pytest.raises(CancelledError):
                await c.get(hg, "x", sync=False)
            assert "Unpickle on the Scheduler isn't allowed" in log.getvalue()

    with dask.config.set(
        {
            "distributed.scheduler.allowed-imports": "test_to_pickle",
            "distributed.scheduler.pickle": allow_pickle,
        }
    ):
        with captured_logger(logging.getLogger("distributed.core")) as log:
            async with Scheduler(dashboard_address=":0", protocol=protocol) as s:
                async with Worker(s.listeners[0].contact_address) as a:
                    async with Client(s.address, asynchronous=True) as c:
                        await client_run(log, a, c)

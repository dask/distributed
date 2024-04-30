from __future__ import annotations

import itertools
from typing import Any

from distributed.core import PooledRPCCall
from distributed.shuffle._core import ShuffleId, ShuffleRun

UNPACK_PREFIX = "shuffle_p2p"
try:
    import dask.dataframe as dd

    if dd._dask_expr_enabled():
        UNPACK_PREFIX = "p2pshuffle"
except ImportError:
    pass


class PooledRPCShuffle(PooledRPCCall):
    def __init__(self, shuffle: ShuffleRun):
        self.shuffle = shuffle

    def __getattr__(self, key):
        async def _(**kwargs):
            from distributed.protocol.serialize import _nested_deserialize

            method_name = key.replace("shuffle_", "")
            kwargs.pop("shuffle_id", None)
            kwargs.pop("run_id", None)
            # TODO: This is a bit awkward. At some point the arguments are
            # already getting wrapped with a `Serialize`. We only want to unwrap
            # here.
            kwargs = _nested_deserialize(kwargs)
            meth = getattr(self.shuffle, method_name)
            return await meth(**kwargs)

        return _


class AbstractShuffleTestPool:
    _shuffle_run_id_iterator = itertools.count()

    def __init__(self, *args, **kwargs):
        self.shuffles = {}

    def __call__(self, addr: str, *args: Any, **kwargs: Any) -> PooledRPCShuffle:
        return PooledRPCShuffle(self.shuffles[addr])

    async def shuffle_barrier(
        self, id: ShuffleId, run_id: int, consistent: bool
    ) -> dict[str, None]:
        out = {}
        for addr, s in self.shuffles.items():
            out[addr] = await s.inputs_done()
        return out

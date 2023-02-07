from __future__ import annotations

import asyncio
import itertools
from typing import Any

import numpy as np
import pytest

from dask.array.core import concatenate3

from distributed.core import PooledRPCCall
from distributed.shuffle._limiter import ResourceLimiter
from distributed.shuffle._scheduler_extension import get_worker_for
from distributed.shuffle._shuffle import ShuffleId
from distributed.shuffle._worker_extension import RechunkRun
from distributed.utils_test import gen_test


class PooledRPCShuffle(PooledRPCCall):
    # FIXME: Mostly copied from test_shuffle
    def __init__(self, shuffle: RechunkRun):
        self.shuffle = shuffle

    def __getattr__(self, key):
        async def _(**kwargs):
            from distributed.protocol.serialize import nested_deserialize

            method_name = key.replace("shuffle_", "")
            kwargs.pop("shuffle_id", None)
            kwargs.pop("run_id", None)
            # TODO: This is a bit awkward. At some point the arguments are
            # already getting wrapped with a `Serialize`. We only want to unwrap
            # here.
            kwargs = nested_deserialize(kwargs)
            meth = getattr(self.shuffle, method_name)
            return await meth(**kwargs)

        return _


class ShuffleTestPool:
    _shuffle_run_id_iterator = itertools.count()

    def __init__(self, *args, **kwargs):
        self.shuffles = {}
        super().__init__(*args, **kwargs)

    def __call__(self, addr: str, *args: Any, **kwargs: Any) -> PooledRPCShuffle:
        return PooledRPCShuffle(self.shuffles[addr])

    async def fake_broadcast(self, msg):

        op = msg.pop("op").replace("shuffle_", "")
        out = {}
        for addr, s in self.shuffles.items():
            out[addr] = await getattr(s, op)()
        return out

    def new_shuffle(
        self,
        name,
        worker_for_mapping,
        old_chunks,
        new_chunks,
        directory,
        loop,
        Shuffle=RechunkRun,
    ):
        s = Shuffle(
            worker_for=worker_for_mapping,
            # FIXME: Is output_workers redundant with worker_for?
            output_workers=set(worker_for_mapping.values()),
            old_chunks=old_chunks,
            new_chunks=new_chunks,
            directory=directory / name,
            id=ShuffleId(name),
            run_id=next(ShuffleTestPool._shuffle_run_id_iterator),
            local_address=name,
            nthreads=2,
            rpc=self,
            broadcast=self.fake_broadcast,
            memory_limiter_disk=ResourceLimiter(10000000),
            memory_limiter_comms=ResourceLimiter(10000000),
        )
        self.shuffles[name] = s
        return s


@pytest.mark.parametrize("n_workers", [1, 10])
@gen_test()
async def test_lowlevel_rechunk(tmpdir, loop_in_thread, n_workers):
    old = ((10,) * 3,)
    new = ((5,) * 6,)

    arrs = [np.random.random(csize) for csize in old[0]]

    workers = list("abcdefghijklmn")[:n_workers]

    worker_for_mapping = {}

    for i in range(len(new[0])):
        worker_for_mapping[(i,)] = get_worker_for(i, workers, len(new[0]))

    assert len(set(worker_for_mapping.values())) == min(n_workers, len(new[0]))

    local_shuffle_pool = ShuffleTestPool()
    shuffles = []
    for i in range(n_workers):
        shuffles.append(
            local_shuffle_pool.new_shuffle(
                name=workers[i],
                worker_for_mapping=worker_for_mapping,
                old_chunks=old,
                new_chunks=new,
                directory=tmpdir,
                loop=loop_in_thread,
            )
        )
    # TODO
    # random.seed(42)
    barrier_worker = shuffles[0]
    try:
        for i, arr in enumerate(arrs):
            s = shuffles[i % len(shuffles)]
            await s.add_partition(arr, (i,))

        await barrier_worker.barrier()

        total_bytes_sent = 0
        total_bytes_recvd = 0
        total_bytes_recvd_shuffle = 0
        for s in shuffles:
            metrics = s.heartbeat()
            assert metrics["comm"]["total"] == metrics["comm"]["written"]
            total_bytes_sent += metrics["comm"]["written"]
            total_bytes_recvd += metrics["disk"]["total"]
            total_bytes_recvd_shuffle += s.total_recvd

        assert total_bytes_recvd_shuffle == total_bytes_sent

        all_chunks = np.empty((len(new[0]),), dtype="O")
        for ix, worker in worker_for_mapping.items():
            s = local_shuffle_pool.shuffles[worker]
            all_chunks[ix] = await s.get_output_partition(ix)

            # all_chunks = await asyncio.gather(*all_chunks)

    finally:
        await asyncio.gather(*[s.close() for s in shuffles])
    np.testing.assert_array_equal(
        concatenate3(arrs), concatenate3(all_chunks.tolist()), strict=True
    )

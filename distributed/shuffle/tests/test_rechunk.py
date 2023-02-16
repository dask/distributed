from __future__ import annotations

import asyncio
import random

import numpy as np
import pytest

import dask.array as da
from dask.array.core import concatenate3
from dask.array.rechunk import normalize_chunks, rechunk

from distributed.shuffle._limiter import ResourceLimiter
from distributed.shuffle._scheduler_extension import get_worker_for
from distributed.shuffle._shuffle import ShuffleId
from distributed.shuffle._worker_extension import ArrayRechunkRun
from distributed.shuffle.tests.utils import AbstractShuffleTestPool
from distributed.utils_test import gen_cluster, gen_test


class ArrayRechunkTestPool(AbstractShuffleTestPool):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    def new_shuffle(
        self,
        name,
        worker_for_mapping,
        old,
        new,
        directory,
        loop,
        Shuffle=ArrayRechunkRun,
    ):
        s = Shuffle(
            worker_for=worker_for_mapping,
            # FIXME: Is output_workers redundant with worker_for?
            output_workers=set(worker_for_mapping.values()),
            old=old,
            new=new,
            directory=directory / name,
            id=ShuffleId(name),
            run_id=next(AbstractShuffleTestPool._shuffle_run_id_iterator),
            local_address=name,
            nthreads=2,
            rpc=self,
            scheduler=self,
            memory_limiter_disk=ResourceLimiter(10000000),
            memory_limiter_comms=ResourceLimiter(10000000),
        )
        self.shuffles[name] = s
        return s


from itertools import product


@pytest.mark.parametrize("n_workers", [1, 10])
@pytest.mark.parametrize("barrier_first_worker", [True, False])
@gen_test()
async def test_lowlevel_rechunk(
    tmpdir, loop_in_thread, n_workers, barrier_first_worker
):
    old = ((1, 2, 3, 4), (5,) * 6)
    new = ((5, 5), (12, 18))

    ind_chunks = [[(i, x) for i, x in enumerate(dim)] for dim in old]
    ind_chunks = [list(zip(x, y)) for x, y in product(*ind_chunks)]
    old_chunks = {idx: np.random.random(chunk) for idx, chunk in ind_chunks}

    workers = list("abcdefghijklmn")[:n_workers]

    worker_for_mapping = {}

    new_indices = list(product(*(range(len(dim)) for dim in new)))
    for i, idx in enumerate(new_indices):
        worker_for_mapping[idx] = get_worker_for(i, workers, len(new_indices))

    assert len(set(worker_for_mapping.values())) == min(n_workers, len(new_indices))

    local_shuffle_pool = ArrayRechunkTestPool()
    shuffles = []
    for i in range(n_workers):
        shuffles.append(
            local_shuffle_pool.new_shuffle(
                name=workers[i],
                worker_for_mapping=worker_for_mapping,
                old=old,
                new=new,
                directory=tmpdir,
                loop=loop_in_thread,
            )
        )
    random.seed(42)
    if barrier_first_worker:
        barrier_worker = shuffles[0]
    else:
        barrier_worker = random.sample(shuffles, k=1)[0]

    try:
        for i, (idx, arr) in enumerate(old_chunks.items()):
            s = shuffles[i % len(shuffles)]
            await s.add_partition(arr, idx)

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

        all_chunks = np.empty(tuple(len(dim) for dim in new), dtype="O")
        for ix, worker in worker_for_mapping.items():
            s = local_shuffle_pool.shuffles[worker]
            all_chunks[ix] = await s.get_output_partition(ix)

    finally:
        await asyncio.gather(*[s.close() for s in shuffles])

    old_cs = np.empty(tuple(len(dim) for dim in old), dtype="O")
    for ix, arr in old_chunks.items():
        old_cs[ix] = arr
    np.testing.assert_array_equal(
        concatenate3(old_cs.tolist()), concatenate3(all_chunks.tolist()), strict=True
    )


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_1d(c, s, *ws):
    """Try rechunking a random 1d matrix"""
    a = np.random.uniform(0, 1, 30)
    x = da.from_array(a, chunks=((10,) * 3,))
    new = ((6,) * 5,)
    x2 = rechunk(x, chunks=new, rechunk="p2p")
    assert x2.chunks == new
    assert np.all(await c.compute(x2) == a)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_2d(c, s, *ws):
    """Try rechunking a random 2d matrix"""
    a = np.random.uniform(0, 1, 300).reshape((10, 30))
    x = da.from_array(a, chunks=((1, 2, 3, 4), (5,) * 6))
    new = ((5, 5), (15,) * 2)
    x2 = rechunk(x, chunks=new, rechunk="p2p")
    assert x2.chunks == new
    assert np.all(await c.compute(x2) == a)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_4d(c, s, *ws):
    """Try rechunking a random 4d matrix"""
    old = ((5, 5),) * 4
    a = np.random.uniform(0, 1, 10000).reshape((10,) * 4)
    x = da.from_array(a, chunks=old)
    new = ((10,),) * 4
    x2 = rechunk(x, chunks=new, rechunk="p2p")
    assert x2.chunks == new
    assert np.all(await c.compute(x2) == a)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_expand(c, s, *ws):
    a = np.random.uniform(0, 1, 100).reshape((10, 10))
    x = da.from_array(a, chunks=(5, 5))
    y = x.rechunk(chunks=((3, 3, 3, 1), (3, 3, 3, 1)), rechunk="p2p")
    assert np.all(await c.compute(y) == a)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_expand2(c, s, *ws):
    (a, b) = (3, 2)
    orig = np.random.uniform(0, 1, a**b).reshape((a,) * b)
    for off, off2 in product(range(1, a - 1), range(1, a - 1)):
        old = ((a - off, off),) * b
        x = da.from_array(orig, chunks=old)
        new = ((a - off2, off2),) * b
        assert np.all(await c.compute(x.rechunk(chunks=new, rechunk="p2p")) == orig)
        if a - off - off2 > 0:
            new = ((off, a - off2 - off, off2),) * b
            y = await c.compute(x.rechunk(chunks=new, rechunk="p2p"))
            assert np.all(y == orig)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_method(c, s, *ws):
    """Test rechunking can be done as a method of dask array."""
    old = ((5, 2, 3),) * 4
    new = ((3, 3, 3, 1),) * 4
    a = np.random.uniform(0, 1, 10000).reshape((10,) * 4)
    x = da.from_array(a, chunks=old)
    x2 = x.rechunk(chunks=new, rechunk="p2p")
    assert x2.chunks == new
    assert np.all(await c.compute(x2) == a)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_blockshape(c, s, *ws):
    """Test that blockshape can be used."""
    new_shape, new_chunks = (10, 10), (4, 3)
    new_blockdims = normalize_chunks(new_chunks, new_shape)
    old_chunks = ((4, 4, 2), (3, 3, 3, 1))
    a = np.random.uniform(0, 1, 100).reshape((10, 10))
    x = da.from_array(a, chunks=old_chunks)
    check1 = rechunk(x, chunks=new_chunks, rechunk="p2p")
    assert check1.chunks == new_blockdims
    assert np.all(await c.compute(check1) == a)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_dtype(c, s, *ws):
    x = da.ones(5, chunks=(2,))
    assert x.rechunk(chunks=(1,), rechunk="p2p").dtype == x.dtype

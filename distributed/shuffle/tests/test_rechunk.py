from __future__ import annotations

import asyncio
import random
import warnings

import pytest

np = pytest.importorskip("numpy")
da = pytest.importorskip("dask.array")

from concurrent.futures import ThreadPoolExecutor

import dask
from dask.array.core import concatenate3
from dask.array.rechunk import normalize_chunks, rechunk
from dask.array.utils import assert_eq

from distributed.shuffle._limiter import ResourceLimiter
from distributed.shuffle._rechunk import ShardID, rechunk_slicing
from distributed.shuffle._scheduler_extension import get_worker_for_range_sharding
from distributed.shuffle._shuffle import ShuffleId
from distributed.shuffle._worker_extension import ArrayRechunkRun
from distributed.shuffle.tests.utils import AbstractShuffleTestPool
from distributed.utils_test import gen_cluster, gen_test, raises_with_cause


class ArrayRechunkTestPool(AbstractShuffleTestPool):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._executor = ThreadPoolExecutor(2)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        try:
            self._executor.shutdown(cancel_futures=True)
        except Exception:  # pragma: no cover
            self._executor.shutdown()

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
            executor=self._executor,
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
    tmp_path, loop_in_thread, n_workers, barrier_first_worker
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
        worker_for_mapping[idx] = get_worker_for_range_sharding(
            i, workers, len(new_indices)
        )

    assert len(set(worker_for_mapping.values())) == min(n_workers, len(new_indices))

    with ArrayRechunkTestPool() as local_shuffle_pool:
        shuffles = []
        for i in range(n_workers):
            shuffles.append(
                local_shuffle_pool.new_shuffle(
                    name=workers[i],
                    worker_for_mapping=worker_for_mapping,
                    old=old,
                    new=new,
                    directory=tmp_path,
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
            concatenate3(old_cs.tolist()),
            concatenate3(all_chunks.tolist()),
            strict=True,
        )


def test_raise_on_fuse_optimization():
    a = np.random.uniform(0, 1, 30)
    x = da.from_array(a, chunks=((10,) * 3,))
    new = ((6,) * 5,)
    with pytest.raises(RuntimeError, match="fuse optimization"):
        rechunk(x, chunks=new, method="p2p")


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_raise_on_lost_annotation(c, s, a, b):
    a = np.random.uniform(0, 1, 30)
    x = da.from_array(a, chunks=((10,) * 3,))
    new = ((6,) * 5,)
    x2 = rechunk(x, chunks=new, method="p2p")

    # Manually drop "shuffle" annotation
    for name, layer in x2.dask.layers.items():
        if name.startswith("rechunk-p2p"):
            del layer.annotations["shuffle"]

    with raises_with_cause(
        RuntimeError,
        "rechunk_transfer failed",
        RuntimeError,
        "lost ``shuffle`` annotation",
    ):
        await c.compute(x2)


@pytest.mark.parametrize("config_value", ["tasks", "p2p", None])
@pytest.mark.parametrize("keyword", ["tasks", "p2p", None])
@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_configuration(c, s, *ws, config_value, keyword):
    """Try rechunking a random 1d matrix

    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_1d
    """
    a = np.random.uniform(0, 1, 30)
    x = da.from_array(a, chunks=((10,) * 3,))
    new = ((6,) * 5,)
    config = {"array.rechunk.method": config_value} if config_value is not None else {}
    with dask.config.set(config):
        x2 = rechunk(x, chunks=new, method=keyword)
    expected_algorithm = keyword if keyword is not None else config_value
    if expected_algorithm == "p2p":
        assert all(key[0].startswith("rechunk-p2p") for key in x2.__dask_keys__())
    else:
        assert not any(key[0].startswith("rechunk-p2p") for key in x2.__dask_keys__())

    assert x2.chunks == new
    assert np.all(await c.compute(x2) == a)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_2d(c, s, *ws):
    """Try rechunking a random 2d matrix

    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_2d
    """
    a = np.random.uniform(0, 1, 300).reshape((10, 30))
    x = da.from_array(a, chunks=((1, 2, 3, 4), (5,) * 6))
    new = ((5, 5), (15,) * 2)
    x2 = rechunk(x, chunks=new, method="p2p")
    assert x2.chunks == new
    assert np.all(await c.compute(x2) == a)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_4d(c, s, *ws):
    """Try rechunking a random 4d matrix

    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_4d
    """
    old = ((5, 5),) * 4
    a = np.random.uniform(0, 1, 10000).reshape((10,) * 4)
    x = da.from_array(a, chunks=old)
    new = ((10,),) * 4
    x2 = rechunk(x, chunks=new, method="p2p")
    assert x2.chunks == new
    assert np.all(await c.compute(x2) == a)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_expand(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_expand
    """
    a = np.random.uniform(0, 1, 100).reshape((10, 10))
    x = da.from_array(a, chunks=(5, 5))
    y = x.rechunk(chunks=((3, 3, 3, 1), (3, 3, 3, 1)), method="p2p")
    assert np.all(await c.compute(y) == a)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_expand2(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_expand2
    """
    (a, b) = (3, 2)
    orig = np.random.uniform(0, 1, a**b).reshape((a,) * b)
    for off, off2 in product(range(1, a - 1), range(1, a - 1)):
        old = ((a - off, off),) * b
        x = da.from_array(orig, chunks=old)
        new = ((a - off2, off2),) * b
        assert np.all(await c.compute(x.rechunk(chunks=new, method="p2p")) == orig)
        if a - off - off2 > 0:
            new = ((off, a - off2 - off, off2),) * b
            y = await c.compute(x.rechunk(chunks=new, method="p2p"))
            assert np.all(y == orig)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_method(c, s, *ws):
    """Test rechunking can be done as a method of dask array.

    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_method
    """
    old = ((5, 2, 3),) * 4
    new = ((3, 3, 3, 1),) * 4
    a = np.random.uniform(0, 1, 10000).reshape((10,) * 4)
    x = da.from_array(a, chunks=old)
    x2 = x.rechunk(chunks=new, method="p2p")
    assert x2.chunks == new
    assert np.all(await c.compute(x2) == a)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_blockshape(c, s, *ws):
    """Test that blockshape can be used.

    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_blockshape
    """
    new_shape, new_chunks = (10, 10), (4, 3)
    new_blockdims = normalize_chunks(new_chunks, new_shape)
    old_chunks = ((4, 4, 2), (3, 3, 3, 1))
    a = np.random.uniform(0, 1, 100).reshape((10, 10))
    x = da.from_array(a, chunks=old_chunks)
    check1 = rechunk(x, chunks=new_chunks, method="p2p")
    assert check1.chunks == new_blockdims
    assert np.all(await c.compute(check1) == a)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_dtype(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_dtype
    """
    x = da.ones(5, chunks=(2,))
    assert x.rechunk(chunks=(1,), method="p2p").dtype == x.dtype


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_with_dict(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_with_dict
    """
    x = da.ones((24, 24), chunks=(4, 8))
    y = x.rechunk(chunks={0: 12}, method="p2p")
    assert y.chunks == ((12, 12), (8, 8, 8))

    x = da.ones((24, 24), chunks=(4, 8))
    y = x.rechunk(chunks={0: (12, 12)}, method="p2p")
    assert y.chunks == ((12, 12), (8, 8, 8))

    x = da.ones((24, 24), chunks=(4, 8))
    y = x.rechunk(chunks={0: -1}, method="p2p")
    assert y.chunks == ((24,), (8, 8, 8))

    x = da.ones((24, 24), chunks=(4, 8))
    y = x.rechunk(chunks={0: None, 1: "auto"}, method="p2p")
    assert y.chunks == ((4, 4, 4, 4, 4, 4), (24,))


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_with_empty_input(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_with_empty_input
    """
    x = da.ones((24, 24), chunks=(4, 8))
    assert x.rechunk(chunks={}, method="p2p").chunks == x.chunks
    with pytest.raises(ValueError):
        x.rechunk(chunks=(), method="p2p")


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_with_null_dimensions(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_with_null_dimensions
    """
    x = da.from_array(np.ones((24, 24)), chunks=(4, 8))
    assert (
        x.rechunk(chunks=(None, 4), method="p2p").chunks
        == da.ones((24, 24), chunks=(4, 4)).chunks
    )
    assert (
        x.rechunk(chunks={0: None, 1: 4}, method="p2p").chunks
        == da.ones((24, 24), chunks=(4, 4)).chunks
    )


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_with_integer(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_with_integer
    """
    x = da.from_array(np.arange(5), chunks=4)
    y = x.rechunk(3, method="p2p")
    assert y.chunks == ((3, 2),)
    assert (await c.compute(x) == await c.compute(y)).all()


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_0d(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_0d
    """
    a = np.array(42)
    x = da.from_array(a, chunks=())
    y = x.rechunk((), method="p2p")
    assert y.chunks == ()
    assert await c.compute(y) == a


@pytest.mark.parametrize(
    "arr", [da.array([]), da.array([[], []]), da.array([[[]], [[]]])]
)
@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_empty_array(c, s, *ws, arr):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_empty_array
    """
    arr.rechunk(method="p2p")
    assert arr.size == 0


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_empty(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_empty
    """
    x = da.ones((0, 10), chunks=(5, 5))
    y = x.rechunk((2, 2), method="p2p")
    assert y.chunks == ((0,), (2,) * 5)
    assert_eq(await c.compute(x), await c.compute(y))


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_zero_dim_array(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_zero_dim_array
    """
    x = da.zeros((4, 0), chunks=3)
    y = x.rechunk({0: 4}, method="p2p")
    assert y.chunks == ((4,), (0,))
    assert_eq(await c.compute(x), await c.compute(y))


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_zero_dim_array_II(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_zero_dim_array_II
    """
    x = da.zeros((4, 0, 6, 10), chunks=3)
    y = x.rechunk({0: 4, 2: 2}, method="p2p")
    assert y.chunks == ((4,), (0,), (2, 2, 2), (3, 3, 3, 1))
    assert_eq(await c.compute(x), await c.compute(y))


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_same(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_same
    """
    x = da.ones((24, 24), chunks=(4, 8))
    y = x.rechunk(x.chunks, method="p2p")
    assert x is y


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_with_zero_placeholders(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_with_zero_placeholders
    """
    x = da.ones((24, 24), chunks=((12, 12), (24, 0)))
    y = da.ones((24, 24), chunks=((12, 12), (12, 12)))
    y = y.rechunk(((12, 12), (24, 0)), method="p2p")
    assert x.chunks == y.chunks


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_minus_one(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_minus_one
    """
    x = da.ones((24, 24), chunks=(4, 8))
    y = x.rechunk((-1, 8), method="p2p")
    assert y.chunks == ((24,), (8, 8, 8))
    assert_eq(await c.compute(x), await c.compute(y))


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_warning(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_warning
    """
    N = 20
    x = da.random.normal(size=(N, N, 100), chunks=(1, N, 100))
    with warnings.catch_warnings(record=True) as w:
        x = x.rechunk((N, 1, 100), method="p2p")
    assert not w


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_unknown_from_pandas(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_unknown_from_pandas
    """
    dd = pytest.importorskip("dask.dataframe")
    pd = pytest.importorskip("pandas")

    arr = np.random.randn(50, 10)
    x = dd.from_pandas(pd.DataFrame(arr), 2).values
    result = x.rechunk((None, (5, 5)), method="p2p")
    assert np.isnan(x.chunks[0]).all()
    assert np.isnan(result.chunks[0]).all()
    assert result.chunks[1] == (5, 5)
    expected = da.from_array(arr, chunks=((25, 25), (10,))).rechunk(
        (None, (5, 5)), method="p2p"
    )
    assert_eq(await c.compute(result), await c.compute(expected))


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_unknown_from_array(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_unknown_from_array
    """
    dd = pytest.importorskip("dask.dataframe")
    x = dd.from_array(da.ones(shape=(4, 4), chunks=(2, 2))).values
    result = x.rechunk((None, 4), method="p2p")
    assert np.isnan(x.chunks[0]).all()
    assert np.isnan(result.chunks[0]).all()
    assert x.chunks[1] == (4,)
    assert_eq(await c.compute(x), await c.compute(result))


@pytest.mark.parametrize(
    "x, chunks",
    [
        (da.ones(shape=(50, 10), chunks=(25, 10)), (None, 5)),
        (da.ones(shape=(50, 10), chunks=(25, 10)), {1: 5}),
        (da.ones(shape=(50, 10), chunks=(25, 10)), (None, (5, 5))),
        (da.ones(shape=(1000, 10), chunks=(5, 10)), (None, 5)),
        (da.ones(shape=(1000, 10), chunks=(5, 10)), {1: 5}),
        (da.ones(shape=(1000, 10), chunks=(5, 10)), (None, (5, 5))),
        (da.ones(shape=(10, 10), chunks=(10, 10)), (None, 5)),
        (da.ones(shape=(10, 10), chunks=(10, 10)), {1: 5}),
        (da.ones(shape=(10, 10), chunks=(10, 10)), (None, (5, 5))),
        (da.ones(shape=(10, 10), chunks=(10, 2)), (None, 5)),
        (da.ones(shape=(10, 10), chunks=(10, 2)), {1: 5}),
        (da.ones(shape=(10, 10), chunks=(10, 2)), (None, (5, 5))),
    ],
)
@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_unknown(c, s, *ws, x, chunks):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_unknown
    """
    dd = pytest.importorskip("dask.dataframe")
    y = dd.from_array(x).values
    result = y.rechunk(chunks, method="p2p")
    expected = x.rechunk(chunks, method="p2p")

    assert_chunks_match(result.chunks, expected.chunks)
    assert_eq(await c.compute(result), await c.compute(expected))


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_unknown_explicit(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_unknown_explicit
    """
    dd = pytest.importorskip("dask.dataframe")
    x = da.ones(shape=(10, 10), chunks=(5, 2))
    y = dd.from_array(x).values
    result = y.rechunk(((float("nan"), float("nan")), (5, 5)), method="p2p")
    expected = x.rechunk((None, (5, 5)), method="p2p")
    assert_chunks_match(result.chunks, expected.chunks)
    assert_eq(await c.compute(result), await c.compute(expected))


def assert_chunks_match(left, right):
    for x, y in zip(left, right):
        if np.isnan(x).any():
            assert np.isnan(x).all()
        else:
            assert x == y


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_unknown_raises(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_unknown_raises
    """
    dd = pytest.importorskip("dask.dataframe")

    x = dd.from_array(da.ones(shape=(10, 10), chunks=(5, 5))).values
    with pytest.raises(ValueError):
        x.rechunk((None, (5, 5, 5)), method="p2p")


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_zero_dim(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_zero_dim
    """
    da = pytest.importorskip("dask.array")

    x = da.ones((0, 10, 100), chunks=(0, 10, 10)).rechunk((0, 10, 50), method="p2p")
    assert len(await c.compute(x)) == 0


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_empty_chunks(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_empty_chunks
    """
    x = da.zeros((7, 24), chunks=((7,), (10, 0, 0, 9, 0, 5)))
    y = x.rechunk((2, 3), method="p2p")
    assert_eq(await c.compute(x), await c.compute(y))


@pytest.mark.skip(reason="FIXME: We should avoid P2P in this case")
@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_avoid_needless_chunking(c, s, *ws):
    x = da.ones(16, chunks=2)
    y = x.rechunk(8, method="p2p")
    dsk = y.__dask_graph__()
    assert len(dsk) <= 8 + 2


@pytest.mark.parametrize(
    "shape,chunks,bs,expected",
    [
        (100, 1, 10, (10,) * 10),
        (100, 50, 10, (10,) * 10),
        (100, 100, 10, (10,) * 10),
        (20, 7, 10, (7, 7, 6)),
        (20, (1, 1, 1, 1, 6, 2, 1, 7), 5, (5, 5, 5, 5)),
    ],
)
@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_auto_1d(c, s, *ws, shape, chunks, bs, expected):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_auto_1d
    """
    x = da.ones(shape, chunks=(chunks,))
    y = x.rechunk({0: "auto"}, block_size_limit=bs * x.dtype.itemsize, method="p2p")
    assert y.chunks == (expected,)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_auto_2d(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_auto_2d
    """
    x = da.ones((20, 20), chunks=(2, 2))
    y = x.rechunk(
        {0: -1, 1: "auto"}, block_size_limit=20 * x.dtype.itemsize, method="p2p"
    )
    assert y.chunks == ((20,), (1,) * 20)

    x = da.ones((20, 20), chunks=(2, 2))
    y = x.rechunk((-1, "auto"), block_size_limit=80 * x.dtype.itemsize, method="p2p")
    assert y.chunks == ((20,), (4,) * 5)

    x = da.ones((20, 20), chunks=((2, 2)))
    y = x.rechunk({0: "auto"}, block_size_limit=20 * x.dtype.itemsize, method="p2p")
    assert y.chunks[1] == x.chunks[1]
    assert y.chunks[0] == (10, 10)

    x = da.ones((20, 20), chunks=((2,) * 10, (2, 2, 2, 2, 2, 5, 5)))
    y = x.rechunk({0: "auto"}, block_size_limit=20 * x.dtype.itemsize, method="p2p")
    assert y.chunks[1] == x.chunks[1]
    assert y.chunks[0] == (4, 4, 4, 4, 4)  # limited by largest


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_auto_3d(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_auto_3d
    """
    x = da.ones((20, 20, 20), chunks=((2, 2, 2)))
    y = x.rechunk(
        {0: "auto", 1: "auto"}, block_size_limit=200 * x.dtype.itemsize, method="p2p"
    )
    assert y.chunks[2] == x.chunks[2]
    assert y.chunks[0] == (10, 10)
    assert y.chunks[1] == (10, 10)  # even split


@pytest.mark.parametrize("n", [100, 1000])
@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_auto_image_stack(c, s, *ws, n):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_auto_image_stack
    """
    with dask.config.set({"array.chunk-size": "10MiB"}):
        x = da.ones((n, 1000, 1000), chunks=(1, 1000, 1000), dtype="uint8")
        y = x.rechunk("auto", method="p2p")
        assert y.chunks == ((10,) * (n // 10), (1000,), (1000,))
        assert y.rechunk("auto", method="p2p").chunks == y.chunks  # idempotent

    with dask.config.set({"array.chunk-size": "7MiB"}):
        z = x.rechunk("auto", method="p2p")
        if n == 100:
            assert z.chunks == ((7,) * 14 + (2,), (1000,), (1000,))
        else:
            assert z.chunks == ((7,) * 142 + (6,), (1000,), (1000,))

    with dask.config.set({"array.chunk-size": "1MiB"}):
        x = da.ones((n, 1000, 1000), chunks=(1, 1000, 1000), dtype="float64")
        z = x.rechunk("auto", method="p2p")
        assert z.chunks == ((1,) * n, (362, 362, 276), (362, 362, 276))


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_down(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_down
    """
    with dask.config.set({"array.chunk-size": "10MiB"}):
        x = da.ones((100, 1000, 1000), chunks=(1, 1000, 1000), dtype="uint8")
        y = x.rechunk("auto", method="p2p")
        assert y.chunks == ((10,) * 10, (1000,), (1000,))

    with dask.config.set({"array.chunk-size": "1MiB"}):
        z = y.rechunk("auto", method="p2p")
        assert z.chunks == ((4,) * 25, (511, 489), (511, 489))

    with dask.config.set({"array.chunk-size": "1MiB"}):
        z = y.rechunk({0: "auto"}, method="p2p")
        assert z.chunks == ((1,) * 100, (1000,), (1000,))

        z = y.rechunk({1: "auto"}, method="p2p")
        assert z.chunks == ((10,) * 10, (104,) * 9 + (64,), (1000,))


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_zero(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_zero
    """
    with dask.config.set({"array.chunk-size": "1B"}):
        x = da.ones(10, chunks=(5,))
        y = x.rechunk("auto", method="p2p")
        assert y.chunks == ((1,) * 10,)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_bad_keys(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_bad_keys
    """
    x = da.zeros((2, 3, 4), chunks=1)
    assert x.rechunk({-1: 4}, method="p2p").chunks == ((1, 1), (1, 1, 1), (4,))
    assert x.rechunk({-x.ndim: 2}, method="p2p").chunks == (
        (2,),
        (1, 1, 1),
        (1, 1, 1, 1),
    )

    with pytest.raises(TypeError) as info:
        x.rechunk({"blah": 4}, method="p2p")

    assert "blah" in str(info.value)

    with pytest.raises(ValueError) as info:
        x.rechunk({100: 4}, method="p2p")

    assert "100" in str(info.value)

    with pytest.raises(ValueError) as info:
        x.rechunk({-100: 4}, method="p2p")

    assert "-100" in str(info.value)


@gen_cluster(client=True, config={"optimization.fuse.active": False})
async def test_rechunk_with_zero(c, s, *ws):
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_rechunk_with_zero
    """
    a = da.ones((8, 8), chunks=(4, 4))
    result = a.rechunk(((4, 4), (4, 0, 0, 4)), method="p2p")
    expected = da.ones((8, 8), chunks=((4, 4), (4, 0, 0, 4)))

    # reverse:
    a, expected = expected, a
    result = a.rechunk((4, 4), method="p2p")
    assert_eq(await c.compute(result), await c.compute(expected))


def test_rechunk_slicing_1():
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_intersect_1
    """
    old = ((10, 10, 10, 10, 10),)
    new = ((25, 5, 20),)
    result = rechunk_slicing(old, new)
    expected = {
        (0,): [(ShardID((0,), (0,)), (slice(0, 10, None),))],
        (1,): [(ShardID((0,), (1,)), (slice(0, 10, None),))],
        (2,): [
            (ShardID((0,), (2,)), (slice(0, 5, None),)),
            (ShardID((1,), (0,)), (slice(5, 10, None),)),
        ],
        (3,): [(ShardID((2,), (0,)), (slice(0, 10, None),))],
        (4,): [(ShardID((2,), (1,)), (slice(0, 10, None),))],
    }
    assert result == expected


def test_rechunk_slicing_2():
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_intersect_2
    """
    old = ((20, 20, 20, 20, 20),)
    new = ((58, 4, 20, 18),)
    result = rechunk_slicing(old, new)
    expected = {
        (0,): [(ShardID((0,), (0,)), (slice(0, 20, None),))],
        (1,): [(ShardID((0,), (1,)), (slice(0, 20, None),))],
        (2,): [
            (ShardID((0,), (2,)), (slice(0, 18, None),)),
            (ShardID((1,), (0,)), (slice(18, 20, None),)),
        ],
        (3,): [
            (ShardID((1,), (1,)), (slice(0, 2, None),)),
            (ShardID((2,), (0,)), (slice(2, 20, None),)),
        ],
        (4,): [
            (ShardID((2,), (1,)), (slice(0, 2, None),)),
            (ShardID((3,), (0,)), (slice(2, 20, None),)),
        ],
    }
    assert result == expected


def test_rechunk_slicing_nan():
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_intersect_nan
    """
    old_chunks = ((float("nan"), float("nan")), (8,))
    new_chunks = ((float("nan"), float("nan")), (4, 4))
    result = rechunk_slicing(old_chunks, new_chunks)
    expected = {
        (0, 0): [
            (
                ShardID((0, 0), (0, 0)),
                (slice(0, None, None), slice(0, 4, None)),
            ),
            (
                ShardID((0, 1), (0, 0)),
                (slice(0, None, None), slice(4, 8, None)),
            ),
        ],
        (1, 0): [
            (ShardID((1, 0), (0, 0)), (slice(0, None, None), slice(0, 4, None))),
            (ShardID((1, 1), (0, 0)), (slice(0, None, None), slice(4, 8, None))),
        ],
    }
    assert result == expected


def test_rechunk_slicing_nan_single():
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_intersect_nan_single
    """
    old_chunks = ((float("nan"),), (10,))
    new_chunks = ((float("nan"),), (5, 5))

    result = rechunk_slicing(old_chunks, new_chunks)
    expected = {
        (0, 0): [
            (ShardID((0, 0), (0, 0)), (slice(0, None, None), slice(0, 5, None))),
            (ShardID((0, 1), (0, 0)), (slice(0, None, None), slice(5, 10, None))),
        ],
    }
    assert result == expected


def test_rechunk_slicing_nan_long():
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_intersect_nan_long
    """
    old_chunks = (tuple([float("nan")] * 4), (10,))
    new_chunks = (tuple([float("nan")] * 4), (5, 5))
    result = rechunk_slicing(old_chunks, new_chunks)
    expected = {
        (0, 0): [
            (ShardID((0, 0), (0, 0)), (slice(0, None, None), slice(0, 5, None))),
            (ShardID((0, 1), (0, 0)), (slice(0, None, None), slice(5, 10, None))),
        ],
        (1, 0): [
            (ShardID((1, 0), (0, 0)), (slice(0, None, None), slice(0, 5, None))),
            (ShardID((1, 1), (0, 0)), (slice(0, None, None), slice(5, 10, None))),
        ],
        (2, 0): [
            (ShardID((2, 0), (0, 0)), (slice(0, None, None), slice(0, 5, None))),
            (ShardID((2, 1), (0, 0)), (slice(0, None, None), slice(5, 10, None))),
        ],
        (3, 0): [
            (ShardID((3, 0), (0, 0)), (slice(0, None, None), slice(0, 5, None))),
            (ShardID((3, 1), (0, 0)), (slice(0, None, None), slice(5, 10, None))),
        ],
    }
    assert result == expected


def test_rechunk_slicing_chunks_with_nonzero():
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_intersect_chunks_with_nonzero
    """
    old = ((4, 4), (2,))
    new = ((8,), (1, 1))
    result = rechunk_slicing(old, new)
    expected = {
        (0, 0): [
            (ShardID((0, 0), (0, 0)), (slice(0, 4, None), slice(0, 1, None))),
            (ShardID((0, 1), (0, 0)), (slice(0, 4, None), slice(1, 2, None))),
        ],
        (1, 0): [
            (ShardID((0, 0), (1, 0)), (slice(0, 4, None), slice(0, 1, None))),
            (ShardID((0, 1), (1, 0)), (slice(0, 4, None), slice(1, 2, None))),
        ],
    }
    assert result == expected


def test_rechunk_slicing_chunks_with_zero():
    """
    See Also
    --------
    dask.array.tests.test_rechunk.test_intersect_chunks_with_zero
    """
    old = ((4, 4), (2,))
    new = ((4, 0, 0, 4), (1, 1))
    result = rechunk_slicing(old, new)

    expected = {
        (0, 0): [
            (ShardID((0, 0), (0, 0)), (slice(0, 4, None), slice(0, 1, None))),
            (ShardID((0, 1), (0, 0)), (slice(0, 4, None), slice(1, 2, None))),
        ],
        (1, 0): [
            # FIXME: We should probably filter these out to avoid sending empty shards
            (ShardID((1, 0), (0, 0)), (slice(0, 0, None), slice(0, 1, None))),
            (ShardID((1, 1), (0, 0)), (slice(0, 0, None), slice(1, 2, None))),
            (ShardID((2, 0), (0, 0)), (slice(0, 0, None), slice(0, 1, None))),
            (ShardID((2, 1), (0, 0)), (slice(0, 0, None), slice(1, 2, None))),
            (ShardID((3, 0), (0, 0)), (slice(0, 4, None), slice(0, 1, None))),
            (ShardID((3, 1), (0, 0)), (slice(0, 4, None), slice(1, 2, None))),
        ],
    }

    assert result == expected

    old = ((4, 0, 0, 4), (1, 1))
    new = ((4, 4), (2,))
    result = rechunk_slicing(old, new)

    expected = {
        (0, 0): [
            (ShardID((0, 0), (0, 0)), (slice(0, 4, None), slice(0, 1, None))),
        ],
        (0, 1): [
            (ShardID((0, 0), (0, 1)), (slice(0, 4, None), slice(0, 1, None))),
        ],
        (3, 0): [
            (ShardID((1, 0), (0, 0)), (slice(0, 4, None), slice(0, 1, None))),
        ],
        (3, 1): [
            (ShardID((1, 0), (0, 1)), (slice(0, 4, None), slice(0, 1, None))),
        ],
    }

    assert result == expected

    old = ((4, 4), (2,))
    new = ((2, 0, 0, 2, 4), (1, 1))
    result = rechunk_slicing(old, new)
    expected = {
        (0, 0): [
            (ShardID((0, 0), (0, 0)), (slice(0, 2, None), slice(0, 1, None))),
            (ShardID((0, 1), (0, 0)), (slice(0, 2, None), slice(1, 2, None))),
            # FIXME: We should probably filter these out to avoid sending empty shards
            (ShardID((1, 0), (0, 0)), (slice(2, 2, None), slice(0, 1, None))),
            (ShardID((1, 1), (0, 0)), (slice(2, 2, None), slice(1, 2, None))),
            (ShardID((2, 0), (0, 0)), (slice(2, 2, None), slice(0, 1, None))),
            (ShardID((2, 1), (0, 0)), (slice(2, 2, None), slice(1, 2, None))),
            (ShardID((3, 0), (0, 0)), (slice(2, 4, None), slice(0, 1, None))),
            (ShardID((3, 1), (0, 0)), (slice(2, 4, None), slice(1, 2, None))),
        ],
        (1, 0): [
            (ShardID((4, 0), (0, 0)), (slice(0, 4, None), slice(0, 1, None))),
            (ShardID((4, 1), (0, 0)), (slice(0, 4, None), slice(1, 2, None))),
        ],
    }

    assert result == expected

    old = ((4, 4), (2,))
    new = ((0, 0, 4, 4), (1, 1))
    result = rechunk_slicing(old, new)
    expected = {
        (0, 0): [
            # FIXME: We should probably filter these out to avoid sending empty shards
            (ShardID((0, 0), (0, 0)), (slice(0, 0, None), slice(0, 1, None))),
            (ShardID((0, 1), (0, 0)), (slice(0, 0, None), slice(1, 2, None))),
            (ShardID((1, 0), (0, 0)), (slice(0, 0, None), slice(0, 1, None))),
            (ShardID((1, 1), (0, 0)), (slice(0, 0, None), slice(1, 2, None))),
            (ShardID((2, 0), (0, 0)), (slice(0, 4, None), slice(0, 1, None))),
            (ShardID((2, 1), (0, 0)), (slice(0, 4, None), slice(1, 2, None))),
        ],
        (1, 0): [
            (ShardID((3, 0), (0, 0)), (slice(0, 4, None), slice(0, 1, None))),
            (ShardID((3, 1), (0, 0)), (slice(0, 4, None), slice(1, 2, None))),
        ],
    }

    assert result == expected

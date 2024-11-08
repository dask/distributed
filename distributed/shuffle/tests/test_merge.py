from __future__ import annotations

import asyncio
import contextlib
from typing import Any
from unittest import mock

import pytest

from dask.typing import Key

from distributed import Worker
from distributed.shuffle._core import ShuffleId, id_from_key
from distributed.shuffle._worker_plugin import ShuffleRun, _ShuffleRunManager
from distributed.utils_test import gen_cluster

pd = pytest.importorskip("pandas")
import dask
import dask.dataframe as dd
from dask.dataframe._compat import tm
from dask.dataframe.utils import assert_eq

from distributed import get_client

try:
    import pyarrow as pa
except ImportError:
    pa = None

pytestmark = pytest.mark.ci1


async def list_eq(a, b):
    c = get_client()
    a = await c.compute(a) if isinstance(a, dd.DataFrame) else a
    b = await c.compute(b) if isinstance(b, dd.DataFrame) else b
    tm.assert_index_equal(a.columns, b.columns)

    if isinstance(a, pd.DataFrame):
        av = a.sort_values(list(a.columns)).values
        bv = b.sort_values(list(b.columns)).values
    else:
        av = a.sort_values().values
        bv = b.sort_values().values

    dd._compat.assert_numpy_array_equal(av, bv)


@pytest.mark.skipif(dd._dask_expr_enabled(), reason="pyarrow>=7.0.0 already required")
@gen_cluster(client=True)
async def test_minimal_version(c, s, a, b):
    no_pyarrow_ctx = (
        mock.patch.dict("sys.modules", {"pyarrow": None})
        if pa is not None
        else contextlib.nullcontext()
    )
    with no_pyarrow_ctx:
        A = pd.DataFrame({"x": [1, 2, 3, 4, 5, 6], "y": [1, 1, 2, 2, 3, 4]})
        a = dd.repartition(A, [0, 4, 5])

        B = pd.DataFrame({"y": [1, 3, 4, 4, 5, 6], "z": [6, 5, 4, 3, 2, 1]})
        b = dd.repartition(B, [0, 2, 5])

        with (
            pytest.raises(ModuleNotFoundError, match="requires pyarrow"),
            dask.config.set({"dataframe.shuffle.method": "p2p"}),
        ):
            await c.compute(dd.merge(a, b, left_on="x", right_on="z"))


@pytest.mark.parametrize("how", ["inner", "left", "right", "outer"])
@gen_cluster(client=True)
async def test_basic_merge(c, s, a, b, how):
    A = pd.DataFrame({"x": [1, 2, 3, 4, 5, 6], "y": [1, 1, 2, 2, 3, 4]})
    a = dd.repartition(A, [0, 4, 5])

    B = pd.DataFrame({"y": [1, 3, 4, 4, 5, 6], "z": [6, 5, 4, 3, 2, 1]})
    b = dd.repartition(B, [0, 2, 5])

    joined = a.merge(b, left_on="y", right_on="y", how=how)

    if dd._dask_expr_enabled():
        # Ensure we're using a hash join
        from dask_expr._merge import HashJoinP2P

        assert any(
            isinstance(expr, HashJoinP2P) for expr in joined.optimize()._expr.walk()
        )

    expected = pd.merge(A, B, how, "y")
    await list_eq(joined, expected)

    # Different columns and npartitions
    joined = a.merge(b, left_on="x", right_on="z", how="outer")

    expected = pd.merge(A, B, "outer", None, "x", "z")
    await list_eq(joined, expected)

    assert (
        a.merge(b, left_on="y", right_on="y", how="inner")._name
        == a.merge(b, left_on="y", right_on="y", how="inner")._name
    )
    assert (
        a.merge(b, left_on="y", right_on="y", how="inner")._name
        != a.merge(b, left_on="y", right_on="y", how="outer")._name
    )


@gen_cluster(client=True)
async def test_merge_p2p_shuffle_reused_dataframe_with_different_parameters(c, s, a, b):
    pdf1 = pd.DataFrame({"a": range(100), "b": range(0, 200, 2)})
    pdf2 = pd.DataFrame({"x": range(200), "y": [1, 2, 3, 4] * 50})
    ddf1 = dd.from_pandas(pdf1, npartitions=5)
    ddf2 = dd.from_pandas(pdf2, npartitions=10)

    with dask.config.set({"dataframe.shuffle.method": "p2p"}):
        out = (
            ddf1.merge(ddf2, left_on="a", right_on="x")
            # Vary the number of output partitions for the shuffles of dd2
            .repartition(npartitions=20).merge(ddf2, left_on="b", right_on="x")
        )
    # Generate unique shuffle IDs if the input frame is the same but
    # parameters differ. Reusing shuffles in merges is dangerous because of the
    # required coordination and complexity introduced through dynamic clusters.
    assert sum(id_from_key(k) is not None for k in out.dask) == 4
    result = await c.compute(out)
    expected = pdf1.merge(pdf2, left_on="a", right_on="x").merge(
        pdf2, left_on="b", right_on="x"
    )
    dd.assert_eq(result, expected, check_index=False)


@gen_cluster(client=True)
async def test_merge_p2p_shuffle_reused_dataframe_with_same_parameters(c, s, a, b):
    pdf1 = pd.DataFrame({"a": range(100), "b": range(0, 200, 2)})
    pdf2 = pd.DataFrame({"x": range(200), "y": [1, 2, 3, 4] * 50})
    ddf1 = dd.from_pandas(pdf1, npartitions=5)
    ddf2 = dd.from_pandas(pdf2, npartitions=10)

    # This performs two shuffles:
    #   * ddf1 is shuffled on `a`
    #   * ddf2 is shuffled on `x`
    with dask.config.set({"dataframe.shuffle.method": "p2p"}):
        ddf3 = ddf1.merge(
            ddf2,
            left_on="a",
            right_on="x",
        )

    # This performs one shuffle:
    #   * ddf3 is shuffled on `b`
    # We can reuse the shuffle of dd2 on `x` from the previous merge.
    with dask.config.set({"dataframe.shuffle.method": "p2p"}):
        out = ddf2.merge(
            ddf3,
            left_on="x",
            right_on="b",
        )
    # Generate unique shuffle IDs if the input frame is the same and all its
    # parameters match. Reusing shuffles in merges is dangerous because of the
    # required coordination and complexity introduced through dynamic clusters.
    assert sum(id_from_key(k) is not None for k in out.dask) == 4
    result = await c.compute(out)
    expected = pdf2.merge(
        pdf1.merge(pdf2, left_on="a", right_on="x"), left_on="x", right_on="b"
    )
    dd.assert_eq(result, expected, check_index=False)


@pytest.mark.parametrize("how", ["inner", "outer", "left", "right"])
@pytest.mark.parametrize("disk", [True, False])
@gen_cluster(client=True)
async def test_merge(c, s, a, b, how, disk):
    A = pd.DataFrame({"x": [1, 2, 3, 4, 5, 6], "y": [1, 1, 2, 2, 3, 4]})
    a = dd.repartition(A, [0, 4, 5])

    B = pd.DataFrame({"y": [1, 3, 4, 4, 5, 6], "z": [6, 5, 4, 3, 2, 1]})
    b = dd.repartition(B, [0, 2, 5])

    with dask.config.set({"dataframe.shuffle.method": "p2p"}):
        with dask.config.set({"distributed.p2p.storage.disk": disk}):
            joined = dd.merge(a, b, left_index=True, right_index=True, how=how)
        res = await c.compute(joined)
        assert_eq(
            res,
            pd.merge(A, B, left_index=True, right_index=True, how=how),
        )
        joined = dd.merge(a, b, on="y", how=how)
        await list_eq(joined, pd.merge(A, B, on="y", how=how))
        assert all(d is None for d in joined.divisions)

        await list_eq(
            dd.merge(a, b, left_on="x", right_on="z", how=how),
            pd.merge(A, B, left_on="x", right_on="z", how=how),
        )
        await list_eq(
            dd.merge(a, b, left_on="x", right_on="z", how=how, suffixes=("1", "2")),
            pd.merge(A, B, left_on="x", right_on="z", how=how, suffixes=("1", "2")),
        )

        await list_eq(dd.merge(a, b, how=how), pd.merge(A, B, how=how))
        await list_eq(dd.merge(a, B, how=how), pd.merge(A, B, how=how))
        await list_eq(dd.merge(A, b, how=how), pd.merge(A, B, how=how))
        await list_eq(dd.merge(A, B, how=how), pd.merge(A, B, how=how))
        await list_eq(
            dd.merge(a, b, left_index=True, right_index=True, how=how),
            pd.merge(A, B, left_index=True, right_index=True, how=how),
        )
        await list_eq(
            dd.merge(
                a, b, left_index=True, right_index=True, how=how, suffixes=("1", "2")
            ),
            pd.merge(
                A, B, left_index=True, right_index=True, how=how, suffixes=("1", "2")
            ),
        )

        await list_eq(
            dd.merge(a, b, left_on="x", right_index=True, how=how),
            pd.merge(A, B, left_on="x", right_index=True, how=how),
        )
        await list_eq(
            dd.merge(a, b, left_on="x", right_index=True, how=how, suffixes=("1", "2")),
            pd.merge(A, B, left_on="x", right_index=True, how=how, suffixes=("1", "2")),
        )


@pytest.mark.slow
@gen_cluster(client=True, timeout=120)
@pytest.mark.parametrize("how", ["inner", "outer", "left", "right"])
async def test_merge_by_multiple_columns(c, s, a, b, how):
    # warnings here from pandas
    pdf1l = pd.DataFrame(
        {
            "a": list("abcdefghij"),
            "b": list("abcdefghij"),
            "c": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        },
        index=list("abcdefghij"),
    )
    pdf1r = pd.DataFrame(
        {
            "d": list("abcdefghij"),
            "e": list("abcdefghij"),
            "f": [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
        },
        index=list("abcdefghij"),
    )

    pdf2l = pd.DataFrame(
        {
            "a": list("abcdeabcde"),
            "b": list("abcabcabca"),
            "c": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        },
        index=list("abcdefghij"),
    )
    pdf2r = pd.DataFrame(
        {
            "d": list("edcbaedcba"),
            "e": list("aaabbbcccd"),
            "f": [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
        },
        index=list("fghijklmno"),
    )

    pdf3l = pd.DataFrame(
        {
            "a": list("aaaaaaaaaa"),
            "b": list("aaaaaaaaaa"),
            "c": [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
        },
        index=list("abcdefghij"),
    )
    pdf3r = pd.DataFrame(
        {
            "d": list("aaabbbccaa"),
            "e": list("abbbbbbbbb"),
            "f": [10, 9, 8, 7, 6, 5, 4, 3, 2, 1],
        },
        index=list("ABCDEFGHIJ"),
    )

    for pdl, pdr in [(pdf1l, pdf1r), (pdf2l, pdf2r), (pdf3l, pdf3r)]:
        for lpart, rpart in [(2, 2), (3, 2), (2, 3)]:
            ddl = dd.from_pandas(pdl, lpart)
            ddr = dd.from_pandas(pdr, rpart)

            with dask.config.set({"dataframe.shuffle.method": "p2p"}):
                expected = pdl.join(pdr, how=how)
                assert_eq(
                    await c.compute(ddl.join(ddr, how=how)),
                    expected,
                    # FIXME: There's an discrepancy with an empty index for
                    # pandas=2.0 (xref https://github.com/dask/dask/issues/9957).
                    # Temporarily avoid index check until the discrepancy is fixed.
                    check_index=not expected.index.empty,
                )

                expected = pdr.join(pdl, how=how)
                assert_eq(
                    await c.compute(ddr.join(ddl, how=how)),
                    expected,
                    # FIXME: There's an discrepancy with an empty index for
                    # pandas=2.0 (xref https://github.com/dask/dask/issues/9957).
                    # Temporarily avoid index check until the discrepancy is fixed.
                    check_index=not expected.index.empty,
                )

                expected = pd.merge(
                    pdl, pdr, how=how, left_index=True, right_index=True
                )
                assert_eq(
                    await c.compute(
                        dd.merge(
                            ddl,
                            ddr,
                            how=how,
                            left_index=True,
                            right_index=True,
                        )
                    ),
                    expected,
                    # FIXME: There's an discrepancy with an empty index for
                    # pandas=2.0 (xref https://github.com/dask/dask/issues/9957).
                    # Temporarily avoid index check until the discrepancy is fixed.
                    check_index=not expected.index.empty,
                )

                expected = pd.merge(
                    pdr, pdl, how=how, left_index=True, right_index=True
                )
                assert_eq(
                    await c.compute(
                        dd.merge(
                            ddr,
                            ddl,
                            how=how,
                            left_index=True,
                            right_index=True,
                        )
                    ),
                    expected,
                    # FIXME: There's an discrepancy with an empty index for
                    # pandas=2.0 (xref https://github.com/dask/dask/issues/9957).
                    # Temporarily avoid index check until the discrepancy is fixed.
                    check_index=not expected.index.empty,
                )

                # hash join
                await list_eq(
                    dd.merge(ddl, ddr, how=how, left_on="a", right_on="d"),
                    pd.merge(pdl, pdr, how=how, left_on="a", right_on="d"),
                )
                await list_eq(
                    dd.merge(ddl, ddr, how=how, left_on="b", right_on="e"),
                    pd.merge(pdl, pdr, how=how, left_on="b", right_on="e"),
                )
                await list_eq(
                    dd.merge(ddr, ddl, how=how, left_on="d", right_on="a"),
                    pd.merge(pdr, pdl, how=how, left_on="d", right_on="a"),
                )
                await list_eq(
                    dd.merge(ddr, ddl, how=how, left_on="e", right_on="b"),
                    pd.merge(pdr, pdl, how=how, left_on="e", right_on="b"),
                )
                await list_eq(
                    dd.merge(
                        ddl, ddr, how=how, left_on=["a", "b"], right_on=["d", "e"]
                    ),
                    pd.merge(
                        pdl, pdr, how=how, left_on=["a", "b"], right_on=["d", "e"]
                    ),
                )


@pytest.mark.parametrize("how", ["inner", "left", "right", "outer"])
@gen_cluster(client=True)
async def test_index_merge_p2p(c, s, a, b, how):
    pdf_left = pd.DataFrame({"a": [4, 2, 3] * 10, "b": 1}).set_index("a")
    pdf_right = pd.DataFrame({"a": [4, 2, 3] * 10, "c": 1})

    left = dd.from_pandas(pdf_left, npartitions=5, sort=False)
    right = dd.from_pandas(pdf_right, npartitions=6)

    with dask.config.set({"dataframe.shuffle.method": "p2p"}):
        assert_eq(
            await c.compute(left.merge(right, how=how, left_index=True, right_on="a")),
            pdf_left.merge(pdf_right, how=how, left_index=True, right_on="a"),
        )

        assert_eq(
            await c.compute(right.merge(left, how=how, right_index=True, left_on="a")),
            pdf_right.merge(pdf_left, how=how, right_index=True, left_on="a"),
        )


@pytest.mark.parametrize("npartitions", [4, 5, 10, 20])
@gen_cluster(client=True)
async def test_merge_with_npartitions(c, s, a, b, npartitions):
    pdf = pd.DataFrame({"a": [1, 2, 3, 4] * 10, "b": 1})

    left = dd.from_pandas(pdf, npartitions=10)
    right = dd.from_pandas(pdf, npartitions=5)

    expected = pdf.merge(pdf)
    with dask.config.set({"dataframe.shuffle.method": "p2p"}):
        result = await c.compute(left.merge(right, npartitions=npartitions))
    assert_eq(result, expected, check_index=False)


class LimitedGetOrCreateShuffleRunManager(_ShuffleRunManager):
    seen: set[ShuffleId]
    block_get_or_create: asyncio.Event
    blocking_get_or_create: asyncio.Event

    def __init__(self, *args: Any, **kwargs: Any):
        super().__init__(*args, **kwargs)
        self.seen = set()
        self.limit = 1
        self.blocking_get_or_create = asyncio.Event()
        self.block_get_or_create = asyncio.Event()

    async def get_or_create(self, shuffle_id: ShuffleId, key: Key) -> ShuffleRun:
        if len(self.seen) >= self.limit and shuffle_id not in self.seen:
            self.blocking_get_or_create.set()
            await self.block_get_or_create.wait()
        self.seen.add(shuffle_id)
        return await super().get_or_create(shuffle_id, key)


@mock.patch(
    "distributed.shuffle._worker_plugin._ShuffleRunManager",
    LimitedGetOrCreateShuffleRunManager,
)
@gen_cluster(client=True, nthreads=[("", 1)])
async def test_merge_does_not_deadlock_if_worker_joins(c, s, a):
    """Regression test for https://github.com/dask/distributed/issues/8411"""
    pdf1 = pd.DataFrame({"a": range(100), "b": range(0, 200, 2)})
    pdf2 = pd.DataFrame({"x": range(200), "y": [1, 2, 3, 4] * 50})
    df1 = dd.from_pandas(pdf1, npartitions=10)
    df2 = dd.from_pandas(pdf2, npartitions=20)

    run_manager_A = a.plugins["shuffle"].shuffle_runs

    with dask.config.set({"dataframe.shuffle.method": "p2p"}):
        joined = dd.merge(df1, df2, left_on="a", right_on="x")
    result = c.compute(joined)

    await run_manager_A.blocking_get_or_create.wait()

    async with Worker(s.address) as b:
        run_manager_A.block_get_or_create.set()
        run_manager_B = b.plugins["shuffle"].shuffle_runs
        run_manager_B.block_get_or_create.set()
        result = await result
    expected = pd.merge(pdf1, pdf2, left_on="a", right_on="x")
    assert_eq(result, expected, check_index=False)


@gen_cluster(client=True)
async def test_merge_indicator(c, s, a, b):
    data = {
        "id": [1, 2, 3],
        "test": [4, 5, 6],
    }
    pdf = pd.DataFrame(data)
    df = dd.from_pandas(pdf, npartitions=2)
    result = df.merge(df, on="id", how="outer", indicator=True)
    x = c.compute(result)
    x = await x
    expected = pdf.merge(pdf, on="id", how="outer", indicator=True)

    pd.testing.assert_frame_equal(
        x.sort_values("id", ignore_index=True),
        expected.sort_values("id", ignore_index=True),
    )

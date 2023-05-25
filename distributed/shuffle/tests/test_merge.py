from __future__ import annotations

import pytest

from distributed.shuffle._merge import hash_join
from distributed.shuffle.tests.utils import invoke_annotation_chaos
from distributed.utils_test import gen_cluster

dd = pytest.importorskip("dask.dataframe")
import pandas as pd

from dask.dataframe._compat import PANDAS_GT_200, tm
from dask.dataframe.utils import assert_eq
from dask.utils_test import hlg_layer_topological


@pytest.fixture(params=[0, 0.3, 1], ids=["none", "some", "all"])
def lose_annotations(request):
    return request.param


def list_eq(aa, bb):
    if isinstance(aa, dd.DataFrame):
        a = aa.compute(scheduler="sync")
    else:
        a = aa
    if isinstance(bb, dd.DataFrame):
        b = bb.compute(scheduler="sync")
    else:
        b = bb
    tm.assert_index_equal(a.columns, b.columns)

    if isinstance(a, pd.DataFrame):
        av = a.sort_values(list(a.columns)).values
        bv = b.sort_values(list(b.columns)).values
    else:
        av = a.sort_values().values
        bv = b.sort_values().values

    dd._compat.assert_numpy_array_equal(av, bv)


@pytest.mark.parametrize("how", ["inner", "left", "right", "outer"])
@gen_cluster(client=True)
async def test_basic_merge(c, s, a, b, how, lose_annotations):
    await invoke_annotation_chaos(lose_annotations, c)
    A = pd.DataFrame({"x": [1, 2, 3, 4, 5, 6], "y": [1, 1, 2, 2, 3, 4]})
    a = dd.repartition(A, [0, 4, 5])

    B = pd.DataFrame({"y": [1, 3, 4, 4, 5, 6], "z": [6, 5, 4, 3, 2, 1]})
    b = dd.repartition(B, [0, 2, 5])

    joined = hash_join(a, "y", b, "y", how)

    assert not hlg_layer_topological(joined.dask, -1).is_materialized()
    result = await c.compute(joined)
    expected = pd.merge(A, B, how, "y")
    list_eq(result, expected)

    # Different columns and npartitions
    joined = hash_join(a, "x", b, "z", "outer", npartitions=3)
    assert not hlg_layer_topological(joined.dask, -1).is_materialized()
    assert joined.npartitions == 3

    result = await c.compute(joined)
    expected = pd.merge(A, B, "outer", None, "x", "z")

    list_eq(result, expected)

    assert (
        hash_join(a, "y", b, "y", "inner")._name
        == hash_join(a, "y", b, "y", "inner")._name
    )
    assert (
        hash_join(a, "y", b, "y", "inner")._name
        != hash_join(a, "y", b, "y", "outer")._name
    )


@pytest.mark.parametrize("how", ["inner", "outer", "left", "right"])
@gen_cluster(client=True)
async def test_merge(c, s, a, b, how, lose_annotations):
    await invoke_annotation_chaos(lose_annotations, c)
    A = pd.DataFrame({"x": [1, 2, 3, 4, 5, 6], "y": [1, 1, 2, 2, 3, 4]})
    a = dd.repartition(A, [0, 4, 5])

    B = pd.DataFrame({"y": [1, 3, 4, 4, 5, 6], "z": [6, 5, 4, 3, 2, 1]})
    b = dd.repartition(B, [0, 2, 5])

    joined = dd.merge(a, b, left_index=True, right_index=True, how=how, shuffle="p2p")
    res = await c.compute(joined)
    assert_eq(
        res,
        pd.merge(A, B, left_index=True, right_index=True, how=how),
    )
    joined = dd.merge(a, b, on="y", how=how)
    result = await c.compute(joined)
    list_eq(result, pd.merge(A, B, on="y", how=how))
    assert all(d is None for d in joined.divisions)

    list_eq(
        await c.compute(
            dd.merge(a, b, left_on="x", right_on="z", how=how, shuffle="p2p")
        ),
        pd.merge(A, B, left_on="x", right_on="z", how=how),
    )
    list_eq(
        await c.compute(
            dd.merge(
                a,
                b,
                left_on="x",
                right_on="z",
                how=how,
                suffixes=("1", "2"),
                shuffle="p2p",
            )
        ),
        pd.merge(A, B, left_on="x", right_on="z", how=how, suffixes=("1", "2")),
    )

    list_eq(
        await c.compute(dd.merge(a, b, how=how, shuffle="p2p")),
        pd.merge(A, B, how=how),
    )
    list_eq(
        await c.compute(dd.merge(a, B, how=how, shuffle="p2p")),
        pd.merge(A, B, how=how),
    )
    list_eq(
        await c.compute(dd.merge(A, b, how=how, shuffle="p2p")),
        pd.merge(A, B, how=how),
    )
    # Note: No await since A and B are both pandas dataframes and this doesn't
    # actually submit anything
    list_eq(
        c.compute(dd.merge(A, B, how=how, shuffle="p2p")),
        pd.merge(A, B, how=how),
    )

    list_eq(
        await c.compute(
            dd.merge(a, b, left_index=True, right_index=True, how=how, shuffle="p2p")
        ),
        pd.merge(A, B, left_index=True, right_index=True, how=how),
    )
    list_eq(
        await c.compute(
            dd.merge(
                a,
                b,
                left_index=True,
                right_index=True,
                how=how,
                suffixes=("1", "2"),
                shuffle="p2p",
            )
        ),
        pd.merge(A, B, left_index=True, right_index=True, how=how, suffixes=("1", "2")),
    )

    list_eq(
        await c.compute(
            dd.merge(a, b, left_on="x", right_index=True, how=how, shuffle="p2p")
        ),
        pd.merge(A, B, left_on="x", right_index=True, how=how),
    )
    list_eq(
        await c.compute(
            dd.merge(
                a,
                b,
                left_on="x",
                right_index=True,
                how=how,
                suffixes=("1", "2"),
                shuffle="p2p",
            )
        ),
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

            expected = pdl.join(pdr, how=how)
            assert_eq(
                await c.compute(ddl.join(ddr, how=how, shuffle="p2p")),
                expected,
                # FIXME: There's an discrepancy with an empty index for
                # pandas=2.0 (xref https://github.com/dask/dask/issues/9957).
                # Temporarily avoid index check until the discrepancy is fixed.
                check_index=not (PANDAS_GT_200 and expected.index.empty),
            )

            expected = pdr.join(pdl, how=how)
            assert_eq(
                await c.compute(ddr.join(ddl, how=how, shuffle="p2p")),
                expected,
                # FIXME: There's an discrepancy with an empty index for
                # pandas=2.0 (xref https://github.com/dask/dask/issues/9957).
                # Temporarily avoid index check until the discrepancy is fixed.
                check_index=not (PANDAS_GT_200 and expected.index.empty),
            )

            expected = pd.merge(pdl, pdr, how=how, left_index=True, right_index=True)
            assert_eq(
                await c.compute(
                    dd.merge(
                        ddl,
                        ddr,
                        how=how,
                        left_index=True,
                        right_index=True,
                        shuffle="p2p",
                    )
                ),
                expected,
                # FIXME: There's an discrepancy with an empty index for
                # pandas=2.0 (xref https://github.com/dask/dask/issues/9957).
                # Temporarily avoid index check until the discrepancy is fixed.
                check_index=not (PANDAS_GT_200 and expected.index.empty),
            )

            expected = pd.merge(pdr, pdl, how=how, left_index=True, right_index=True)
            assert_eq(
                await c.compute(
                    dd.merge(
                        ddr,
                        ddl,
                        how=how,
                        left_index=True,
                        right_index=True,
                        shuffle="p2p",
                    )
                ),
                expected,
                # FIXME: There's an discrepancy with an empty index for
                # pandas=2.0 (xref https://github.com/dask/dask/issues/9957).
                # Temporarily avoid index check until the discrepancy is fixed.
                check_index=not (PANDAS_GT_200 and expected.index.empty),
            )

            # hash join
            list_eq(
                await c.compute(
                    dd.merge(
                        ddl,
                        ddr,
                        how=how,
                        left_on="a",
                        right_on="d",
                        shuffle="p2p",
                    )
                ),
                pd.merge(pdl, pdr, how=how, left_on="a", right_on="d"),
            )
            list_eq(
                await c.compute(
                    dd.merge(
                        ddl,
                        ddr,
                        how=how,
                        left_on="b",
                        right_on="e",
                        shuffle="p2p",
                    )
                ),
                pd.merge(pdl, pdr, how=how, left_on="b", right_on="e"),
            )

            list_eq(
                await c.compute(
                    dd.merge(
                        ddr,
                        ddl,
                        how=how,
                        left_on="d",
                        right_on="a",
                        shuffle="p2p",
                    )
                ),
                pd.merge(pdr, pdl, how=how, left_on="d", right_on="a"),
            )
            list_eq(
                await c.compute(
                    dd.merge(
                        ddr,
                        ddl,
                        how=how,
                        left_on="e",
                        right_on="b",
                        shuffle="p2p",
                    )
                ),
                pd.merge(pdr, pdl, how=how, left_on="e", right_on="b"),
            )

            list_eq(
                await c.compute(
                    dd.merge(
                        ddl,
                        ddr,
                        how=how,
                        left_on=["a", "b"],
                        right_on=["d", "e"],
                        shuffle="p2p",
                    )
                ),
                pd.merge(pdl, pdr, how=how, left_on=["a", "b"], right_on=["d", "e"]),
            )

# This entire file was copied from dask/dask and has been adjusted to run on a
# distributed scheduler using the P2P backend.
# Tests that are not requiring a shuffle have been omitted.
# Omitted tests
# - test_merge_known_to_known
# - test_merge_known_to_single
# - test_merge_single_to_known
# - test_merge_column_with_nulls
# - test_merge_known_to_double_bcast_right
# - test_merge_known_to_double_bcast_left

from __future__ import annotations

import pytest

np = pytest.importorskip("numpy")
dd = pytest.importorskip("dask.dataframe")
import pandas as pd

from dask.dataframe.utils import assert_eq

from distributed.shuffle import HashJoinP2PLayer, P2PShuffleLayer
from distributed.utils_test import gen_cluster


# Fixtures
# ========
@pytest.fixture
def df_left():
    # Create frame with 10 partitions
    # Frame has 11 distinct idx values
    partition_sizes = np.array([3, 4, 2, 5, 3, 2, 5, 9, 4, 7, 4])
    idx = [i for i, s in enumerate(partition_sizes) for _ in range(s)]
    k = [i for s in partition_sizes for i in range(s)]
    vi = range(len(k))

    return pd.DataFrame(dict(idx=idx, k=k, v1=vi)).set_index(["idx"])


@pytest.fixture
def df_right():
    # Create frame with 10 partitions
    # Frame has 11 distinct idx values
    partition_sizes = np.array([4, 2, 5, 3, 2, 5, 9, 4, 7, 4, 8])
    idx = [i for i, s in enumerate(partition_sizes) for _ in range(s)]
    k = [i for s in partition_sizes for i in range(s)]
    vi = range(len(k))

    return pd.DataFrame(dict(idx=idx, k=k, v1=vi)).set_index(["idx"])


@pytest.fixture
def ddf_left(df_left):
    # Create frame with 10 partitions
    # Skip division on 2 so there is one mismatch with ddf_right
    return dd.repartition(df_left, [0, 1, 3, 4, 5, 6, 7, 8, 9, 10, 11])


@pytest.fixture
def ddf_left_unknown(ddf_left):
    return ddf_left.clear_divisions()


@pytest.fixture
def ddf_left_single(df_left):
    return dd.from_pandas(df_left, npartitions=1, sort=False)


@pytest.fixture
def ddf_right(df_right):
    # Create frame with 10 partitions
    # Skip division on 3 so there is one mismatch with ddf_left
    return dd.repartition(df_right, [0, 1, 2, 4, 5, 6, 7, 8, 9, 10, 11])


@pytest.fixture
def ddf_right_unknown(ddf_right):
    return ddf_right.clear_divisions()


@pytest.fixture
def ddf_right_single(df_right):
    return dd.from_pandas(df_right, npartitions=1, sort=False)


@pytest.fixture
def ddf_right_double(df_right):
    return dd.from_pandas(df_right, npartitions=2, sort=False)


@pytest.fixture
def ddf_left_double(df_left):
    return dd.from_pandas(df_left, npartitions=2, sort=False)


@pytest.fixture(params=["inner", "left", "right", "outer"])
def how(request):
    return request.param


@pytest.fixture(params=["idx", ["idx"], ["idx", "k"], ["k", "idx"]])
def on(request):
    return request.param


# Tests
# =====


@gen_cluster(client=True)
async def test_merge_known_to_unknown(
    c,
    s,
    a,
    b,
    df_left,
    df_right,
    ddf_left,
    ddf_right_unknown,
    on,
    how,
):
    # Compute expected
    expected = df_left.merge(df_right, on=on, how=how)

    # Perform merge
    result_graph = ddf_left.merge(ddf_right_unknown, on=on, how=how, shuffle="p2p")
    result = await c.compute(result_graph)
    # Assertions
    assert_eq(result, expected)
    assert_eq(result_graph.divisions, tuple(None for _ in range(11)))


@gen_cluster(client=True)
async def test_merge_unknown_to_known(
    c,
    s,
    a,
    b,
    df_left,
    df_right,
    ddf_left_unknown,
    ddf_right,
    on,
    how,
):
    # Compute expected
    expected = df_left.merge(df_right, on=on, how=how)

    # Perform merge
    result_graph = ddf_left_unknown.merge(ddf_right, on=on, how=how, shuffle="p2p")
    result = await c.compute(result_graph)

    # Assertions
    assert_eq(result, expected)
    assert_eq(result_graph.divisions, tuple(None for _ in range(11)))


@gen_cluster(client=True)
async def test_merge_unknown_to_unknown(
    c,
    s,
    a,
    b,
    df_left,
    df_right,
    ddf_left_unknown,
    ddf_right_unknown,
    on,
    how,
):
    # Compute expected
    expected = df_left.merge(df_right, on=on, how=how)

    # Merge unknown to unknown
    result_graph = ddf_left_unknown.merge(
        ddf_right_unknown, on=on, how=how, shuffle="p2p"
    )
    if not any(
        isinstance(layer, (HashJoinP2PLayer, P2PShuffleLayer))
        for layer in result_graph.dask.layers.values()
    ):
        pytest.skip("No HashJoin or P2P layer involved")
    result = await c.compute(result_graph)
    # Assertions
    assert_eq(result, expected)
    assert_eq(result_graph.divisions, tuple(None for _ in range(11)))

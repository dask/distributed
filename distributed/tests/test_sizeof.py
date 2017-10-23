from __future__ import print_function, division, absolute_import

import sys

import pytest

from distributed.sizeof import sizeof, getsizeof


def test_base():
    assert sizeof(1) == getsizeof(1)


def test_containers():
    assert sizeof([1, 2, [3]]) > (getsizeof(3) * 3 + getsizeof([]))


def test_numpy():
    np = pytest.importorskip('numpy')
    assert 8000 <= sizeof(np.empty(1000, dtype='f8')) <= 9000
    dt = np.dtype('f8')
    assert sizeof(dt) == sys.getsizeof(dt)


def test_pandas():
    pd = pytest.importorskip('pandas')
    df = pd.DataFrame({'x': [1, 2, 3], 'y': ['a' * 100, 'b' * 100, 'c' * 100]},
                      index=[10, 20, 30])

    assert sizeof(df) >= sizeof(df.x) + sizeof(df.y) - sizeof(df.index)
    assert sizeof(df.x) >= sizeof(df.index)
    if pd.__version__ >= '0.17.1':
        assert sizeof(df.y) >= 100 * 3
    assert sizeof(df.index) >= 20

    assert isinstance(sizeof(df), int)
    assert isinstance(sizeof(df.x), int)
    assert isinstance(sizeof(df.index), int)


def test_pandas_repeated_column():
    pd = pytest.importorskip('pandas')
    df = pd.DataFrame({'x': [1, 2, 3]})

    assert sizeof(df[['x', 'x', 'x']]) > sizeof(df)


def test_sparse_matrix():
    sparse = pytest.importorskip('scipy.sparse')
    sp = sparse.eye(10)
    # These are the 32-bit Python 2.7 values.
    assert sizeof(sp.todia()) >= 152
    assert sizeof(sp.tobsr()) >= 232
    assert sizeof(sp.tocoo()) >= 240
    assert sizeof(sp.tocsc()) >= 232
    assert sizeof(sp.tocsr()) >= 232
    assert sizeof(sp.todok()) >= 192
    assert sizeof(sp.tolil()) >= 204


def test_serires_object_dtype():
    pd = pytest.importorskip('pandas')
    s = pd.Series(['a'] * 1000)
    assert sizeof('a') * 1000 < sizeof(s) < 2 * sizeof('a') * 1000

    s = pd.Series(['a' * 1000] * 1000)
    assert sizeof(s) > 1000000


def test_dataframe_object_dtype():
    pd = pytest.importorskip('pandas')
    df = pd.DataFrame({'x': ['a'] * 1000})
    assert sizeof('a') * 1000 < sizeof(df) < 2 * sizeof('a') * 1000

    s = pd.Series(['a' * 1000] * 1000)
    assert sizeof(s) > 1000000


def test_empty():
    pd = pytest.importorskip('pandas')
    df = pd.DataFrame({'x': [1, 2, 3], 'y': ['a' * 100, 'b' * 100, 'c' * 100]},
                      index=[10, 20, 30])
    empty = df.head(0)

    assert sizeof(empty) > 0
    assert sizeof(empty.x) > 0
    assert sizeof(empty.y) > 0
    assert sizeof(empty.index) > 0

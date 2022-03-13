import os
import random

import numpy as np
import pandas as pd
import pytest

from distributed.multi_file import MultiFile


def test_basic(tmp_path):
    with MultiFile(
        directory=tmp_path, n_files=4, memory_limit="16 MiB", join=pd.concat
    ) as mf:
        df = pd.DataFrame({"x": np.arange(1000), "y": np.arange(1000) * 2})
        mf.write(df, "a")
        mf.write(df, "b")
        mf.write(df * 2, "a")

        a = mf.read("a")
        b = mf.read("b")

        assert (df == b).all().all()
        assert (pd.concat([df, df * 2]) == a).all().all()

    assert not os.path.exists(tmp_path)


@pytest.mark.parametrize("count", [2, 100, 1000])
def test_many(tmp_path, count):
    with MultiFile(
        directory=tmp_path, n_files=4, memory_limit="16 MiB", join=pd.concat
    ) as mf:
        df = pd.DataFrame({"x": np.arange(10), "y": np.arange(10) * 2})

        L = list(range(count))

        random.shuffle(L)
        for i in L:
            mf.write(df + i, str(i))

        random.shuffle(L)
        for i in L:
            mf.write(df * i, str(i))

        random.shuffle(L)
        for i in L:
            out = mf.read(str(i))

            assert (pd.concat([df + i, df * i]) == out).all().all()

    assert not os.path.exists(tmp_path)

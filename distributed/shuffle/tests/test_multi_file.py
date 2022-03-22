import os
import random

import numpy as np
import pandas as pd
import pytest

from distributed.shuffle.multi_file import MultiFile


@pytest.mark.asyncio
async def test_basic(tmp_path):
    with MultiFile(directory=tmp_path, memory_limit="16 MiB", join=pd.concat) as mf:
        df = pd.DataFrame({"x": np.arange(1000), "y": np.arange(1000) * 2})
        await mf.write(df, "a")
        await mf.write(df, "b")
        await mf.write(df * 2, "a")

        a = mf.read("a")
        b = mf.read("b")

        assert (df == b).all().all()
        assert (pd.concat([df, df * 2]) == a).all().all()

    assert not os.path.exists(tmp_path)


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [2, 100, 1000])
async def test_many(tmp_path, count):
    with MultiFile(directory=tmp_path, memory_limit="16 MiB", join=pd.concat) as mf:
        df = pd.DataFrame({"x": np.arange(10), "y": np.arange(10) * 2})

        L = list(range(count))

        random.shuffle(L)
        for i in L:
            await mf.write(df + i, i)

        random.shuffle(L)
        for i in L:
            await mf.write(df * i, i)

        random.shuffle(L)
        for i in L:
            out = mf.read(i)

            assert (pd.concat([df + i, df * i]) == out).all().all()

    assert not os.path.exists(tmp_path)

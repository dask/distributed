import asyncio
import time

import pytest

from distributed.diagnostics import MemorySampler
from distributed.utils_test import gen_cluster


@gen_cluster(client=True)
async def test_async(c, s, a, b):
    ms = MemorySampler()
    async with ms.sample("foo", measure="managed", interval=0.4):
        await asyncio.sleep(0.2)
        f = c.submit(lambda: 1)
        await f
        await asyncio.sleep(0.4)

    assert len(ms.samples["foo"]) == 2
    assert ms.samples["foo"][0][1] == 0
    assert ms.samples["foo"][1][1] > 0

    # Test that there is no server-side memory leak
    assert not s.extensions["memory_sampler"].samples


def test_sync(client):
    ms = MemorySampler()
    with ms.sample("foo", measure="managed", interval=0.4):
        time.sleep(0.2)
        f = client.submit(lambda: 1)
        f.result()
        time.sleep(0.4)

    assert len(ms.samples["foo"]) == 2
    assert ms.samples["foo"][0][1] == 0
    assert ms.samples["foo"][1][1] > 0


@gen_cluster()
async def test_at_least_one_sample(s, a, b):
    """The first sample is taken immediately"""
    ms = MemorySampler()
    async with ms.sample("foo"):
        pass
    assert len(ms.samples["foo"]) == 1


@gen_cluster(client=True)
async def test_pandas(c, s, a, b):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("matplotlib")

    ms = MemorySampler()
    async with ms.sample("foo", measure="managed", interval=0.4):
        await asyncio.sleep(0.2)
        f = c.submit(lambda: 1)
        await f
        await asyncio.sleep(0.4)

    assert len(ms.samples["foo"]) == 2
    assert ms.samples["foo"][0][1] == 0
    assert ms.samples["foo"][1][1] > 0

    df = ms.to_pandas()
    assert isinstance(df, pd.DataFrame)
    assert df.shape == (2, 1)
    assert df["foo"].iloc[0] == 0
    assert df["foo"].iloc[1] > 0
    assert df.index[0] == pd.Timedelta(0, unit="s")
    assert df.index[1] > pd.Timedelta(0, unit="s")
    assert df.index[1] < pd.Timedelta(0.8, unit="s")

    plt = ms.plot(grid=True)
    assert plt


@pytest.mark.slow
@gen_cluster(client=True)
async def test_pandas_multiseries(c, s, a, b):
    """Test that multiple series are upscaled and aligned to each other"""
    pd = pytest.importorskip("pandas")

    ms = MemorySampler()
    for label in ("foo", "bar"):
        async with ms.sample(label, measure="managed", interval=0.4):
            await asyncio.sleep(0.2)
            f = c.submit(lambda: 1)
            await f
            await asyncio.sleep(0.8)
        del f
        await asyncio.sleep(0.2)

    for label in ("foo", "bar"):
        assert len(ms.samples[label]) == 3
        assert ms.samples[label][0][1] == 0
        assert ms.samples[label][-1][1] > 0

    df = ms.to_pandas()
    # Rescaled to 0.1S
    assert df.shape == (9, 2)
    assert df["foo"].iloc[0] == 0
    assert df["foo"].iloc[-1] > 0
    assert df["bar"].iloc[0] == 0
    assert df["bar"].iloc[-1] > 0
    assert df.index[0] == pd.Timedelta(0, unit="s")
    assert df.index[1] == pd.Timedelta(0.1, unit="s")
    assert df.index[-1] == pd.Timedelta(0.8, unit="s")

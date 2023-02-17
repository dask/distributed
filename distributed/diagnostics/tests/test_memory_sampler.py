from __future__ import annotations

import asyncio
import time

import pytest

from distributed.diagnostics import MemorySampler
from distributed.utils_test import gen_cluster


@gen_cluster(client=True)
async def test_async(c, s, a, b):
    ms = MemorySampler()
    async with ms.sample("foo", measure="managed", interval=0.1):
        f = c.submit(lambda: 1)
        await f
        await asyncio.sleep(0.5)

    assert ms.samples["foo"][0][1] == 0
    assert ms.samples["foo"][-1][1] > 0

    # Test that there is no server-side memory leak
    assert not s.extensions["memory_sampler"].samples


def test_sync(client):
    ms = MemorySampler()
    with ms.sample("foo", measure="managed", interval=0.1):
        f = client.submit(lambda: 1)
        f.result()
        time.sleep(0.5)

    assert ms.samples["foo"][0][1] == 0
    assert ms.samples["foo"][-1][1] > 0


@gen_cluster(client=True)  # MemorySampler internally fetches the client
async def test_at_least_one_sample(c, s, a, b):
    """The first sample is taken immediately
    Also test omitting the label
    """
    ms = MemorySampler()
    async with ms.sample():
        pass
    assert len(next(iter(ms.samples.values()))) == 1


@pytest.mark.slow
@gen_cluster(client=True)
async def test_multi_sample(c, s, a, b):
    ms = MemorySampler()
    s1 = ms.sample("managed", measure="managed", interval=0.15)
    s2 = ms.sample("process", interval=0.2)
    async with s1, s2:
        idle_mem = s.memory.process
        f = c.submit(lambda: "x" * 100 * 2**20)  # 100 MiB
        await f
        while s.memory.process < idle_mem + 80 * 2**20:
            # Wait for heartbeat
            await asyncio.sleep(0.01)
        await asyncio.sleep(0.6)

    m = ms.samples["managed"]
    p = ms.samples["process"]
    assert len(m) >= 2
    assert m[0][1] == 0
    assert m[-1][1] >= 100 * 2**20
    assert len(p) >= 2
    assert p[0][1] > 2**20  # Assume > 1 MiB for idle process
    assert p[-1][1] > p[0][1] + 80 * 2**20
    assert m[-1][1] < p[-1][1]


@gen_cluster(client=True)
@pytest.mark.parametrize("align", [False, True])
async def test_pandas(c, s, a, b, align):
    pd = pytest.importorskip("pandas")
    pytest.importorskip("matplotlib")

    ms = MemorySampler()
    async with ms.sample("foo", measure="managed", interval=0.15):
        f = c.submit(lambda: 1)
        await f
        await asyncio.sleep(0.7)

    assert ms.samples["foo"][0][1] == 0
    assert ms.samples["foo"][-1][1] > 0

    df = ms.to_pandas(align=align)
    assert isinstance(df, pd.DataFrame)
    if align:
        assert isinstance(df.index, pd.TimedeltaIndex)
        assert df["foo"].iloc[0] == 0
        assert df["foo"].iloc[-1] > 0
        assert df.index[0] == pd.Timedelta(0, unit="s")
        assert pd.Timedelta(0, unit="s") < df.index[1]
        assert df.index[1] < pd.Timedelta(1.5, unit="s")
    else:
        assert isinstance(df.index, pd.DatetimeIndex)
        assert pd.Timedelta(0, unit="s") < df.index[1] - df.index[0]
        assert df.index[1] - df.index[0] < pd.Timedelta(1.5, unit="s")

    plt = ms.plot(align=align, grid=True)
    assert plt


@pytest.mark.slow
@gen_cluster(client=True)
@pytest.mark.parametrize("align", [False, True])
async def test_pandas_multiseries(c, s, a, b, align):
    """Test that multiple series are upsampled and aligned to each other"""
    pd = pytest.importorskip("pandas")

    ms = MemorySampler()
    for label, interval, final_sleep in (("foo", 0.15, 1.0), ("bar", 0.2, 0.6)):
        async with ms.sample(label, measure="managed", interval=interval):
            x = c.submit(lambda: 1, key="x")
            await x
            await asyncio.sleep(final_sleep)
        del x
        while "x" in s.tasks:
            await asyncio.sleep(0.01)

    for label in ("foo", "bar"):
        assert ms.samples[label][0][1] == 0
        assert ms.samples[label][-1][1] > 0

    df = ms.to_pandas(align=align)
    if align:
        assert df.index[0] == pd.Timedelta(0, unit="s")

        # ffill does not go beyond the end of the series
        assert df["foo"].iloc[0] == 0
        assert df["foo"].iloc[-1] > 0

        assert df["bar"].iloc[0] == 0
        assert pd.isna(df["bar"].iloc[-1])
        assert df["bar"].ffill().iloc[-1] > 0

    else:
        assert df["foo"].iloc[0] == 0
        assert pd.isna(df["foo"].iloc[-1])
        assert pd.isna(df["bar"].iloc[0])
        assert df["bar"].iloc[-1] > 0

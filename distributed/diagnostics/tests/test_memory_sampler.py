from __future__ import annotations

import asyncio
import time

import pytest

import dask

from distributed import Client
from distributed.diagnostics import MemorySampler
from distributed.utils_test import SizeOf, cluster, gen_cluster, gen_test


@pytest.fixture(scope="module")
@gen_cluster(client=True, config={"distributed.admin.system-monitor.interval": "1ms"})
async def some_sample(c, s, *workers):
    ms = MemorySampler()
    name = "foo"
    async with ms.sample(name, measure="managed", interval=0.001):
        f = c.submit(lambda: 1)
        await f
        await asyncio.sleep(0.1)
        f.release()
        await asyncio.sleep(0.1)

    assert ms.samples[name][0][1] == 0
    assert sum([s[1] for s in ms.samples[name]]) > 0

    # Test that there is no server-side memory leak
    assert not s.extensions["memory_sampler"].samples
    return name, ms


def test_sync(loop):
    with (
        dask.config.set({"distributed.admin.system-monitor.interval": "1ms"}),
        cluster() as (scheduler, _),
        Client(scheduler["address"], loop=loop) as client,
    ):
        ms = MemorySampler()
        with ms.sample("foo", measure="managed", interval=0.001):
            f = client.submit(lambda: 1)
            f.result()
            time.sleep(0.01)

        assert ms.samples["foo"][0][1] == 0
        assert sum([s[1] for s in ms.samples["foo"]]) > 0


@gen_cluster(client=True)  # MemorySampler internally fetches the client
async def test_at_least_one_sample(c, s, a, b):
    """The first sample is taken immediately
    Also test omitting the label
    """
    ms = MemorySampler()
    async with ms.sample():
        pass
    assert len(next(iter(ms.samples.values()))) == 1


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_multi_sample(c, s, a):
    expected_process_memory = 20 * 1024**2

    def mock_process_memory():
        return expected_process_memory if a.data.fast else 0

    a.monitor.get_process_memory = mock_process_memory
    a.monitor.update()
    await a.heartbeat()

    ms = MemorySampler()
    s1 = ms.sample("managed", measure="managed", interval=0.001)
    s2 = ms.sample("process", interval=0.001)

    expected_managed_memory = 100 * 1024
    payload = SizeOf(expected_managed_memory)

    async with s1, s2:
        f = c.submit(lambda: payload)
        await f
        a.monitor.update()
        await a.heartbeat()
        await asyncio.sleep(0.01)

    m = ms.samples["managed"]
    p = ms.samples["process"]

    assert len(m) >= 2
    assert m[0][1] == 0
    assert m[-1][1] == expected_managed_memory
    assert len(p) >= 2
    assert p[0][1] == 0
    assert p[-1][1] == expected_process_memory


@gen_test()
@pytest.mark.parametrize("align", [False, True])
async def test_pandas(some_sample, align):
    name, ms = some_sample
    pd = pytest.importorskip("pandas")
    pytest.importorskip("matplotlib")

    df = ms.to_pandas(align=align)
    assert isinstance(df, pd.DataFrame)
    if align:
        assert isinstance(df.index, pd.TimedeltaIndex)
        assert df[name].iloc[0] == 0
        assert df[name].sum() > 1
        assert df.index[0] == pd.Timedelta(0, unit="s")
        assert pd.Timedelta(0, unit="s") < df.index[1]
        assert df.index[1] < pd.Timedelta(1.5, unit="s")
    else:
        assert isinstance(df.index, pd.DatetimeIndex)
        assert pd.Timedelta(0, unit="s") < df.index[1] - df.index[0]
        assert df.index[1] - df.index[0] < pd.Timedelta(1.5, unit="s")

    plt = ms.plot(align=align, grid=True)
    assert plt


@gen_cluster(client=True)
@pytest.mark.parametrize("align", [False, True])
async def test_pandas_multiseries(c, s, a, b, align):
    """Test that multiple series are upsampled and aligned to each other"""
    pd = pytest.importorskip("pandas")

    ms = MemorySampler()
    for label, interval, final_sleep in (
        ("foo", 0.001, 0.2),
        ("bar", 0.002, 0.01),
    ):
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

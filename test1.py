import pytest


def test_ipywidgets():
    import ipywidgets


@pytest.mark.asyncio
async def test_profile_nested_sizeof():
    from distributed.comm.utils import to_frames

    # https://github.com/dask/distributed/issues/1674
    n = 500
    original = outer = {}
    inner = {}

    for i in range(n):
        outer["children"] = inner
        outer, inner = inner, {}

    msg = {"data": original}
    frames = await to_frames(msg)


@pytest.mark.asyncio
async def test_offload():
    from distributed.utils import offload

    def f():
        pass

    await offload(f)


@pytest.mark.asyncio
async def test_run1():
    import asyncio
    from concurrent.futures import ThreadPoolExecutor

    ex = ThreadPoolExecutor(max_workers=1, thread_name_prefix="Dask-Offload")

    def f():
        print("Hello world")

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(ex, f)


@pytest.mark.asyncio
async def test_run2():
    import asyncio

    def f():
        print("Hello world")

    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, f)

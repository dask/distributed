import asyncio
import pytest


def gen_msg():
    n = 500
    original = outer = {}
    inner = {}

    for i in range(n):
        outer["children"] = inner
        outer, inner = inner, {}

    return {"data": original}


def test_ipywidgets():
    import ipywidgets


@pytest.mark.asyncio
async def test_profile_nested_sizeof():
    from distributed.comm.utils import to_frames
    msg = gen_msg()
    await to_frames(msg)


@pytest.mark.asyncio
async def test1():
    from distributed import protocol
    msg = gen_msg()
    protocol.dumps(msg)


def test2():
    from distributed import protocol
    msg = gen_msg()
    protocol.dumps(msg)


def test3():
    import msgpack
    msg = gen_msg()
    msgpack.dumps(msg, use_bin_type=True)


@pytest.mark.asyncio
async def test4():
    from distributed import protocol
    msg = gen_msg()
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, protocol.dumps, msg)


@pytest.mark.asyncio
async def test5():
    import msgpack
    msg = gen_msg()
    loop = asyncio.get_event_loop()
    await loop.run_in_executor(None, lambda x: msgpack.dumps(x, use_bin_type=True), msg)


@pytest.mark.asyncio
async def test6():
    from distributed import protocol
    from distributed.utils import offload

    msg = gen_msg()

    def _to_frames():
        return list(protocol.dumps(msg))

    return await offload(_to_frames)


def test7():
    from dask.sizeof import sizeof

    msg = gen_msg()
    with pytest.raises(RecursionError):
        sizeof(msg)

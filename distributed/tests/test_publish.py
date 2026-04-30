from __future__ import annotations

import asyncio

import pytest

from dask import delayed

from distributed import Client
from distributed.client import futures_of
from distributed.metrics import time
from distributed.protocol import Serialized
from distributed.utils_test import gen_cluster, inc


@gen_cluster(client=True)
async def test_publish_simple(c, s, a, b):
    async with Client(s.address, asynchronous=True) as c2:
        data = await c.scatter(range(3))
        await c.publish_dataset(data=data)
        assert "data" in s.extensions["publish"].datasets
        assert isinstance(s.extensions["publish"].datasets["data"]["data"], Serialized)

        with pytest.raises(KeyError) as exc_info:
            await c.publish_dataset(data=data)

        assert "exists" in str(exc_info.value)
        assert "data" in str(exc_info.value)

        result = await c.scheduler.publish_list()
        assert result == ("data",)

        result = await c2.scheduler.publish_list()
        assert result == ("data",)


@gen_cluster(client=True)
@pytest.mark.parametrize("name", [("a", "b"), 9.0, 8])
async def test_publish_non_string_names(c, s, a, b, name):
    future = c.submit(lambda: 123, key="x")
    await c.publish_dataset(future, name=name)
    datasets = await c.scheduler.publish_list()
    assert datasets == (name,)
    # Note: name=("a", "b") tests that get_dataset and unpublish_dataset don't
    # treat a single tuple name as a sequence of names.
    future = await c.get_dataset(name)
    await c.unpublish_dataset(name)
    assert await c.scheduler.publish_list() == ()
    assert await future == 123


@gen_cluster(client=True)
async def test_publish_multiple_names(c, s, a, b):
    f1 = c.submit(lambda: 1, key="x")
    f2 = c.submit(lambda: 2, key="y")
    f3 = c.submit(lambda: 3, key="z")
    await c.publish_dataset({("a", "b"): f1, 8: f2}, n3=f3)
    # publish_list sorts output; test that it copes with sorting non-sortable names.
    datasets = await c.scheduler.publish_list()
    assert datasets == (("a", "b"), 8, "n3")
    f1, f2, f3 = await c.get_dataset([("a", "b"), 8, "n3"])
    await c.unpublish_dataset([("a", "b"), 8, "n3"])
    assert await c.scheduler.publish_list() == ()
    assert await f1 == 1
    assert await f2 == 2
    assert await f3 == 3


@gen_cluster(client=True)
async def test_publish_roundtrip(c, s, a, b):
    async with Client(s.address, asynchronous=True) as c2:
        data = await c.scatter([0, 1, 2])
        await c.publish_dataset(data=data)

        assert any(
            cs.client_key == "published-data" for cs in s.tasks[data[0].key].who_wants
        )
        result = await c2.get_dataset(name="data")

        assert len(result) == len(data)
        out = await c2.gather(result)
        assert out == [0, 1, 2]

        with pytest.raises(KeyError) as exc_info:
            await c2.get_dataset(name="nonexistent")

        assert "not found" in str(exc_info.value)
        assert "nonexistent" in str(exc_info.value)


@gen_cluster(client=True)
async def test_unpublish(c, s, a, b):
    data = await c.scatter([0, 1, 2])
    await c.publish_dataset(data=data)

    key = data[0].key
    del data

    await c.unpublish_dataset(name="data")

    assert "data" not in s.extensions["publish"].datasets

    start = time()
    while key in s.tasks:
        await asyncio.sleep(0.01)
        assert time() < start + 5

    with pytest.raises(KeyError) as exc_info:
        await c.get_dataset(name="data")

    assert "not found" in str(exc_info.value)
    assert "data" in str(exc_info.value)


def test_unpublish_sync(client):
    data = client.scatter([0, 1, 2])
    client.publish_dataset(data=data)
    client.unpublish_dataset(name="data")

    with pytest.raises(KeyError) as exc_info:
        client.get_dataset(name="data")

    assert "not found" in str(exc_info.value)
    assert "data" in str(exc_info.value)


@gen_cluster(client=True)
async def test_unpublish_nonexistent(c, s, a, b):
    """Unpublishing a dataset that does not exist quietly succeeds.
    This is important when unpublish_dataset is executed by a task that may be run twice.
    """
    await c.unpublish_dataset(name="nonexistent")


@gen_cluster(client=True)
async def test_publish_multiple_datasets(c, s, a, b):
    await c.publish_dataset(x=1, y=2)
    datasets = await c.scheduler.publish_list()
    assert datasets == ("x", "y")


@gen_cluster(client=True)
async def test_publish_multiple_collections_one_name(c, s, a, b):
    x = c.submit(lambda: 1, key="x")
    y = c.submit(lambda: 2, key="y")
    await c.publish_dataset(x, y, name="n")
    datasets = await c.scheduler.publish_list()
    assert datasets == ("n",)
    x, y = await c.get_dataset("n")
    assert await x == 1
    assert await y == 2


def test_unpublish_multiple_datasets_sync(client):
    client.publish_dataset(x=1, y=2)
    client.unpublish_dataset(name="x")

    with pytest.raises(KeyError) as exc_info:
        client.get_dataset(name="x")

    datasets = client.list_datasets()
    assert set(datasets) == {"y"}

    assert "not found" in str(exc_info.value)
    assert "x" in str(exc_info.value)

    client.unpublish_dataset(name="y")

    with pytest.raises(KeyError) as exc_info:
        client.get_dataset(name="y")

    assert "not found" in str(exc_info.value)
    assert "y" in str(exc_info.value)


@gen_cluster(client=True)
async def test_publish_bag(c, s, a, b):
    db = pytest.importorskip("dask.bag")
    async with Client(s.address, asynchronous=True) as c2:
        bag = db.from_sequence([0, 1, 2])
        bagp = c.persist(bag)

        assert len(futures_of(bagp)) == 3
        keys = {f.key for f in futures_of(bagp)}
        assert keys == set(bag.dask)

        await c.publish_dataset(data=bagp)

        # check that serialization didn't affect original bag's dask
        assert len(futures_of(bagp)) == 3

        result = await c2.get_dataset("data")
        assert set(result.dask.keys()) == set(bagp.dask.keys())
        assert {f.key for f in result.dask.values()} == {
            f.key for f in bagp.dask.values()
        }

        out = await c2.compute(result)
        assert out == [0, 1, 2]


def test_datasets_setitem(client):
    for key in ["key", ("key", "key"), 1]:
        value = "value"
        client.datasets[key] = value
        assert client.get_dataset(key) == value
        assert client.get_dataset(key, default="something else") == value


def test_datasets_getitem(client):
    for key in ["key", ("key", "key"), 1]:
        value = "value"
        client.publish_dataset(value, name=key)
        assert client.datasets[key] == value
        assert client.datasets.get(key) == value
        assert client.datasets.get(key, default="something else") == value


def test_datasets_getitem_default(client):
    with pytest.raises(KeyError):
        client.get_dataset("key")

    assert client.datasets.get("key", default="value") == "value"
    assert client.datasets.get("key", default=None) is None
    assert client.get_dataset("key", default="value") == "value"


def test_datasets_delitem(client):
    for key in ["key", ("key", "key"), 1]:
        value = "value"
        client.publish_dataset(value, name=key)
        del client.datasets[key]
        assert key not in client.list_datasets()


def test_datasets_keys(client):
    client.publish_dataset(**{str(n): n for n in range(10)})
    keys = list(client.datasets.keys())
    assert keys == [str(n) for n in range(10)]


def test_datasets_contains(client):
    key, value = "key", "value"
    client.publish_dataset(key=value)
    assert key in client.datasets


def test_publish_dataset_override(client):
    key, value, value2 = "key", "value", "value2"
    client.publish_dataset(key=value)
    assert client.get_dataset(key) == value

    with pytest.raises(KeyError) as exc_info:
        client.publish_dataset(key=value)

    client.publish_dataset(key=value2, override=True)
    assert client.get_dataset(key) == value2


@gen_cluster(client=True)
async def test_publish_dataset_override_releases_old_keys(c, s, a, b):
    """Test that publish_dataset(..., override=True) releases any keys in
    the old dataset that are not also in the new dataset
    """
    key = "key"
    d1 = delayed(inc)(0)
    d2 = delayed(inc)(1)
    d3 = delayed(inc)(2)
    d1, d2, d3 = c.persist([d1, d2, d3])

    await c.publish_dataset(key=[d1, d2])
    assert (await c.gather(c.compute(await c.get_dataset(key)))) == [1, 2]
    # New keys only partially overlap with old keys
    await c.publish_dataset(key=[d2, d3], override=True)
    assert (await c.gather(c.compute(await c.get_dataset(key)))) == [2, 3]

    futures = futures_of([d1, d2, d3])
    for future in futures:
        future.release()
    # d1 has been unpublished so is dereferenced on the cluster;
    # d2 and d3 are still published so they remain live.
    while d1.key in s.tasks:
        await asyncio.sleep(0.01)
    assert d2.key in s.tasks
    assert d3.key in s.tasks
    assert set(s.tasks) == {d2.key, d3.key}


@gen_cluster(client=True)
async def test_publish_dataset_override_partial(c, s, a, b):
    """Test override=True but no all datasets already exist"""
    d1 = delayed(inc)(0)
    d2 = delayed(inc)(1)
    d3 = delayed(inc)(2)
    d4 = delayed(inc)(3)
    d5 = delayed(inc)(4)
    d1, d2, d3, d4, d5 = c.persist([d1, d2, d3, d4, d5])

    await c.publish_dataset(a=d1, b=d2, c=d3)
    # Do not override a
    # Override b with same futures
    # Override c with new futures
    # d is new
    with pytest.raises(KeyError):
        await c.publish_dataset(b=d2, c=d4, d=d5)
    await c.publish_dataset(b=d2, c=d4, d=d5, override=True)

    values = await c.gather(c.compute(await c.get_dataset(["a", "b", "c", "d"])))
    assert values == [1, 2, 4, 5]

    futures_of(d3)[0].release()
    while d3.key in s.tasks:
        await asyncio.sleep(0.01)
    assert set(s.tasks) == {d1.key, d2.key, d4.key, d5.key}


def test_datasets_iter(client):
    keys = [n for n in range(10)]
    client.publish_dataset(**{str(key): key for key in keys})
    for n, key in enumerate(client.datasets):
        assert key == str(n)
    with pytest.raises(TypeError):
        client.datasets.__aiter__()


@gen_cluster(client=True)
async def test_datasets_async(c, s, a, b):
    await c.publish_dataset(foo=1, bar=2)
    assert await c.datasets["foo"] == 1
    assert {k async for k in c.datasets} == {"foo", "bar"}
    with pytest.raises(TypeError):
        c.datasets["baz"] = 3
    with pytest.raises(TypeError):
        del c.datasets["foo"]
    with pytest.raises(TypeError):
        next(iter(c.datasets))
    with pytest.raises(TypeError):
        len(c.datasets)


@gen_cluster(client=True)
async def test_pickle_safe(c, s, a, b):
    async with Client(s.address, asynchronous=True, serializers=["msgpack"]) as c2:
        await c2.publish_dataset(x=[1, 2, 3])
        result = await c2.get_dataset("x")
        assert result == [1, 2, 3]

        with pytest.raises(TypeError):
            await c2.publish_dataset(y=lambda x: x)

        await c.publish_dataset(z=lambda x: x)  # this can use pickle

        with pytest.raises(TypeError):
            await c2.get_dataset("z")


@gen_cluster(client=True)
async def test_deserialize_client(c, s, a, b):
    """Test that the client attached to Futures returned by Client.get_dataset is always
    the instance of the client that invoked the method.
    Specifically:

    - when the client is defined by hostname, test that it is not accidentally
      reinitialised by IP;
    - when multiple clients are connected to the same scheduler, test that they don't
      interfere with each other.

    See: test_client.test_serialize_future
    See: https://github.com/dask/distributed/issues/3227
    """
    future = await c.scatter("123")
    await c.publish_dataset(foo=future)
    future = await c.get_dataset("foo")
    assert future.client is c

    for addr in (s.address, "localhost:" + s.address.split(":")[-1]):
        async with Client(addr, asynchronous=True) as c2:
            future = await c.get_dataset("foo")
            assert future.client is c
            future = await c2.get_dataset("foo")
            assert future.client is c2

    # Ensure cleanup
    from distributed.client import _current_client

    assert _current_client.get() is c


@gen_cluster(client=True)
async def test_publish_unpublish_wait_for_batched_comms(c, s, a, b):
    """Test two race conditions in publish_dataset and unpublish_dataset respectively:

    1. submit/persist sends new future(s) through batched comms
    2. publish_dataset sends an asynchronous RPC call to the scheduler, which may land
       before the batched comms from step 1
    3. As soon as publish_dataset returns, the user immediately releases the future(s).
       This is a typical use case when publish_dataset is called by a task which then
       returns None.
    4. The publish dataset now holds a reference to forgotten future(s).

    1. get_dataset internally calls future.bind_client() to notify the scheduler that
       the client now holds a reference to the future(s). This uses batched comms.
    2. The user calls unpublish_dataset immediately after get_dataset returns. This uses
       an asynchronous RPC call, which may land on the scheduler before the batched
       comms from step 1.
    3. The client now holds a reference to a forgotten future.
    """
    x = c.submit(lambda: 123, key="x")
    await c.publish_dataset(x=x)
    # First race condition: unless publish_dataset waited for submit to flush through
    # the batched comms, both submit and release may hit the scheduler before
    # publish_dataset.
    x.release()

    while s.clients[c.id] in s.tasks["x"].who_wants:
        await asyncio.sleep(0.01)

    x = await c.get_dataset("x")
    # Second race condition: unless unpublish_dataset waits for the batched comms to
    # flush future.bind_client (called by get_dataset), unpublish_dataset may hit the
    # scheduler before bind_client and the client will end up with a reference to a
    # forgotten future.
    await c.unpublish_dataset("x")

    assert s.tasks["x"].who_wants == {s.clients[c.id]}
    assert await x == 123

    x.release()
    while "x" in s.tasks:
        await asyncio.sleep(0.01)

    # Test that the synchronization events weren't leaked
    assert not s.extensions["publish"]._flush_received


@gen_cluster(client=True)
async def test_publish_bad_syntax(c, s, a, b):
    with pytest.raises(
        ValueError, match="If name is provided, expecting call signature"
    ):
        await c.publish_dataset(name="x")

    with pytest.raises(
        ValueError, match="If name is omitted, positional argument must be"
    ):
        await c.publish_dataset(1)

    with pytest.raises(
        ValueError, match="If name is omitted, positional argument must be"
    ):
        await c.publish_dataset(1, "x")

    with pytest.raises(
        ValueError, match="If name is omitted, positional argument must be"
    ):
        await c.publish_dataset({"x": 1}, {"y": 2})


@gen_cluster(client=True)
async def test_publish_unpersisted(c, s, a, b):
    """Test that unpersisted keys are returned as-is by get_dataset and not
    published by the scheduler.
    """
    x = delayed(inc)(1)
    await c.publish_dataset(x=x)
    assert not s.tasks

    x = await c.get_dataset("x")
    assert not futures_of(x)
    assert not s.tasks
    assert x.compute(scheduler="sync") == 2

    await c.unpublish_dataset("x")  # Don't try releasing unpublished keys

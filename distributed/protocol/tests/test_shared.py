from __future__ import annotations

import shlex
import subprocess
import sys
import time

import pytest

import dask.distributed

np = pytest.importorskip("numpy")

from distributed.utils_test import gen_cluster, inc, wait_for

path = "/tmp/plasma"


@pytest.fixture(scope="module")
def plasma_process():
    pytest.importorskip("pyarrow.plasma")
    cmd = shlex.split(f"plasma_store -m 10000000 -s {path}")  # 10MB
    proc = subprocess.Popen(cmd)
    yield
    proc.terminate()
    proc.wait()


@pytest.fixture()
def plasma_session(plasma_process):
    from pyarrow import plasma

    timeout = 10
    while True:
        try:
            client = plasma.connect(path)
            client.list()
            break
        except (ValueError, TypeError, RuntimeError):
            timeout -= 0.1
            if timeout < 0:
                raise RuntimeError
            time.sleep(0.1)
    client.evict(1000000)
    yield client
    client.evict(1000000)


@gen_cluster(client=True, client_kwargs={"serializers": ["plasma", "error"]})
async def test_plasma_client_worker(c, s, a, b, plasma_session):
    await b.close()  # ensure we reuse the worker data
    l0 = len(plasma_session.list())
    # uses temporary and unlikely hard-coded buffer min size of 1 byte
    x = np.arange(100)  # 800 bytes
    f = await c.scatter(x)  # produces one shared buffer
    assert len(plasma_session.list()) - l0 == 1
    f2 = c.submit(inc, f)  # does not yet make new shared buffer
    assert len(plasma_session.list()) - l0 == 1
    f3 = c.submit(inc, f)  # does not make another buffer, key exists
    assert len(plasma_session.list()) - l0 == 1
    out = await f2  # getting result creates buffer for y + 1
    assert len(plasma_session.list()) - l0 == 2
    _ = await c.gather([f2, f3])  # does not make another buffer, ser already exists
    assert (x + 1 == out).all()
    assert all([_["data_size"] == x.nbytes for _ in plasma_session.list().values()])
    assert len(plasma_session.list()) - l0 == 2


def test_plasma_worker_worker(plasma_session):
    x = np.arange(100)  # 800 bytes
    client = dask.distributed.Client(
        n_workers=2,
        threads_per_worker=1,
        serializers=["plasma", "error"],
        worker_passthrough=dict(serializers=["plasma", "error"]),
    )

    # ensure our config got through
    sers = client.run(lambda: dask.distributed.get_worker().rpc.serializers)
    assert list(sers.values()) == [["plasma", "error"], ["plasma", "error"]]

    f = client.scatter(x)  # ephemeral ref in this process, which does not persist
    ll = plasma_session.list()
    assert len(ll) == 1
    assert list(ll.values())[0]["ref_count"] == 1  # only lasting ref in worker
    assert len(client.who_has(f)[f.key]) == 1
    client.replicate(f)
    wait_for(lambda: len(client.who_has(f)[f.key]) == 2, timeout=5)
    ll = plasma_session.list()
    assert len(ll) == 1
    assert list(ll.values())[0]["ref_count"] == 2
    client.close()


@pytest.fixture()
def lmdb_deleter():
    import shutil

    yield
    shutil.rmtree("/tmp/lmdb")


@gen_cluster(
    client=True,
    client_kwargs={"serializers": ["lmdb", "error"]},
    worker_kwargs={"serializers": ["lmdb", "error"]},
)
async def test_lmdb_client_worker(c, s, a, b, lmdb_deleter):
    from distributed.protocol.shared import _get_lmdb

    await b.close()  # ensure we reuse the worker data

    lmdb = _get_lmdb()
    # uses temporary and unlikely hard-coded buffer min size of 1 byte
    x = np.arange(100)  # 800 bytes
    f = await c.scatter(x)  # produces one shared buffer
    assert lmdb.stat()["entries"] == 1
    f2 = c.submit(inc, f)  # does not yet make new shared buffer
    f3 = c.submit(inc, f)  # does not make another buffer, key exists
    out = await f2  # getting result creates buffer for y + 1
    _ = await c.gather([f2, f3])  # does not make another buffer, ser already exists
    assert (x + 1 == out).all()


vineyard_path = "/tmp/vineyard.sock"


@pytest.fixture(scope="module")
def vineyard_process():
    pytest.importorskip("vineyard")
    cmd = [sys.executable, '-m', 'vineyard', '--socket', vineyard_path, '--size', '256Mi', '--meta', 'local']
    proc = subprocess.Popen(cmd)
    yield
    proc.terminate()
    proc.wait()


@pytest.fixture()
def vineyard_session(vineyard_process):
    import vineyard

    timeout = 10
    while True:
        try:
            client = vineyard.connect(vineyard_path)
            if client.connected:
                break
        except (ValueError, TypeError, RuntimeError):
            timeout -= 0.1
            if timeout < 0:
                raise RuntimeError
            time.sleep(0.1)
    yield client


def test_vineyard_worker_worker(vineyard_session):
    x = np.arange(100)  # 800 bytes
    client = dask.distributed.Client(
        n_workers=2,
        threads_per_worker=1,
        serializers=["vineyard", "error"],
        worker_passthrough=dict(serializers=["vineyard", "error"]),
    )

    # ensure our config got through
    sers = client.run(lambda: dask.distributed.get_worker().rpc.serializers)
    assert list(sers.values()) == [["vineyard", "error"], ["vineyard", "error"]]

    f = client.scatter(x)  # ephemeral ref in this process, which does not persist
    assert len(client.who_has(f)[f.key]) == 1
    client.replicate(f)
    wait_for(lambda: len(client.who_has(f)[f.key]) == 2, timeout=5)
    client.close()


@gen_cluster(client=True, client_kwargs={"serializers": ["vineyard", "error"]})
async def test_vineyard_client_worker(c, s, a, b, vineyard_session):
    await b.close()  # ensure we reuse the worker data
    # uses temporary and unlikely hard-coded buffer min size of 1 byte
    x = np.arange(100)  # 800 bytes
    f = await c.scatter(x)  # produces one shared buffer
    f2 = c.submit(inc, f)  # does not yet make new shared buffer
    f3 = c.submit(inc, f)  # does not make another buffer, key exists
    out = await f2  # getting result creates buffer for y + 1
    _ = await c.gather([f2, f3])  # does not make another buffer, ser already exists
    assert (x + 1 == out).all()

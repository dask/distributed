from __future__ import annotations

import shlex
import subprocess
import time

import pytest

import dask.distributed

plasma = pytest.importorskip("pyarrow.plasma")
np = pytest.importorskip("numpy")

from distributed.utils_test import gen_cluster, inc, wait_for

path = "/tmp/plasma"


@pytest.fixture(scope="module")
def plasma_process():
    cmd = shlex.split(f"plasma_store -m 10000000 -s {path}")  # 10MB
    proc = subprocess.Popen(cmd)
    yield
    proc.terminate()
    proc.wait()


@pytest.fixture()
def plasma_session(plasma_process):
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
async def test_client_worker(c, s, a, b, plasma_session):
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


def test_worker_worker(plasma_session):
    import os

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

    print(os.getpid(), client.run(os.getpid))
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

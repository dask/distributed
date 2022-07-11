from __future__ import annotations

import shlex
import subprocess
import time

import pytest

plasma = pytest.importorskip("pyarrow.plasma")
np = pytest.importorskip("numpy")

from distributed.utils_test import gen_cluster, inc

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
async def test_roundtrip(c, s, a, b, plasma_session):
    await b.close()  # ensure we reuse the worker data
    # uses temporary and unlikely hard-coded buffer min size of 1 byte
    x = np.arange(100)  # 800 bytes
    f = await c.scatter(x)  # produces one shared buffer
    assert len(plasma_session.list()) == 1
    f2 = c.submit(inc, f)  # does not yet make new shared buffer
    assert len(plasma_session.list()) == 1
    f3 = c.submit(inc, f)  # does not make another buffer, key exists
    assert len(plasma_session.list()) == 1
    out = await f2  # getting result creates buffer for y + 1
    assert len(plasma_session.list()) == 2
    _ = await c.gather([f2, f3])  # does not make another buffer, ser already exists
    assert (x + 1 == out).all()
    assert all([_["data_size"] == x.nbytes for _ in plasma_session.list().values()])
    assert len(plasma_session.list()) == 2

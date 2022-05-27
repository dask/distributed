import shlex
import subprocess

import pytest

plasma = pytest.importorskip("pyarrow.plasma")
np = pytest.importorskip("numpy")

from distributed.utils_test import gen_cluster

path = "/tmp/plasma"


@pytest.fixture(scope="module")
def plasma_process():
    cmd = shlex.split(f"plasma_store -m 10000000 -s {path}")  # 10MB
    proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    yield
    proc.terminate()
    proc.wait()


@pytest.fixture()
def plasma_session(plasma_process):
    client = plasma.connect(path)
    client.evict(1000000)
    yield client
    client.evict(1000000)


@gen_cluster(client=True)
async def test_roundtrip(c, s, a, b, plasma_session):
    # uses temporary and unlikely hard-coded buffer min size of 1 byte
    x = np.arange(100)  # 800 bytes
    f = await c.scatter(x)  # produces one shared buffer
    f2 = c.submit(lambda y: y + 1, f)  # produces another shared buffer
    f3 = c.submit(lambda y: y + 1, f)  # does not make another buffer, key exists
    out = await f2
    _ = await c.gather([f2, f3])  # does not make another buffer, ser already exists
    assert (x + 1 == out).all()
    assert all([_["data_size"] == x.nbytes for _ in plasma_session.list().values()])

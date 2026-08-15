from __future__ import annotations

import sys
from time import sleep

import pytest

pytest.importorskip("paramiko")

from distributed import Client
from distributed.deploy.old_ssh import SSHCluster
from distributed.metrics import time

# https://github.com/dask/distributed/issues/9114
pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="Hangs on Windows in CI"
)


@pytest.mark.skip
def test_cluster(loop):
    with SSHCluster(
        scheduler_addr="127.0.0.1",
        scheduler_port=7437,
        worker_addrs=["127.0.0.1", "127.0.0.1"],
    ) as c:
        with Client(c, loop=loop) as e:
            start = time()
            while len(e.ncores()) != 2:
                sleep(0.01)
                assert time() < start + 5

            c.add_worker("127.0.0.1")

            start = time()
            while len(e.ncores()) != 3:
                sleep(0.01)
                assert time() < start + 5

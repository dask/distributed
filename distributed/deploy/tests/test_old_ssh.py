from time import sleep

import pytest

pytest.importorskip("paramiko")

from distributed import Client
from distributed.deploy.old_ssh import SSHCluster
from distributed.metrics import time


@pytest.mark.avoid_ci
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


def test_old_ssh_nprocs_renamed_to_num_workers():
    with pytest.warns(FutureWarning, match="renamed to num_workers"):
        with SSHCluster(
            scheduler_addr="127.0.0.1",
            scheduler_port=8687,
            worker_addrs=["127.0.0.1", "127.0.0.1"],
            nprocs=2,
        ) as c:
            assert c.num_workers == 2


def test_old_ssh_num_workers_with_nprocs_is_an_error():
    with pytest.raises(ValueError, match="Both nprocs and num_workers"):
        SSHCluster(
            scheduler_addr="127.0.0.1",
            scheduler_port=8687,
            worker_addrs=(),
            nprocs=2,
            num_workers=2,
        )

import asyncio
import os
import pytest
from time import sleep

ucp = pytest.importorskip("ucp")

import dask
from dask.utils import format_bytes
from distributed import Client, Worker, Scheduler, wait
from distributed.comm import ucx, listen, connect
from distributed.comm.registry import backends, get_backend
from distributed.comm import ucx, parse_address
from distributed.protocol import to_serialize
from distributed.deploy.local import LocalCluster
from dask.dataframe.utils import assert_eq
from distributed.utils_test import gen_test, loop, inc, cleanup, popen  # noqa: 401
from distributed.utils import tmpfile, get_ip, ignoring

from .test_comms import check_deserialize


try:
    HOST = get_ip()
except Exception:
    HOST = "127.0.0.1"


@pytest.mark.asyncio
async def test_ucx_config(cleanup):
    ucx = {"nvlink": True, "infiniband": True, "tcp-over-ucx": True}

    def get_ucp_config():
        import ucp

        return ucp.get_config()

    with dask.config.set(ucx=ucx):
        async with Scheduler(protocol="ucx") as s:
            async with Worker(s.address) as a:
                async with Client(s.address, asynchronous=True) as c:

                    # scheduler is configured with NVLINK/IB
                    ucx_config = await c.run_on_scheduler(get_ucp_config)
                    assert ucx_config.get("TLS") == "rc,tcp,sockcm,cuda_copy,cuda_ipc"
                    assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"

                    # worker is configured with NVLINK/IB
                    worker_ucx_config = await c.run(get_ucp_config)
                    ucx_config = worker_ucx_config[a.contact_address]
                    assert ucx_config.get("TLS") == "rc,tcp,sockcm,cuda_copy,cuda_ipc"
                    assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"


@pytest.mark.asyncio
async def test_ucx_w_rmm(cleanup):
    ucx = {"nvlink": True, "infiniband": True, "tcp-over-ucx": True}
    size = "1000.00 MB"
    rmm = {"pool-size": size}

    def get_ucp_config():
        import ucp

        return ucp.get_config()

    def get_rmm():
        import rmm

        return rmm.get_info()

    with dask.config.set(ucx=ucx, rmm=rmm):
        async with Scheduler(protocol="ucx") as s:
            async with Worker(s.address) as a:
                async with Client(s.address, asynchronous=True) as c:

                    # scheduler is configured with NVLINK/IB
                    ucx_config = await c.run_on_scheduler(get_ucp_config)
                    assert ucx_config.get("TLS") == "rc,tcp,sockcm,cuda_copy,cuda_ipc"
                    assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"

                    # configured with 1G pool
                    rmm_usage = await c.run_on_scheduler(get_rmm)
                    assert size == format_bytes(rmm_usage.free)

                    # worker is configured with NVLINK/IB
                    worker_ucx_config = await c.run(get_ucp_config)
                    ucx_config = worker_ucx_config[a.contact_address]
                    assert ucx_config.get("TLS") == "rc,tcp,sockcm,cuda_copy,cuda_ipc"
                    assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"

                    # configured with 1G pool
                    worker_rmm_usage = await c.run(get_rmm)
                    rmm_usage = worker_rmm_usage[a.contact_address]
                    assert size == format_bytes(rmm_usage.free)


def test_ucx_config_env_var(cleanup, loop, monkeypatch):
    def get_ucp_config():
        import ucp

        return ucp.get_config()

    monkeypatch.setenv("DASK_UCX__NVLNK", "False")
    monkeypatch.setenv("DASK_UCX__INFINIBAND", "True")
    monkeypatch.setenv("DASK_UCX__TCP_OVER_UCX", "True")

    port = "13337"
    sched_addr = "ucx://%s:%s" % (HOST, port)
    with popen(
        ["dask-scheduler", "--no-dashboard", "--protocol", "ucx", "--port", port]
    ) as sched:
        with popen(["dask-worker", sched_addr, "--no-dashboard", "--protocol", "ucx"]):
            with Client(sched_addr, loop=loop, timeout=10) as c:
                while not c.scheduler_info()["workers"]:
                    sleep(0.1)

                # scheduler is configured with NVLINK (no cuda_ipc)
                ucx_config = c.run_on_scheduler(get_ucp_config)
                assert ucx_config.get("TLS") == "rc,tcp,sockcm,cuda_copy"
                assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"

                # worker is configured with NVLINK (no cuda_ipc)
                ucx_config = c.run(get_ucp_config)
                assert ucx_config.get("TLS") == "rc,tcp,sockcm,cuda_copy"
                assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"

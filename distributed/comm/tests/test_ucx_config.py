import os
import pytest
from time import sleep

import dask
from dask.utils import format_bytes
from distributed import Client, Worker, Scheduler
from distributed.utils_test import gen_test, loop, inc, cleanup, popen  # noqa: 401
from distributed.utils import get_ip


def _check_dgx_version():
    dgx_server = None

    if not os.path.isfile("/etc/dgx-release"):
        return dgx_server

    for line in open("/etc/dgx-release"):
        if line.startswith("DGX_PLATFORM"):
            if "DGX Server for DGX-1" in line:
                dgx_server = 1
            elif "DGX Server for DGX-2" in line:
                dgx_server = 2
            break

    return dgx_server


if _check_dgx_version() is None:
    pytest.skip("Not a DGX server", allow_module_level=True)

try:
    HOST = get_ip()
except Exception:
    HOST = "127.0.0.1"


@pytest.mark.asyncio
async def test_ucx_config(cleanup):
    ucp = pytest.importorskip("ucp")
    rmm = pytest.importorskip("rmm")

    ucx = {
        "nvlink": True,
        "infiniband": True,
        "tcp-over-ucx": True,
        "net-devices": "mlx5_0:1",
    }

    with dask.config.set(ucx=ucx):
        async with Scheduler(protocol="ucx") as s:
            async with Worker(s.address) as a:
                async with Client(s.address, asynchronous=True) as c:

                    # scheduler is configured with NVLINK/IB
                    ucx_config = await c.run_on_scheduler(ucp.get_config)
                    assert ucx_config.get("TLS") == "rc,tcp,sockcm,cuda_copy,cuda_ipc"
                    assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"
                    assert ucx_config.get("NET_DEVICES") == "mlx5_0:1"

                    # worker is configured with NVLINK/IB
                    worker_ucx_config = await c.run(ucp.get_config)
                    ucx_config = worker_ucx_config[a.contact_address]
                    assert ucx_config.get("TLS") == "rc,tcp,sockcm,cuda_copy,cuda_ipc"
                    assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"
                    assert ucx_config.get("NET_DEVICES") == "mlx5_0:1"


@pytest.mark.asyncio
async def test_ucx_w_rmm(cleanup):
    ucp = pytest.importorskip("ucp")
    rmm = pytest.importorskip("rmm")

    port = 13338
    ucx = {"nvlink": True, "infiniband": True, "tcp-over-ucx": True, "net-devices": ""}
    size = "1000.00 MB"
    rmm_dict = {"pool-size": size}

    with dask.config.set(ucx=ucx, rmm=rmm_dict):
        async with Scheduler(protocol="ucx", port=port) as s:
            async with Worker(s.address) as a:
                async with Client(s.address, asynchronous=True) as c:

                    # scheduler is configured with NVLINK/IB
                    ucx_config = await c.run_on_scheduler(ucp.get_config)
                    assert ucx_config.get("TLS") == "rc,tcp,sockcm,cuda_copy,cuda_ipc"
                    assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"

                    # configured with 1G pool
                    rmm_usage = await c.run_on_scheduler(rmm.get_info)
                    assert size == format_bytes(rmm_usage.free)

                    # worker is configured with NVLINK/IB
                    worker_ucx_config = await c.run(ucp.get_config)
                    ucx_config = worker_ucx_config[a.contact_address]
                    assert ucx_config.get("TLS") == "rc,tcp,sockcm,cuda_copy,cuda_ipc"
                    assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"

                    # configured with 1G pool
                    worker_rmm_usage = await c.run(rmm.get_info)
                    rmm_usage = worker_rmm_usage[a.contact_address]
                    assert size == format_bytes(rmm_usage.free)


def test_ucx_config_w_env_var(cleanup, loop, monkeypatch):
    ucp = pytest.importorskip("ucp")
    rmm = pytest.importorskip("rmm")

    monkeypatch.setenv("DASK_UCX__NVLINK", "False")
    monkeypatch.setenv("DASK_UCX__INFINIBAND", "True")
    monkeypatch.setenv("DASK_UCX__TCP_OVER_UCX", "True")
    monkeypatch.setenv("DASK_UCX__NET_DEVICES", "")

    dask.config.refresh()

    port = "13339"
    sched_addr = "ucx://%s:%s" % (HOST, port)

    with popen(
        ["dask-scheduler", "--no-dashboard", "--protocol", "ucx", "--port", port]
    ) as sched:
        with popen(
            [
                "dask-worker",
                sched_addr,
                "--no-dashboard",
                "--protocol",
                "ucx",
                "--no-nanny",
            ]
        ) as w:
            with Client(sched_addr, loop=loop, timeout=10) as c:
                while not c.scheduler_info()["workers"]:
                    sleep(0.1)

                # scheduler is configured with NVLINK (no cuda_ipc)
                ucx_config = c.run_on_scheduler(ucp.get_config)
                assert ucx_config.get("TLS") == "rc,tcp,sockcm,cuda_copy"
                assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"

                # worker is configured with NVLINK (no cuda_ipc)
                worker_addr = list(c.scheduler_info()["workers"])[0]
                worker_ucx_config = c.run(ucp.get_config)
                ucx_config = worker_ucx_config[worker_addr]
                assert ucx_config.get("TLS") == "rc,tcp,sockcm,cuda_copy"
                assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"

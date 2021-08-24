from time import sleep

import pytest

pytestmark = pytest.mark.gpu

import dask

from distributed import Client
from distributed.comm.ucx import _scrub_ucx_config
from distributed.utils import get_ip
from distributed.utils_test import popen

try:
    HOST = get_ip()
except Exception:
    HOST = "127.0.0.1"

ucp = pytest.importorskip("ucp")
rmm = pytest.importorskip("rmm")


@pytest.mark.asyncio
async def test_ucx_config(cleanup):
    ucx_110 = ucp.get_ucx_version() >= (1, 10, 0)

    ucx = {
        "nvlink": True,
        "infiniband": True,
        "rdmacm": False,
        "net-devices": "",
        "tcp": True,
        "cuda_copy": True,
    }

    with dask.config.set({"distributed.comm.ucx": ucx}):
        ucx_config = _scrub_ucx_config()
        if ucx_110:
            assert ucx_config.get("TLS") == "rc,tcp,cuda_copy,cuda_ipc"
            assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "tcp"
        else:
            assert ucx_config.get("TLS") == "rc,tcp,sockcm,cuda_copy,cuda_ipc"
            assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"
        assert ucx_config.get("NET_DEVICES") is None

    ucx = {
        "nvlink": False,
        "infiniband": True,
        "rdmacm": False,
        "net-devices": "mlx5_0:1",
        "tcp": True,
        "cuda_copy": False,
    }

    with dask.config.set({"distributed.comm.ucx": ucx}):
        ucx_config = _scrub_ucx_config()
        if ucx_110:
            assert ucx_config.get("TLS") == "rc,tcp"
            assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "tcp"
        else:
            assert ucx_config.get("TLS") == "rc,tcp,sockcm"
            assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "sockcm"
        assert ucx_config.get("NET_DEVICES") == "mlx5_0:1"

    ucx = {
        "nvlink": False,
        "infiniband": True,
        "rdmacm": True,
        "net-devices": "all",
        "tcp": True,
        "cuda_copy": True,
    }

    with dask.config.set({"distributed.comm.ucx": ucx}):
        ucx_config = _scrub_ucx_config()
        if ucx_110:
            assert ucx_config.get("TLS") == "rc,tcp,cuda_copy"
        else:
            assert ucx_config.get("TLS") == "rc,tcp,rdmacm,cuda_copy"
        assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "rdmacm"


def test_ucx_config_w_env_var(cleanup, loop, monkeypatch):
    size = "1000.00 MB"
    monkeypatch.setenv("DASK_RMM__POOL_SIZE", size)

    dask.config.refresh()

    port = "13339"
    sched_addr = f"ucx://{HOST}:{port}"

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

                # Check for RMM pool resource type
                rmm_resource = c.run_on_scheduler(
                    rmm.mr.get_current_device_resource_type
                )
                assert rmm_resource == rmm.mr.PoolMemoryResource

                worker_addr = list(c.scheduler_info()["workers"])[0]
                worker_rmm_usage = c.run(rmm.mr.get_current_device_resource_type)
                rmm_resource = worker_rmm_usage[worker_addr]
                assert rmm_resource == rmm.mr.PoolMemoryResource

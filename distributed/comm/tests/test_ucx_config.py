import os
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
    ucx = {
        "nvlink": True,
        "infiniband": True,
        "rdmacm": False,
        "tcp": True,
        "cuda-copy": True,
    }

    with dask.config.set({"distributed.comm.ucx": ucx}):
        ucx_config = _scrub_ucx_config()
        assert ucx_config.get("TLS") == "rc,tcp,cuda_copy,cuda_ipc"
        assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "tcp"

    ucx = {
        "nvlink": False,
        "infiniband": True,
        "rdmacm": False,
        "tcp": True,
        "cuda-copy": False,
    }

    with dask.config.set({"distributed.comm.ucx": ucx}):
        ucx_config = _scrub_ucx_config()
        assert ucx_config.get("TLS") == "rc,tcp"
        assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "tcp"

    ucx = {
        "nvlink": False,
        "infiniband": True,
        "rdmacm": True,
        "tcp": True,
        "cuda-copy": True,
    }

    with dask.config.set({"distributed.comm.ucx": ucx}):
        ucx_config = _scrub_ucx_config()
        assert ucx_config.get("TLS") == "rc,tcp,cuda_copy"
        assert ucx_config.get("SOCKADDR_TLS_PRIORITY") == "rdmacm"

    ucx = {
        "nvlink": None,
        "infiniband": None,
        "rdmacm": None,
        "tcp": None,
        "cuda-copy": None,
    }

    with dask.config.set({"distributed.comm.ucx": ucx}):
        ucx_config = _scrub_ucx_config()
        assert ucx_config == {}


def test_ucx_config_w_env_var(cleanup, loop):
    env = os.environ.copy()
    env["DASK_RMM__POOL_SIZE"] = "1000.00 MB"

    port = "13339"
    sched_addr = f"ucx://{HOST}:{port}"

    with popen(
        ["dask-scheduler", "--no-dashboard", "--protocol", "ucx", "--port", port],
        env=env,
    ) as sched:
        with popen(
            [
                "dask-worker",
                sched_addr,
                "--no-dashboard",
                "--protocol",
                "ucx",
                "--no-nanny",
            ],
            env=env,
        ):
            with Client(sched_addr, loop=loop, timeout=10) as c:
                while not c.scheduler_info()["workers"]:
                    sleep(0.1)

                # Check for RMM pool resource type
                rmm_resource = c.run_on_scheduler(
                    rmm.mr.get_current_device_resource_type
                )
                assert rmm_resource == rmm.mr.PoolMemoryResource

                rmm_resource_workers = c.run(rmm.mr.get_current_device_resource_type)
                for v in rmm_resource_workers.values():
                    assert v == rmm.mr.PoolMemoryResource

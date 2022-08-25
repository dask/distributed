from __future__ import annotations

import os
from time import sleep

import pytest

pytestmark = pytest.mark.gpu

import dask

from distributed import Client
from distributed.comm.ucx import _scrub_ucx_config
from distributed.utils import get_ip
from distributed.utils_test import gen_test, popen

try:
    HOST = get_ip()
except Exception:
    HOST = "127.0.0.1"

ucp = pytest.importorskip("ucp")
rmm = pytest.importorskip("rmm")


@gen_test()
async def test_ucx_config(ucx_loop, cleanup):
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


@pytest.mark.flaky(
    reruns=10,
    reruns_delay=5,
)
def test_ucx_config_w_env_var(ucx_loop, cleanup, loop):
    env = os.environ.copy()
    env["DASK_RMM__POOL_SIZE"] = "1000.00 MB"

    port = "13339"
    # Using localhost appears to be less flaky than {HOST}. Additionally, this is
    # closer to how other dask-worker tests are written.
    sched_addr = f"ucx://127.0.0.1:{port}"

    with popen(
        ["dask-scheduler", "--no-dashboard", "--protocol", "ucx", "--port", port],
        env=env,
    ):
        with popen(
            [
                "dask-worker",
                sched_addr,
                "--host",
                "127.0.0.1",
                "--no-dashboard",
                "--protocol",
                "ucx",
                "--no-nanny",
            ],
            env=env,
        ):
            with Client(sched_addr, loop=loop, timeout=60) as c:
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

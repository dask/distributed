import asyncio
import os
from multiprocessing import cpu_count
from time import sleep

import pytest
from click.testing import CliRunner

from dask.utils import tmpfile

import distributed.cli.dask_worker
from distributed import Client
from distributed.compatibility import LINUX, to_thread
from distributed.deploy.utils import nprocesses_nthreads
from distributed.metrics import time
from distributed.utils_test import gen_cluster, popen, requires_ipv6


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[])
async def test_nanny_worker_ports(c, s):
    with popen(
        [
            "dask-worker",
            s.address,
            "--host",
            "127.0.0.1",
            "--worker-port",
            "9684",
            "--nanny-port",
            "5273",
            "--no-dashboard",
        ]
    ):
        await c.wait_for_workers(1)
        d = await c.scheduler.identity()
        assert d["workers"]["tcp://127.0.0.1:9684"]["nanny"] == "tcp://127.0.0.1:5273"


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[])
async def test_nanny_worker_port_range(c, s):
    with popen(
        [
            "dask-worker",
            s.address,
            "--nworkers",
            "3",
            "--host",
            "127.0.0.1",
            "--worker-port",
            "9684:9686",
            "--nanny-port",
            "9688:9690",
            "--no-dashboard",
        ]
    ):
        await c.wait_for_workers(3)
        worker_ports = await c.run(lambda dask_worker: dask_worker.port)
        assert set(worker_ports.values()) == {9684, 9685, 9686}
        nanny_ports = await c.run(lambda dask_worker: dask_worker.port, nanny=True)
        assert set(nanny_ports.values()) == {9688, 9689, 9690}


@gen_cluster(nthreads=[])
async def test_nanny_worker_port_range_too_many_workers_raises(s):
    with popen(
        [
            "dask-worker",
            s.address,
            "--nworkers",
            "3",
            "--host",
            "127.0.0.1",
            "--worker-port",
            "9684:9685",
            "--nanny-port",
            "9686:9687",
            "--no-dashboard",
        ],
        flush_output=False,
    ) as worker:
        assert any(b"Could not start" in worker.stdout.readline() for _ in range(100))


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[])
async def test_memory_limit(c, s):
    with popen(
        [
            "dask-worker",
            s.address,
            "--memory-limit",
            "2e3MB",
            "--no-dashboard",
        ]
    ):
        await c.wait_for_workers(1)
        info = c.scheduler_info()
        (d,) = info["workers"].values()
        assert isinstance(d["memory_limit"], int)
        assert d["memory_limit"] == 2e9


@gen_cluster(client=True, nthreads=[])
async def test_no_nanny(c, s):
    with popen(["dask-worker", s.address, "--no-nanny", "--no-dashboard"]):
        await c.wait_for_workers(1)


@pytest.mark.slow
@pytest.mark.parametrize("nanny", ["--nanny", "--no-nanny"])
@gen_cluster(client=True, nthreads=[])
async def test_no_reconnect(c, s, nanny):
    with popen(
        [
            "dask-worker",
            s.address,
            "--no-reconnect",
            nanny,
            "--no-dashboard",
        ],
        flush_output=False,
    ) as worker:
        # roundtrip works
        assert await c.submit(lambda x: x + 1, 10) == 11

        (comm,) = s.stream_comms.values()
        comm.abort()

        # worker terminates as soon as the connection is aborted
        await to_thread(worker.communicate, timeout=5)
        assert worker.returncode == 0


@pytest.mark.slow
@pytest.mark.parametrize("nanny", ["--nanny", "--no-nanny"])
@gen_cluster(client=True, nthreads=[])
async def test_reconnect(c, s, nanny):
    with popen(
        [
            "dask-worker",
            s.address,
            "--reconnect",
            nanny,
            "--no-dashboard",
        ],
        flush_output=False,
    ) as worker:
        # roundtrip works
        await c.submit(lambda x: x + 1, 10) == 11

        (comm,) = s.stream_comms.values()
        comm.abort()

        # roundtrip still works, which means the worker reconnected
        assert await c.submit(lambda x: x + 1, 11) == 12

        # closing the scheduler cleanly does terminate the worker
        await s.close()
        await to_thread(worker.communicate, timeout=5)
        assert worker.returncode == 0


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[])
async def test_resources(c, s):
    with popen(
        [
            "dask-worker",
            s.address,
            "--no-dashboard",
            "--resources",
            "A=1 B=2,C=3",
        ]
    ):
        await c.wait_for_workers(1)
        info = c.scheduler_info()
        (d,) = info["workers"].values()
        assert d["resources"] == {"A": 1, "B": 2, "C": 3}


@pytest.mark.slow
@pytest.mark.parametrize("nanny", ["--nanny", "--no-nanny"])
@gen_cluster(client=True, nthreads=[])
async def test_local_directory(c, s, nanny, tmpdir):
    with popen(
        [
            "dask-worker",
            s.address,
            nanny,
            "--no-dashboard",
            "--local-directory",
            str(tmpdir),
        ]
    ):
        await c.wait_for_workers(1)
        info = c.scheduler_info()
        (d,) = info["workers"].values()
        assert d["local_directory"].startswith(str(tmpdir))


@pytest.mark.slow
@pytest.mark.parametrize("nanny", ["--nanny", "--no-nanny"])
def test_scheduler_file(loop, nanny):
    with tmpfile() as fn:
        with popen(["dask-scheduler", "--no-dashboard", "--scheduler-file", fn]):
            with popen(
                ["dask-worker", "--scheduler-file", fn, nanny, "--no-dashboard"]
            ):
                with Client(scheduler_file=fn, loop=loop) as c:
                    start = time()
                    while not c.scheduler_info()["workers"]:
                        sleep(0.1)
                        assert time() < start + 10


@pytest.mark.slow
def test_scheduler_address_env(loop, monkeypatch):
    monkeypatch.setenv("DASK_SCHEDULER_ADDRESS", "tcp://127.0.0.1:8786")
    with popen(["dask-scheduler", "--no-dashboard"]):
        with popen(["dask-worker", "--no-dashboard"]):
            with Client(os.environ["DASK_SCHEDULER_ADDRESS"], loop=loop) as c:
                start = time()
                while not c.scheduler_info()["workers"]:
                    sleep(0.1)
                    assert time() < start + 10


@gen_cluster(nthreads=[])
async def test_nworkers_requires_nanny(s):
    with popen(
        ["dask-worker", s.address, "--nworkers=2", "--no-nanny"],
        flush_output=False,
    ) as worker:
        assert any(
            b"Failed to launch worker" in worker.stdout.readline() for _ in range(15)
        )


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[])
async def test_nworkers_negative(c, s):
    with popen(["dask-worker", s.address, "--nworkers=-1"]):
        await c.wait_for_workers(cpu_count())


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[])
async def test_nworkers_auto(c, s):
    with popen(["dask-worker", s.address, "--nworkers=auto"]):
        procs, _ = nprocesses_nthreads()
        await c.wait_for_workers(procs)


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[])
async def test_nworkers_expands_name(c, s):
    with popen(["dask-worker", s.address, "--nworkers", "2", "--name", "0"]):
        with popen(["dask-worker", s.address, "--nworkers", "2"]):
            await c.wait_for_workers(4)
            info = c.scheduler_info()

    names = [d["name"] for d in info["workers"].values()]
    assert len(names) == len(set(names)) == 4
    zeros = [n for n in names if n.startswith("0-")]
    assert len(zeros) == 2


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[])
async def test_worker_cli_nprocs_renamed_to_nworkers(c, s):
    with popen(
        ["dask-worker", s.address, "--nprocs=2"],
        flush_output=False,
    ) as worker:
        await c.wait_for_workers(2)
        assert any(
            b"renamed to --nworkers" in worker.stdout.readline() for _ in range(15)
        )


@gen_cluster(nthreads=[])
async def test_worker_cli_nworkers_with_nprocs_is_an_error(s):
    with popen(
        ["dask-worker", s.address, "--nprocs=2", "--nworkers=2"],
        flush_output=False,
    ) as worker:
        assert any(
            b"Both --nprocs and --nworkers" in worker.stdout.readline()
            for _ in range(15)
        )


@pytest.mark.slow
@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@pytest.mark.parametrize("nanny", ["--nanny", "--no-nanny"])
@pytest.mark.parametrize(
    "listen_address", ["tcp://0.0.0.0:39837", "tcp://127.0.0.2:39837"]
)
@gen_cluster(client=True, nthreads=[])
async def test_contact_listen_address(c, s, nanny, listen_address):
    with popen(
        [
            "dask-worker",
            s.address,
            nanny,
            "--no-dashboard",
            "--contact-address",
            "tcp://127.0.0.2:39837",
            "--listen-address",
            listen_address,
        ]
    ):
        await c.wait_for_workers(1)
        info = c.scheduler_info()
        assert info["workers"].keys() == {"tcp://127.0.0.2:39837"}

        # roundtrip works
        assert await c.submit(lambda x: x + 1, 10) == 11

        def func(dask_worker):
            return dask_worker.listener.listen_address

        assert await c.run(func) == {"tcp://127.0.0.2:39837": listen_address}


@pytest.mark.slow
@requires_ipv6
@pytest.mark.parametrize("nanny", ["--nanny", "--no-nanny"])
@pytest.mark.parametrize("listen_address", ["tcp://:39838", "tcp://[::1]:39838"])
@gen_cluster(client=True, nthreads=[])
async def test_listen_address_ipv6(c, s, nanny, listen_address):
    with popen(
        [
            "dask-worker",
            s.address,
            nanny,
            "--no-dashboard",
            "--listen-address",
            listen_address,
        ]
    ):
        # IPv4 used by default for name of global listener; IPv6 used by default when
        # listening only on IPv6.
        bind_all = "[::1]" not in listen_address
        expected_ip = "127.0.0.1" if bind_all else "[::1]"
        expected_name = f"tcp://{expected_ip}:39838"
        expected_listen = "tcp://0.0.0.0:39838" if bind_all else listen_address

        await c.wait_for_workers(1)
        info = c.scheduler_info()
        assert info["workers"].keys() == {expected_name}
        assert await c.submit(lambda x: x + 1, 10) == 11

        def func(dask_worker):
            return dask_worker.listener.listen_address

        assert await c.run(func) == {expected_name: expected_listen}


@pytest.mark.slow
@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
@pytest.mark.parametrize("nanny", ["--nanny", "--no-nanny"])
@pytest.mark.parametrize("host", ["127.0.0.2", "0.0.0.0"])
@gen_cluster(client=True, nthreads=[])
async def test_respect_host_listen_address(c, s, nanny, host):
    with popen(["dask-worker", s.address, nanny, "--no-dashboard", "--host", host]):
        await c.wait_for_workers(1)

        # roundtrip works
        assert await c.submit(lambda x: x + 1, 10) == 11

        def func(dask_worker):
            return dask_worker.listener.listen_address

        listen_addresses = await c.run(func)
        assert all(host in v for v in listen_addresses.values())


@pytest.mark.slow
@gen_cluster(
    client=True, nthreads=[], scheduler_kwargs={"dashboard_address": "localhost:8787"}
)
async def test_dashboard_non_standard_ports(c, s):
    pytest.importorskip("bokeh")
    requests = pytest.importorskip("requests")

    try:
        import jupyter_server_proxy  # noqa: F401

        proxy_exists = True
    except ImportError:
        proxy_exists = False

    with popen(
        [
            "dask-worker",
            s.address,
            "--dashboard-address",
            ":4833",
            "--host",
            "127.0.0.1",
        ]
    ):
        await c.wait_for_workers(1)

        response = requests.get("http://127.0.0.1:4833/status")
        response.raise_for_status()
        # TEST PROXYING WORKS
        if proxy_exists:
            response = requests.get("http://127.0.0.1:8787/proxy/4833/127.0.0.1/status")
            response.raise_for_status()

    with pytest.raises(requests.ConnectionError):
        requests.get("http://localhost:4833/status/")


def test_version_option():
    runner = CliRunner()
    result = runner.invoke(distributed.cli.dask_worker.main, ["--version"])
    assert result.exit_code == 0


@pytest.mark.slow
@pytest.mark.parametrize("no_nanny", [True, False])
def test_worker_timeout(no_nanny):
    runner = CliRunner()
    args = ["192.168.1.100:7777", "--death-timeout=1"]
    if no_nanny:
        args.append("--no-nanny")
    result = runner.invoke(distributed.cli.dask_worker.main, args)
    assert result.exit_code != 0


def test_bokeh_deprecation():
    pytest.importorskip("bokeh")

    runner = CliRunner()
    with pytest.warns(UserWarning, match="dashboard"):
        try:
            runner.invoke(distributed.cli.dask_worker.main, ["--bokeh"])
        except ValueError:
            # didn't pass scheduler
            pass

    with pytest.warns(UserWarning, match="dashboard"):
        try:
            runner.invoke(distributed.cli.dask_worker.main, ["--no-bokeh"])
        except ValueError:
            # didn't pass scheduler
            pass


@pytest.mark.slow
@gen_cluster(nthreads=[])
async def test_integer_names(s):
    with popen(["dask-worker", s.address, "--name", "123"]):
        while not s.workers:
            await asyncio.sleep(0.01)
        [ws] = s.workers.values()
        assert ws.name == 123


@pytest.mark.slow
@pytest.mark.parametrize("nanny", ["--nanny", "--no-nanny"])
@gen_cluster(client=True, nthreads=[])
async def test_worker_class(c, s, tmp_path, nanny):
    # Create module with custom worker class
    WORKER_CLASS_TEXT = """
from distributed.worker import Worker

class MyWorker(Worker):
    pass
"""
    tmpdir = str(tmp_path)
    tmpfile = str(tmp_path / "myworker.py")
    with open(tmpfile, "w") as f:
        f.write(WORKER_CLASS_TEXT)

    # Put module on PYTHONPATH
    env = os.environ.copy()
    if "PYTHONPATH" in env:
        env["PYTHONPATH"] = tmpdir + ":" + env["PYTHONPATH"]
    else:
        env["PYTHONPATH"] = tmpdir

    with popen(
        [
            "dask-worker",
            s.address,
            nanny,
            "--worker-class",
            "myworker.MyWorker",
        ],
        env=env,
    ):
        await c.wait_for_workers(1)

        def worker_type(dask_worker):
            return type(dask_worker).__name__

        worker_types = await c.run(worker_type)
        assert all(name == "MyWorker" for name in worker_types.values())


@pytest.mark.slow
@gen_cluster(nthreads=[], client=True)
async def test_preload_config(c, s):
    # Ensure dask-worker pulls the preload from the Dask config if
    # not specified via a command line option
    preload_text = """
def dask_setup(worker):
    worker.foo = 'setup'
"""
    env = os.environ.copy()
    env["DASK_DISTRIBUTED__WORKER__PRELOAD"] = preload_text
    with popen(["dask-worker", s.address], env=env):
        await c.wait_for_workers(1)
        [foo] = (await c.run(lambda dask_worker: dask_worker.foo)).values()
        assert foo == "setup"

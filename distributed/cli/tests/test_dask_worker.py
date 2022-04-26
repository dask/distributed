from __future__ import annotations

import asyncio
import os
import signal
import sys
from multiprocessing import cpu_count
from time import sleep

import pytest
from click.testing import CliRunner

from dask.utils import tmpfile

from distributed import Client
from distributed.cli.dask_worker import _apportion_ports, main
from distributed.compatibility import LINUX, to_thread
from distributed.deploy.utils import nprocesses_nthreads
from distributed.metrics import time
from distributed.utils_test import gen_cluster, popen, requires_ipv6


@pytest.mark.parametrize(
    # args: (worker_port, nanny_port, n_workers, nanny)
    # Passing *args tuple instead of single args is to improve readability with black
    "args,expect",
    [
        # Single worker
        (
            (None, None, 1, False),
            [{"port": None}],
        ),
        (
            (None, None, 1, True),
            [{"port": None, "worker_port": None}],
        ),
        (("123", None, 1, False), [{"port": 123}]),
        (
            ("123", None, 1, True),
            [{"port": None, "worker_port": 123}],
        ),
        (
            (None, "456", 1, True),
            [{"port": 456, "worker_port": None}],
        ),
        (
            ("123", "456", 1, True),
            [{"port": 456, "worker_port": 123}],
        ),
        # port=None or 0 and multiple workers
        (
            (None, None, 2, False),
            [
                {"port": None},
                {"port": None},
            ],
        ),
        (
            (None, None, 2, True),
            [
                {"port": None, "worker_port": None},
                {"port": None, "worker_port": None},
            ],
        ),
        (
            (0, "0", 2, True),
            [
                {"port": 0, "worker_port": 0},
                {"port": 0, "worker_port": 0},
            ],
        ),
        (
            ("0", None, 2, True),
            [
                {"port": None, "worker_port": 0},
                {"port": None, "worker_port": 0},
            ],
        ),
        # port ranges
        (
            ("100:103", None, 1, False),
            [{"port": [100, 101, 102, 103]}],
        ),
        (
            ("100:103", None, 2, False),
            [
                {"port": [100, 102]},  # Round robin apportion
                {"port": [101, 103]},
            ],
        ),
        # port range is not an exact multiple of n_workers
        (
            ("100:107", None, 3, False),
            [
                {"port": [100, 103, 106]},
                {"port": [101, 104, 107]},
                {"port": [102, 105]},
            ],
        ),
        (
            ("100:103", None, 2, True),
            [
                {"port": None, "worker_port": [100, 102]},
                {"port": None, "worker_port": [101, 103]},
            ],
        ),
        (
            (None, "110:113", 2, True),
            [
                {"port": [110, 112], "worker_port": None},
                {"port": [111, 113], "worker_port": None},
            ],
        ),
        # port ranges have different length between nannies and workers
        (
            ("100:103", "110:114", 2, True),
            [
                {"port": [110, 112, 114], "worker_port": [100, 102]},
                {"port": [111, 113], "worker_port": [101, 103]},
            ],
        ),
        # identical port ranges
        (
            ("100:103", "100:103", 2, True),
            [
                {"port": 101, "worker_port": 100},
                {"port": 103, "worker_port": 102},
            ],
        ),
        # overlapping port ranges
        (
            ("100:105", "104:106", 2, True),
            [
                {"port": [104, 106], "worker_port": [100, 102]},
                {"port": 105, "worker_port": [101, 103]},
            ],
        ),
    ],
)
def test_apportion_ports(args, expect):
    assert _apportion_ports(*args) == expect


def test_apportion_ports_bad():
    with pytest.raises(ValueError, match="Not enough ports in range"):
        _apportion_ports("100:102", None, 4, False)
    with pytest.raises(ValueError, match="Not enough ports in range"):
        _apportion_ports(None, "100:102", 4, False)
    with pytest.raises(ValueError, match="Not enough ports in range"):
        _apportion_ports("100:102", "100:102", 3, True)
    with pytest.raises(ValueError, match="Not enough ports in range"):
        _apportion_ports("100:102", "102:104", 3, True)
    with pytest.raises(ValueError, match="port_stop must be greater than port_start"):
        _apportion_ports("102:100", None, 4, False)
    with pytest.raises(ValueError, match="invalid literal for int"):
        _apportion_ports("foo", None, 1, False)
    with pytest.raises(ValueError, match="too many values to unpack"):
        _apportion_ports("100:101:102", None, 1, False)


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
@pytest.mark.flaky(
    LINUX and sys.version_info[:2] == (3, 9),
    reason="Race condition in Nanny.process.start? "
    "See https://github.com/dask/distributed/issues/6045",
)
@gen_cluster(client=True, nthreads=[])
async def test_nanny_worker_port_range(c, s):
    async def assert_ports(min_: int, max_: int, nanny: bool) -> None:
        port_ranges = await c.run(
            lambda dask_worker: dask_worker._start_port, nanny=nanny
        )

        for a in port_ranges.values():
            assert isinstance(a, list)
            assert len(a) in (333, 334)
            assert all(min_ <= i <= max_ for i in a)

            # Test no overlap
            for b in port_ranges.values():
                assert a is b or not set(a) & set(b)

        ports = await c.run(lambda dask_worker: dask_worker.port, nanny=nanny)
        assert all(min_ <= p <= max_ for p in ports.values())
        for addr, range in port_ranges.items():
            assert ports[addr] in range

    with popen(
        [
            "dask-worker",
            s.address,
            "--nworkers",
            "3",
            "--host",
            "127.0.0.1",
            "--worker-port",
            "10000:11000",
            "--nanny-port",
            "11000:12000",
            "--no-dashboard",
        ]
    ):
        await c.wait_for_workers(3)
        await assert_ports(10000, 10999, nanny=False)
        await assert_ports(11000, 12000, nanny=True)


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
        assert any(
            b"Not enough ports in range" in worker.stdout.readline() for _ in range(100)
        )


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
        ]
    ) as worker:
        # roundtrip works
        assert await c.submit(lambda x: x + 1, 10) == 11

        (comm,) = s.stream_comms.values()
        comm.abort()

        # worker terminates as soon as the connection is aborted
        await to_thread(worker.wait, timeout=5)
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
        ]
    ) as worker:
        # roundtrip works
        assert await c.submit(lambda x: x + 1, 10) == 11

        (comm,) = s.stream_comms.values()
        comm.abort()

        # roundtrip still works, which means the worker reconnected
        assert await c.submit(lambda x: x + 1, 11) == 12

        # closing the scheduler cleanly does terminate the worker
        await s.close()
        await to_thread(worker.wait, timeout=5)
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
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0


@pytest.mark.slow
@pytest.mark.parametrize("no_nanny", [True, False])
def test_worker_timeout(no_nanny):
    runner = CliRunner()
    args = ["192.168.1.100:7777", "--death-timeout=1"]
    if no_nanny:
        args.append("--no-nanny")
    result = runner.invoke(main, args)
    assert result.exit_code != 0


def test_bokeh_deprecation():
    pytest.importorskip("bokeh")

    runner = CliRunner()
    with pytest.warns(UserWarning, match="dashboard"):
        try:
            runner.invoke(main, ["--bokeh"])
        except ValueError:
            # didn't pass scheduler
            pass

    with pytest.warns(UserWarning, match="dashboard"):
        try:
            runner.invoke(main, ["--no-bokeh"])
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


@pytest.mark.slow
@pytest.mark.parametrize("nanny", ["--nanny", "--no-nanny"])
def test_timeout(nanny):
    with popen(
        ["dask-worker", "192.168.1.100:7777", nanny, "--death-timeout=1"],
        flush_output=False,
    ) as worker:
        logs = [worker.stdout.readline().decode().lower() for _ in range(15)]

        assert any("timed out starting worker" in log for log in logs)
        assert any("end worker" in log for log in logs)
        assert worker.returncode != 0


@pytest.mark.slow
@pytest.mark.parametrize("nanny", ["--nanny", "--no-nanny"])
@gen_cluster(client=True, nthreads=[])
async def test_sigint(c, s, nanny):
    with popen(["dask-worker", s.address, nanny], flush_output=False) as worker:
        await c.wait_for_workers(1)
        worker.send_signal(signal.SIGINT)
        logs = [worker.stdout.readline().decode().lower() for _ in range(25)]
        assert not any("timed out" in log for log in logs)
        assert not any(f"signal {signal.SIGINT}" in log for log in logs)
        assert any("end worker" in log for log in logs)
        assert worker.returncode != 0


@pytest.mark.slow
@pytest.mark.parametrize("nanny", ["--nanny", "--no-nanny"])
@gen_cluster(client=True, nthreads=[])
async def test_sigterm(c, s, nanny):
    with popen(["dask-worker", s.address, nanny], flush_output=False) as worker:
        await c.wait_for_workers(1)
        worker.send_signal(signal.SIGTERM)
        logs = [worker.stdout.readline().decode().lower() for _ in range(25)]
        assert not any("timed out" in log for log in logs)
        assert any(f"signal {signal.SIGTERM}" in log for log in logs)
        assert any("end worker" in log for log in logs)
        assert worker.returncode != 0

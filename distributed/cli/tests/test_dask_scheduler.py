import psutil
import pytest

pytest.importorskip("requests")

import os
import shutil
import sys
import tempfile
from time import sleep

import requests
from click.testing import CliRunner

import distributed
import distributed.cli.dask_scheduler
from distributed import Client, Scheduler
from distributed.compatibility import LINUX
from distributed.metrics import time
from distributed.utils import get_ip, get_ip_interface, tmpfile
from distributed.utils_test import (
    assert_can_connect_from_everywhere_4_6,
    assert_can_connect_locally_4,
    popen,
)


def test_defaults(loop):
    with popen(["dask-scheduler"]):

        async def f():
            # Default behaviour is to listen on all addresses
            await assert_can_connect_from_everywhere_4_6(8786, timeout=5.0)

        with Client(f"127.0.0.1:{Scheduler.default_port}", loop=loop) as c:
            c.sync(f)

        response = requests.get("http://127.0.0.1:8787/status/")
        response.raise_for_status()

    with pytest.raises(Exception):
        response = requests.get("http://127.0.0.1:9786/info.json")


def test_hostport(loop):
    with popen(["dask-scheduler", "--no-dashboard", "--host", "127.0.0.1:8978"]):

        async def f():
            # The scheduler's main port can't be contacted from the outside
            await assert_can_connect_locally_4(8978, timeout=5.0)

        with Client("127.0.0.1:8978", loop=loop) as c:
            assert len(c.nthreads()) == 0
            c.sync(f)


def test_no_dashboard(loop):
    pytest.importorskip("bokeh")
    with popen(["dask-scheduler", "--no-dashboard"]) as proc:
        with Client("127.0.0.1:%d" % Scheduler.default_port, loop=loop) as c:
            response = requests.get("http://127.0.0.1:8787/status/")
            assert response.status_code == 404


def test_dashboard(loop):
    pytest.importorskip("bokeh")

    with popen(["dask-scheduler"]) as proc:
        for line in proc.stderr:
            if b"dashboard at" in line:
                dashboard_port = int(line.decode().split(":")[-1].strip())
                break
        else:
            raise Exception("dashboard not found")

        with Client(f"127.0.0.1:{Scheduler.default_port}", loop=loop):
            pass

        names = ["localhost", "127.0.0.1", get_ip()]
        start = time()
        while True:
            try:
                # All addresses should respond
                for name in names:
                    uri = f"http://{name}:{dashboard_port}/status/"
                    response = requests.get(uri)
                    response.raise_for_status()
                break
            except Exception as e:
                print(f"Got error on {uri!r}: {e.__class__.__name__}: {e}")
                elapsed = time() - start
                if elapsed > 10:
                    print(f"Timed out after {elapsed:.2f} seconds")
                    raise
                sleep(0.1)

    with pytest.raises(Exception):
        requests.get(f"http://127.0.0.1:{dashboard_port}/status/")


def test_dashboard_non_standard_ports(loop):
    pytest.importorskip("bokeh")

    with popen(
        ["dask-scheduler", "--port", "3448", "--dashboard-address", ":4832"]
    ) as proc:
        with Client("127.0.0.1:3448", loop=loop) as c:
            pass

        start = time()
        while True:
            try:
                response = requests.get("http://localhost:4832/status/")
                assert response.ok
                break
            except Exception:
                sleep(0.1)
                assert time() < start + 20
    with pytest.raises(Exception):
        requests.get("http://localhost:4832/status/")


@pytest.mark.skipif(not LINUX, reason="Need 127.0.0.2 to mean localhost")
def test_dashboard_whitelist(loop):
    pytest.importorskip("bokeh")
    with pytest.raises(Exception):
        requests.get("http://localhost:8787/status/").ok

    with popen(["dask-scheduler"]) as proc:
        with Client("127.0.0.1:%d" % Scheduler.default_port, loop=loop) as c:
            pass

        start = time()
        while True:
            try:
                for name in ["127.0.0.2", "127.0.0.3"]:
                    response = requests.get("http://%s:8787/status/" % name)
                    assert response.ok
                break
            except Exception as f:
                print(f)
                sleep(0.1)
                assert time() < start + 20


def test_interface(loop):
    if_names = sorted(psutil.net_if_addrs())
    for if_name in if_names:
        try:
            ipv4_addr = get_ip_interface(if_name)
        except ValueError:
            pass
        else:
            if ipv4_addr == "127.0.0.1":
                break
    else:
        pytest.skip(
            "Could not find loopback interface. "
            "Available interfaces are: %s." % (if_names,)
        )

    with popen(["dask-scheduler", "--no-dashboard", "--interface", if_name]) as s:
        with popen(
            ["dask-worker", "127.0.0.1:8786", "--no-dashboard", "--interface", if_name]
        ) as a:
            with Client("tcp://127.0.0.1:%d" % Scheduler.default_port, loop=loop) as c:
                start = time()
                while not len(c.nthreads()):
                    sleep(0.1)
                    assert time() - start < 30
                info = c.scheduler_info()
                assert "tcp://127.0.0.1" in info["address"]
                assert all("127.0.0.1" == d["host"] for d in info["workers"].values())


def test_pid_file(loop):
    def check_pidfile(proc, pidfile):
        start = time()
        while not os.path.exists(pidfile):
            sleep(0.01)
            assert time() < start + 30

        text = False
        start = time()
        while not text:
            sleep(0.01)
            assert time() < start + 30
            with open(pidfile) as f:
                text = f.read()
        pid = int(text)
        if sys.platform.startswith("win"):
            # On Windows, `dask-XXX` invokes the dask-XXX.exe
            # shim, but the PID is written out by the child Python process
            assert pid
        else:
            assert proc.pid == pid

    with tmpfile() as s:
        with popen(["dask-scheduler", "--pid-file", s, "--no-dashboard"]) as sched:
            check_pidfile(sched, s)

        with tmpfile() as w:
            with popen(
                ["dask-worker", "127.0.0.1:8786", "--pid-file", w, "--no-dashboard"]
            ) as worker:
                check_pidfile(worker, w)


def test_scheduler_port_zero(loop):
    with tmpfile() as fn:
        with popen(
            ["dask-scheduler", "--no-dashboard", "--scheduler-file", fn, "--port", "0"]
        ) as sched:
            with Client(scheduler_file=fn, loop=loop) as c:
                assert c.scheduler.port
                assert c.scheduler.port != 8786


def test_dashboard_port_zero(loop):
    pytest.importorskip("bokeh")
    with tmpfile() as fn:
        with popen(["dask-scheduler", "--dashboard-address", ":0"]) as proc:
            count = 0
            while count < 1:
                line = proc.stderr.readline()
                if b"dashboard" in line.lower():
                    sleep(0.01)
                    count += 1
                    assert b":0" not in line


PRELOAD_TEXT = """
_scheduler_info = {}

def dask_setup(scheduler):
    _scheduler_info['address'] = scheduler.address
    scheduler.foo = "bar"

def get_scheduler_address():
    return _scheduler_info['address']
"""


def test_preload_file(loop):
    def check_scheduler():
        import scheduler_info

        return scheduler_info.get_scheduler_address()

    tmpdir = tempfile.mkdtemp()
    try:
        path = os.path.join(tmpdir, "scheduler_info.py")
        with open(path, "w") as f:
            f.write(PRELOAD_TEXT)
        with tmpfile() as fn:
            with popen(["dask-scheduler", "--scheduler-file", fn, "--preload", path]):
                with Client(scheduler_file=fn, loop=loop) as c:
                    assert c.run_on_scheduler(check_scheduler) == c.scheduler.address
    finally:
        shutil.rmtree(tmpdir)


def test_preload_module(loop):
    def check_scheduler():
        import scheduler_info

        return scheduler_info.get_scheduler_address()

    tmpdir = tempfile.mkdtemp()
    try:
        path = os.path.join(tmpdir, "scheduler_info.py")
        with open(path, "w") as f:
            f.write(PRELOAD_TEXT)
        env = os.environ.copy()
        if "PYTHONPATH" in env:
            env["PYTHONPATH"] = tmpdir + ":" + env["PYTHONPATH"]
        else:
            env["PYTHONPATH"] = tmpdir
        with tmpfile() as fn:
            with popen(
                [
                    "dask-scheduler",
                    "--scheduler-file",
                    fn,
                    "--preload",
                    "scheduler_info",
                ],
                env=env,
            ):
                with Client(scheduler_file=fn, loop=loop) as c:
                    assert c.run_on_scheduler(check_scheduler) == c.scheduler.address
    finally:
        shutil.rmtree(tmpdir)


def test_preload_remote_module(loop, tmp_path):
    with open(tmp_path / "scheduler_info.py", "w") as f:
        f.write(PRELOAD_TEXT)

    with popen([sys.executable, "-m", "http.server", "9382"], cwd=tmp_path):
        with popen(
            [
                "dask-scheduler",
                "--scheduler-file",
                str(tmp_path / "scheduler-file.json"),
                "--preload",
                "http://localhost:9382/scheduler_info.py",
            ]
        ) as proc:
            with Client(
                scheduler_file=tmp_path / "scheduler-file.json", loop=loop
            ) as c:
                assert (
                    c.run_on_scheduler(
                        lambda dask_scheduler: getattr(dask_scheduler, "foo", None)
                    )
                    == "bar"
                )


PRELOAD_COMMAND_TEXT = """
import click
_config = {}

@click.command()
@click.option("--passthrough", type=str, default="default")
def dask_setup(scheduler, passthrough):
    _config["passthrough"] = passthrough

def get_passthrough():
    return _config["passthrough"]
"""


def test_preload_command(loop):
    def check_passthrough():
        import passthrough_info

        return passthrough_info.get_passthrough()

    tmpdir = tempfile.mkdtemp()
    try:
        path = os.path.join(tmpdir, "passthrough_info.py")
        with open(path, "w") as f:
            f.write(PRELOAD_COMMAND_TEXT)

        with tmpfile() as fn:
            print(fn)
            with popen(
                [
                    "dask-scheduler",
                    "--scheduler-file",
                    fn,
                    "--preload",
                    path,
                    "--passthrough",
                    "foobar",
                ]
            ):
                with Client(scheduler_file=fn, loop=loop) as c:
                    assert c.run_on_scheduler(check_passthrough) == "foobar"
    finally:
        shutil.rmtree(tmpdir)


def test_preload_command_default(loop):
    def check_passthrough():
        import passthrough_info

        return passthrough_info.get_passthrough()

    tmpdir = tempfile.mkdtemp()
    try:
        path = os.path.join(tmpdir, "passthrough_info.py")
        with open(path, "w") as f:
            f.write(PRELOAD_COMMAND_TEXT)

        with tmpfile() as fn2:
            print(fn2)
            with popen(
                ["dask-scheduler", "--scheduler-file", fn2, "--preload", path],
                stdout=sys.stdout,
                stderr=sys.stderr,
            ):
                with Client(scheduler_file=fn2, loop=loop) as c:
                    assert c.run_on_scheduler(check_passthrough) == "default"

    finally:
        shutil.rmtree(tmpdir)


def test_version_option():
    runner = CliRunner()
    result = runner.invoke(distributed.cli.dask_scheduler.main, ["--version"])
    assert result.exit_code == 0


@pytest.mark.slow
def test_idle_timeout(loop):
    start = time()
    runner = CliRunner()
    runner.invoke(distributed.cli.dask_scheduler.main, ["--idle-timeout", "1s"])
    stop = time()
    assert 1 < stop - start < 10


def test_multiple_workers_2(loop):
    text = """
def dask_setup(worker):
    worker.foo = 'setup'
"""
    with popen(["dask-scheduler", "--no-dashboard"]) as s:
        with popen(
            [
                "dask-worker",
                "localhost:8786",
                "--no-dashboard",
                "--preload",
                text,
                "--preload-nanny",
                text,
            ]
        ) as a:
            with Client("127.0.0.1:8786", loop=loop) as c:
                c.wait_for_workers(1)
                [foo] = c.run(lambda dask_worker: dask_worker.foo).values()
                assert foo == "setup"
                [foo] = c.run(lambda dask_worker: dask_worker.foo, nanny=True).values()
                assert foo == "setup"


def test_multiple_workers(loop):
    with popen(["dask-scheduler", "--no-dashboard"]) as s:
        with popen(["dask-worker", "localhost:8786", "--no-dashboard"]) as a:
            with popen(["dask-worker", "localhost:8786", "--no-dashboard"]) as b:
                with Client("127.0.0.1:%d" % Scheduler.default_port, loop=loop) as c:
                    start = time()
                    while len(c.nthreads()) < 2:
                        sleep(0.1)
                        assert time() < start + 10

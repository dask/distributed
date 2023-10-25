from __future__ import annotations

import asyncio
import json
import signal
import subprocess

import pytest
import yaml

from distributed import Client
from distributed.compatibility import WINDOWS
from distributed.utils import open_port
from distributed.utils_test import gen_cluster, gen_test, popen


@gen_test(timeout=120)
async def test_text():
    port = open_port()
    with popen(
        [
            "dask",
            "spec",
            "--spec",
            '{"cls": "dask.distributed.Scheduler", "opts": {"port": %d}}' % port,
        ]
    ):
        with popen(
            [
                "dask",
                "spec",
                "tcp://localhost:%d" % port,
                "--spec",
                '{"cls": "dask.distributed.Worker", "opts": {"nanny": false, "nthreads": 3, "name": "foo"}}',
            ]
        ):
            async with Client("tcp://localhost:%d" % port, asynchronous=True) as client:
                await client.wait_for_workers(1)
                info = await client.scheduler.identity()
                [w] = info["workers"].values()
                assert w["name"] == "foo"
                assert w["nthreads"] == 3


@gen_cluster(client=True, nthreads=[])
async def test_file(c, s, tmp_path):
    fn = str(tmp_path / "foo.yaml")
    with open(fn, "w") as f:
        yaml.dump(
            {
                "cls": "dask.distributed.Worker",
                "opts": {"nanny": False, "nthreads": 3, "name": "foo"},
            },
            f,
        )
    with popen(
        [
            "dask",
            "spec",
            s.address,
            "--spec-file",
            fn,
        ]
    ):
        await c.wait_for_workers(1)
        info = await c.scheduler.identity()
        [w] = info["workers"].values()
        assert w["name"] == "foo"
        assert w["nthreads"] == 3


def test_errors():
    with popen(
        [
            "dask",
            "spec",
            "--spec",
            '{"foo": "bar"}',
            "--spec-file",
            "foo.yaml",
        ],
        capture_output=True,
    ) as proc:
        line = proc.stdout.readline().decode()
        assert "exactly one" in line
        assert "--spec" in line and "--spec-file" in line

    with popen(["dask", "spec"], capture_output=True) as proc:
        line = proc.stdout.readline().decode()
        assert "exactly one" in line
        assert "--spec" in line and "--spec-file" in line


@pytest.mark.skipif(WINDOWS, reason="POSIX only")
@pytest.mark.parametrize("worker_type", ["Nanny", "Worker"])
@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM])
@gen_cluster(client=True, nthreads=[])
async def test_signal_handling_worker(c, s, worker_type, sig):
    worker = await asyncio.create_subprocess_exec(
        "dask",
        "spec",
        "--spec",
        json.dumps(
            {
                "cls": f"dask.distributed.{worker_type}",
                "opts": {"scheduler_ip": s.address, "nthreads": 3},
            },
        ),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        await c.wait_for_workers(1)

        worker.send_signal(sig)
        stdout, stderr = await worker.communicate()
        assert stdout == b""
        logs = stderr.decode().lower()
        assert sig.name.lower() in logs
        assert worker.returncode == 0
        if worker_type == "Nanny":
            assert "closing nanny" in logs
            assert "stopping worker" in logs
        else:
            assert "nanny" not in logs
        assert "timed out" not in logs
        assert "error" not in logs
        assert "exception" not in logs
    finally:
        await worker.wait()


@pytest.mark.skipif(WINDOWS, reason="POSIX only")
@pytest.mark.parametrize("sig", [signal.SIGINT, signal.SIGTERM])
@gen_test()
async def test_signal_handling_scheduler(sig):
    port = open_port()
    scheduler = await asyncio.create_subprocess_exec(
        "dask",
        "spec",
        "--spec",
        json.dumps({"cls": "dask.distributed.Scheduler", "opts": {"port": port}}),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
    )
    try:
        async with Client(f"tcp://localhost:{port}", asynchronous=True) as client:
            pass

        scheduler.send_signal(sig)
        stdout, stderr = await scheduler.communicate()
        assert stdout == b""
        logs = stderr.decode().lower()
        assert sig.name.lower() in logs
        assert scheduler.returncode == 0
        assert "scheduler closing all comms" in logs
        assert "timed out" not in logs
        assert "error" not in logs
        assert "exception" not in logs
    finally:
        await scheduler.wait()

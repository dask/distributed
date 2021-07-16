import sys

import pytest
import yaml

from distributed import Client
from distributed.scheduler import COMPILED
from distributed.utils_test import gen_test, popen


@pytest.mark.skipif(COMPILED, reason="Fails with cythonized scheduler")
@gen_test(timeout=120)
async def test_text():
    with popen(
        [
            sys.executable,
            "-m",
            "distributed.cli.dask_spec",
            "--spec",
            '{"cls": "dask.distributed.Scheduler", "opts": {"port": 9373}}',
        ]
    ):
        with popen(
            [
                sys.executable,
                "-m",
                "distributed.cli.dask_spec",
                "tcp://localhost:9373",
                "--spec",
                '{"cls": "dask.distributed.Worker", "opts": {"nanny": false, "nthreads": 3, "name": "foo"}}',
            ]
        ):
            async with Client("tcp://localhost:9373", asynchronous=True) as client:
                await client.wait_for_workers(1)
                info = await client.scheduler.identity()
                [w] = info["workers"].values()
                assert w["name"] == "foo"
                assert w["nthreads"] == 3


@pytest.mark.skipif(COMPILED, reason="Fails with cythonized scheduler")
@pytest.mark.asyncio
async def test_file(cleanup, tmp_path):
    fn = str(tmp_path / "foo.yaml")
    with open(fn, "w") as f:
        yaml.dump(
            {
                "cls": "dask.distributed.Worker",
                "opts": {"nanny": False, "nthreads": 3, "name": "foo"},
            },
            f,
        )

    with popen(["dask-scheduler", "--port", "9373", "--no-dashboard"]):
        with popen(
            [
                sys.executable,
                "-m",
                "distributed.cli.dask_spec",
                "tcp://localhost:9373",
                "--spec-file",
                fn,
            ]
        ):
            async with Client("tcp://localhost:9373", asynchronous=True) as client:
                await client.wait_for_workers(1)
                info = await client.scheduler.identity()
                [w] = info["workers"].values()
                assert w["name"] == "foo"
                assert w["nthreads"] == 3


def test_errors():
    with popen(
        [
            sys.executable,
            "-m",
            "distributed.cli.dask_spec",
            "--spec",
            '{"foo": "bar"}',
            "--spec-file",
            "foo.yaml",
        ]
    ) as proc:
        line = proc.stdout.readline().decode()
        assert "exactly one" in line
        assert "--spec" in line and "--spec-file" in line

    with popen([sys.executable, "-m", "distributed.cli.dask_spec"]) as proc:
        line = proc.stdout.readline().decode()
        assert "exactly one" in line
        assert "--spec" in line and "--spec-file" in line

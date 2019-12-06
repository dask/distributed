import pytest
import yaml

from distributed import Client
from distributed.utils_test import popen
from distributed.utils_test import cleanup  # noqa: F401


@pytest.mark.asyncio
async def test_text(cleanup):
    with popen(
        [
            "dask-spec",
            "--spec",
            '{"cls": "dask.distributed.Scheduler", "opts": {"port": 9373}}',
        ]
    ) as sched:
        with popen(
            [
                "dask-spec",
                "tcp://localhost:9373",
                "--spec",
                '{"cls": "dask.distributed.Worker", "opts": {"nanny": false, "nthreads": 3, "name": "foo"}}',
            ]
        ) as w:
            async with Client("tcp://localhost:9373", asynchronous=True) as client:
                await client.wait_for_workers(1)
                info = await client.scheduler.identity()
                [w] = info["workers"].values()
                assert w["name"] == "foo"
                assert w["nthreads"] == 3


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

    with popen(["dask-scheduler", "--port", "9373", "--no-dashboard"]) as sched:
        with popen(["dask-spec", "tcp://localhost:9373", "--spec-file", fn]) as w:
            async with Client("tcp://localhost:9373", asynchronous=True) as client:
                await client.wait_for_workers(1)
                info = await client.scheduler.identity()
                [w] = info["workers"].values()
                assert w["name"] == "foo"
                assert w["nthreads"] == 3

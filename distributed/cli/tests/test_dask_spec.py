from __future__ import annotations

import yaml

from distributed import Client
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

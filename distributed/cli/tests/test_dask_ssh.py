from click.testing import CliRunner

from distributed.cli.dask_ssh import main
from distributed.utils_test import popen


def test_version_option():
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0


def test_nprocs_renamed_to_num_workers():
    with popen(["dask-ssh", "--nprocs=2", "127.0.0.1"]) as cluster:
        assert any(
            b"renamed to --num-workers" in cluster.stderr.readline() for i in range(15)
        )


def test_num_workers_with_nprocs_is_an_error():
    with popen(["dask-ssh", "localhost", "--nprocs=2", "--num-workers=2"]) as c:
        assert any(
            b"Both --nprocs and --num-workers" in c.stderr.readline() for i in range(15)
        )

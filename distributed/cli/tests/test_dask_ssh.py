import pytest
from click.testing import CliRunner

from distributed import Client
from distributed.cli.dask_ssh import main
from distributed.compatibility import MACOS, WINDOWS
from distributed.utils_test import popen

pytestmark = [
    pytest.mark.xfail(MACOS, reason="very high flakiness; see distributed/issues/4543"),
    pytest.mark.skipif(WINDOWS, reason="no CI support; see distributed/issues/4509"),
]


def test_version_option():
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0


def test_ssh_cli_nprocs_renamed_to_nworkers(loop):
    n_workers = 2
    with popen(
        ["dask-ssh", f"--nprocs={n_workers}", "--nohost", "localhost"]
    ) as cluster:
        with Client("tcp://127.0.0.1:8786", timeout="15 seconds", loop=loop) as c:
            c.wait_for_workers(n_workers, timeout="15 seconds")
        # This interrupt is necessary for the cluster to place output into the stdout
        # and stderr pipes
        cluster.send_signal(2)
        _, stderr = cluster.communicate()

    assert any(b"renamed to --nworkers" in l for l in stderr.splitlines())


def test_ssh_cli_nworkers_with_nprocs_is_an_error():
    with popen(["dask-ssh", "localhost", "--nprocs=2", "--nworkers=2"]) as c:
        assert any(
            b"Both --nprocs and --nworkers" in c.stderr.readline() for i in range(15)
        )

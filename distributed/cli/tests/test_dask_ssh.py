import pytest
from click.testing import CliRunner

from distributed import Client
from distributed.cli.dask_ssh import main
from distributed.compatibility import MACOS, WINDOWS
from distributed.utils_test import popen

pytest.importorskip("paramiko")
pytestmark = [
    pytest.mark.xfail(MACOS, reason="very high flakiness; see distributed/issues/4543"),
    pytest.mark.skipif(WINDOWS, reason="no CI support; see distributed/issues/4509"),
]


def test_version_option():
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0


@pytest.mark.slow
def test_ssh_cli_nprocs_renamed_to_nworkers(loop):
    with popen(
        ["dask-ssh", "--nprocs=2", "--nohost", "localhost"],
        flush_output=False,
    ) as proc:
        with Client("tcp://127.0.0.1:8786", timeout="15 seconds", loop=loop) as c:
            c.wait_for_workers(2, timeout="15 seconds")
        # This interrupt is necessary for the cluster to place output into the stdout
        # and stderr pipes
        proc.send_signal(2)
        assert any(
            b"renamed to --nworkers" in proc.stdout.readline() for _ in range(15)
        )


def test_ssh_cli_nworkers_with_nprocs_is_an_error():
    with popen(
        ["dask-ssh", "localhost", "--nprocs=2", "--nworkers=2"],
        flush_output=False,
    ) as proc:
        assert any(
            b"Both --nprocs and --nworkers" in proc.stdout.readline() for _ in range(15)
        )

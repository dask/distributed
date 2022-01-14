import time

from click.testing import CliRunner

from distributed.cli.dask_ssh import main
from distributed.utils_test import popen


def test_version_option():
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0


def test_ssh_cli_nprocs_renamed_to_num_workers():
    num_workers = 2
    with popen(
        ["dask-ssh", f"--nprocs={num_workers}", "--nohost", "localhost"]
    ) as cluster:
        # Sleeping seems to be necessary to allow the SSHCluster to start up.
        # 15 seconds is arbitrary but seems long enough. Startup to shutdown
        # takes about 8 seconds in my testing.
        time.sleep(15)
        # Signal 2 is SIGINT, KeyboardInterrupt. Output is only put onto the
        # stderr and stdout pipes when the cluster is interrupted.
        cluster.send_signal(2)
        # Retrieve the standard and error output from the cluster
        stdout, stderr = cluster.communicate()
        assert any(b"renamed to --num-workers" in line for line in stderr.splitlines())
        assert any(
            f"--num-workers {num_workers}".encode() in line
            for line in stdout.splitlines()
        )


def test_ssh_cli_num_workers_with_nprocs_is_an_error():
    with popen(["dask-ssh", "localhost", "--nprocs=2", "--num-workers=2"]) as c:
        assert any(
            b"Both --nprocs and --num-workers" in c.stderr.readline() for i in range(15)
        )

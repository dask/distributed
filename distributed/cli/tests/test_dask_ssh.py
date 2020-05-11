from subprocess import Popen


def test_version_option():
    with Popen(["dask-ssh", "--version"]) as cmd:
        cmd.communicate()
        assert cmd.returncode == 0

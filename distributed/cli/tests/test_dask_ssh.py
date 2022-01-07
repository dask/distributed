import pytest
from click.testing import CliRunner

from distributed.cli.dask_ssh import main


def test_version_option():
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0


# Turn warnings into errors for this test so that the cluster
# isn't started.
@pytest.mark.filterwarnings("error::FutureWarning")
def test_nprocs_deprecation():
    runner = CliRunner()
    with pytest.raises(FutureWarning, match="renamed to --num-workers"):
        runner.invoke(main, ["localhost", "--nprocs=2"], catch_exceptions=False)

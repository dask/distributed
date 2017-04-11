from click.testing import CliRunner
from distributed.cli.dask_remote import main
import re


def test_dask_remote():
    runner = CliRunner()
    result = runner.invoke(main, ['--help'])
    # The click cli will format the cli arguments to cause descriptions to line up.
    # Consequently we need to remove the double spacing.
    normalized_output, _ = re.subn('  +', '__', result.output)
    assert '--host TEXT__IP or hostname of this server' in normalized_output
    assert result.exit_code == 0

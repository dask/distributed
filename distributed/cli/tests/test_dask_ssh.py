from __future__ import annotations

import pytest
from click.testing import CliRunner

from distributed.cli.dask_ssh import main
from distributed.compatibility import MACOS, WINDOWS

pytest.importorskip("paramiko")
pytestmark = [
    pytest.mark.xfail(MACOS, reason="very high flakiness; see distributed/issues/4543"),
    pytest.mark.skipif(WINDOWS, reason="no CI support; see distributed/issues/4509"),
]


def test_version_option():
    runner = CliRunner()
    result = runner.invoke(main, ["--version"])
    assert result.exit_code == 0

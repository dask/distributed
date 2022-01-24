from __future__ import annotations

import subprocess
import sys

import_check_code = """
import sys

current_pandas_modules = [m for m in sys.modules if m.startswith("pandas")]
assert (
    not current_pandas_modules
), "pandas is already imported at startup: " + "\\n".join(current_pandas_modules)

# Make pandas un-importable
sys.modules["pandas"] = None
# "if the value is None, then a ModuleNotFoundError is raised"
# https://docs.python.org/3.6/reference/import.html#the-module-cache

import distributed.shuffle  # noqa: F401
"""


def test_import_no_pandas():
    p = subprocess.run(
        [sys.executable],
        input=import_check_code,
        text=True,
        timeout=10,
    )
    assert (
        p.returncode == 0
    ), "Importing the shuffle extension without pandas failed. See logs for details."

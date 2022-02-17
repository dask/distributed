from __future__ import annotations

import logging
import platform
import sys

logging_names: dict[str | int, int | str] = {}
logging_names.update(logging._levelToName)  # type: ignore
logging_names.update(logging._nameToLevel)  # type: ignore

PYPY = platform.python_implementation().lower() == "pypy"
LINUX = sys.platform == "linux"
MACOS = sys.platform == "darwin"
WINDOWS = sys.platform.startswith("win")

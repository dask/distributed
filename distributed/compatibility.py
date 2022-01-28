from __future__ import annotations

import logging
import platform
import sys
from distutils.version import LooseVersion

import tornado

logging_names: dict[str | int, int | str] = {}
logging_names.update(logging._levelToName)  # type: ignore
logging_names.update(logging._nameToLevel)  # type: ignore

PY_VERSION = LooseVersion(".".join(map(str, sys.version_info[:3])))
PYPY = platform.python_implementation().lower() == "pypy"
LINUX = sys.platform == "linux"
MACOS = sys.platform == "darwin"
WINDOWS = sys.platform.startswith("win")
TORNADO6 = tornado.version_info[0] >= 6

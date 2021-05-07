import logging
import platform
import sys
from distutils.version import LooseVersion

import tornado

logging_names = logging._levelToName.copy()
logging_names.update(logging._nameToLevel)

PY_VERSION = LooseVersion(".".join(map(str, sys.version_info[:3])))
PYPY = platform.python_implementation().lower() == "pypy"
MACOS = sys.platform == "darwin"
WINDOWS = sys.platform.startswith("win")
TORNADO6 = tornado.version_info[0] >= 6

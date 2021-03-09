import logging
import platform
import sys

import tornado

logging_names = logging._levelToName.copy()
logging_names.update(logging._nameToLevel)

PYPY = platform.python_implementation().lower() == "pypy"
MACOS = sys.platform == "darwin"
WINDOWS = sys.platform.startswith("win")
TORNADO6 = tornado.version_info[0] >= 6

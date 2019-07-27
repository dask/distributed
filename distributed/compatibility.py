from __future__ import print_function, division, absolute_import

import logging
import sys

# flake8: noqa


def isqueue(o):
    return isinstance(o, Queue)


logging_names = logging._levelToName.copy()
logging_names.update(logging._nameToLevel)

import platform

PYPY = platform.python_implementation().lower() == "pypy"
WINDOWS = sys.platform.startswith("win")

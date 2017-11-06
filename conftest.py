# https://pytest.org/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option
import os
import pytest


# Enable more logging and checks (https://docs.python.org/3/library/asyncio-dev.html)
os.environ["PYTHONASYNCIODEBUG"] = "1"

try:
    import faulthandler
except ImportError:
    pass
else:
    faulthandler.enable()


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", help="run slow tests")

pytest_plugins = ['distributed.pytest_resourceleaks']

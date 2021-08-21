# https://pytest.org/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option
import copy
import logging

import pytest

import dask

# Uncomment to enable more logging and checks
# (https://docs.python.org/3/library/asyncio-dev.html)
# Note this makes things slower and might consume much memory.
# os.environ["PYTHONASYNCIODEBUG"] = "1"

try:
    import faulthandler
except ImportError:
    pass
else:
    try:
        faulthandler.enable()
    except Exception:
        pass

# Make all fixtures available
from distributed.utils_test import *  # noqa


def pytest_addoption(parser):
    parser.addoption("--runslow", action="store_true", help="run slow tests")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--runslow"):
        # --runslow given in cli: do not skip slow tests
        return
    skip_slow = pytest.mark.skip(reason="need --runslow option to run")
    for item in items:
        if "slow" in item.keywords:
            item.add_marker(skip_slow)


pytest_plugins = ["distributed.pytest_resourceleaks"]


_original_config = copy.deepcopy(dask.config.config)
# Custom preloads can interact with the test suite in unexpected ways
# so we remove them when running tests
for node in ["scheduler", "worker", "nanny"]:
    _original_config["distributed"][node]["preload"] = []
    _original_config["distributed"][node]["preload-argv"] = []

_logging_levels = {
    name: logger.level
    for name, logger in logging.root.manager.loggerDict.items()
    if isinstance(logger, logging.Logger)
}


@pytest.fixture(autouse=True)
def clean_config():

    # Restore default logging levels
    for name, level in _logging_levels.items():
        logging.getLogger(name).setLevel(level)

    # Ensure a clean config
    with dask.config.set(copy.deepcopy(_original_config)):
        yield

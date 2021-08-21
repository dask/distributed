# https://pytest.org/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option
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


@pytest.fixture(scope="session", autouse=True)
def clean_preloads():
    # Custom preloads can interact with the test suite in unexpected ways.
    # Temporarily remove any configured preloads while tests are being run.
    original = {}
    nodes = ["scheduler", "worker", "nanny"]
    for node in nodes:
        preload = f"distributed.{node}.preload"
        preload_argv = f"distributed.{node}.preload-argv"
        original[preload] = dask.config.get(preload)
        original[preload_argv] = dask.config.get(preload_argv)
        dask.config.set({preload: []})
        dask.config.set({preload_argv: []})

    yield

    for node in nodes:
        dask.config.set({preload: original[f"distributed.{node}.preload"]})
        dask.config.set({preload_argv: original[f"distributed.{node}.preload-argv"]})

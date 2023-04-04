# https://pytest.org/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option
from __future__ import annotations

from unittest import mock

import pytest

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

        if "ws" in item.fixturenames:
            item.add_marker(pytest.mark.workerstate)


@pytest.fixture(autouse=True)
def patch_gilknocker(request):
    """
    A very subtle bug where a SIGABRT can kill the main Python thread out
    from underneath gilknocker's GIL sampling thread which can lead to:

        FATAL: exception not rethrown
        Fatal Python error: Aborted

        Current thread 0x00007fbae56bc640 (most recent call first):
        <no Python frame>

    Due to a test/object/ect not properly calling 'gilknocker.KnockKnock.stop'.
    This bug rarely shows up as is, even when trying to bring it up deliberatly.
    More details can be found here: https://github.com/PyO3/pyo3/issues/2102

    Therefore, we patch all tests so `start` doesn't trigger the sampling thread,
    except when explicitly tested in 'test_gil_contention' test.
    """
    if "gil" in request.node.name:
        yield
    else:
        try:
            import gilknocker  # noqa F401
        except ImportError:
            yield
        else:
            with mock.patch("gilknocker.KnockKnock.start"):
                yield


pytest_plugins = ["distributed.pytest_resourceleaks"]

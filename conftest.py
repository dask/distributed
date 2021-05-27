# https://pytest.org/latest/example/simple.html#control-skipping-of-tests-according-to-command-line-option
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

# The following fixtures are required for pytest-asyncio compatibility


@pytest.fixture()
def event_loop():
    from distributed.utils_test import clean

    with clean() as loop:
        yield loop.asyncio_loop


@pytest.hookimpl(trylast=True)
def pytest_fixture_post_finalizer(fixturedef, request):
    """Pytest-asyncio resets the eventloop policy after the above event loop is closed."""
    import asyncio

    if fixturedef.argname == "event_loop":
        try:
            # Tornado > 6.0.3
            from tornado.platform.asyncio import AnyThreadEventLoopPolicy
        except ImportError:
            try:
                from distributed.utils import AnyThreadEventLoopPolicy
            except ImportError:
                return
        asyncio.set_event_loop_policy(AnyThreadEventLoopPolicy())

from __future__ import annotations

import logging

import pytest

from distributed import Nanny, NannyPlugin
from distributed.protocol.pickle import dumps
from distributed.utils_test import captured_logger, gen_cluster


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_register_worker_plugin_is_deprecated(c, s, a):
    class DuckPlugin(NannyPlugin):
        def setup(self, nanny):
            nanny.foo = 123

        def teardown(self, nanny):
            pass

    n_existing_plugins = len(a.plugins)
    assert not hasattr(a, "foo")
    with pytest.warns(DeprecationWarning, match="register_worker_plugin.*deprecated"):
        await c.register_worker_plugin(DuckPlugin())
    assert len(a.plugins) == n_existing_plugins + 1
    assert a.foo == 123


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_register_worker_plugin_typing_over_nanny_keyword(c, s, a):
    class DuckPlugin(NannyPlugin):
        def setup(self, nanny):
            nanny.foo = 123

        def teardown(self, nanny):
            pass

    n_existing_plugins = len(a.plugins)
    assert not hasattr(a, "foo")
    with (
        pytest.warns(UserWarning, match="`NannyPlugin` as a worker plugin"),
        pytest.warns(DeprecationWarning, match="please use `Client.register_plugin`"),
    ):
        await c.register_worker_plugin(DuckPlugin(), nanny=False)

    assert len(a.plugins) == n_existing_plugins + 1
    assert a.foo == 123


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_duck_typed_register_nanny_plugin_is_deprecated(c, s, a):
    class DuckPlugin:
        def setup(self, nanny):
            nanny.foo = 123

        def teardown(self, nanny):
            pass

    n_existing_plugins = len(a.plugins)
    assert not hasattr(a, "foo")
    with (
        pytest.warns(DeprecationWarning, match="duck-typed.*NannyPlugin"),
        pytest.warns(DeprecationWarning, match="please use `Client.register_plugin`"),
    ):
        await c.register_worker_plugin(DuckPlugin(), nanny=True)
    assert len(a.plugins) == n_existing_plugins + 1
    assert a.foo == 123


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_register_idempotent_plugin(c, s, a):
    class IdempotentPlugin(NannyPlugin):
        def __init__(self, instance=None):
            self.name = "idempotentplugin"
            self.instance = instance
            self.idempotent = True

        def setup(self, nanny):
            if self.instance != "first":
                raise RuntimeError(
                    "Only the first plugin should be started when idempotent is set"
                )

    first = IdempotentPlugin(instance="first")
    await c.register_plugin(first)
    assert "idempotentplugin" in a.plugins

    second = IdempotentPlugin(instance="second")
    await c.register_plugin(second)
    assert "idempotentplugin" in a.plugins
    assert a.plugins["idempotentplugin"].instance == "first"


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_register_non_idempotent_plugin(c, s, a):
    class NonIdempotentPlugin(NannyPlugin):
        def __init__(self, instance=None):
            self.name = "nonidempotentplugin"
            self.instance = instance

    first = NonIdempotentPlugin(instance="first")
    await c.register_plugin(first)
    assert "nonidempotentplugin" in a.plugins

    second = NonIdempotentPlugin(instance="second")
    await c.register_plugin(second)
    assert "nonidempotentplugin" in a.plugins
    assert a.plugins["nonidempotentplugin"].instance == "second"

    third = NonIdempotentPlugin(instance="third")
    with pytest.warns(
        FutureWarning,
        match="`Scheduler.register_nanny_plugin` now requires `idempotent`",
    ):
        await s.register_nanny_plugin(
            comm=None, plugin=dumps(third), name="nonidempotentplugin"
        )
    assert "nonidempotentplugin" in a.plugins
    assert a.plugins["nonidempotentplugin"].instance == "third"


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_register_plugin_with_idempotent_keyword_is_deprecated(c, s, a):
    class NonIdempotentPlugin(NannyPlugin):
        def __init__(self, instance=None):
            self.name = "nonidempotentplugin"
            self.instance = instance
            # We want to overrule this
            self.idempotent = True

    first = NonIdempotentPlugin(instance="first")
    with pytest.warns(FutureWarning, match="`idempotent` argument is deprecated"):
        await c.register_plugin(first, idempotent=False)
    assert "nonidempotentplugin" in a.plugins

    second = NonIdempotentPlugin(instance="second")
    with pytest.warns(FutureWarning, match="`idempotent` argument is deprecated"):
        await c.register_plugin(second, idempotent=False)
    assert "nonidempotentplugin" in a.plugins
    assert a.plugins["nonidempotentplugin"].instance == "second"

    class IdempotentPlugin(NannyPlugin):
        def __init__(self, instance=None):
            self.name = "idempotentplugin"
            self.instance = instance
            # We want to overrule this
            self.idempotent = False

        def setup(self, nanny):
            if self.instance != "first":
                raise RuntimeError(
                    "Only the first plugin should be started when idempotent is set"
                )

    first = IdempotentPlugin(instance="first")
    with pytest.warns(FutureWarning, match="`idempotent` argument is deprecated"):
        await c.register_plugin(first, idempotent=True)
    assert "idempotentplugin" in a.plugins

    second = IdempotentPlugin(instance="second")
    with pytest.warns(FutureWarning, match="`idempotent` argument is deprecated"):
        await c.register_plugin(second, idempotent=True)
    assert "idempotentplugin" in a.plugins
    assert a.plugins["idempotentplugin"].instance == "first"


class BrokenSetupPlugin(NannyPlugin):
    def setup(self, nanny):
        raise RuntimeError("test error")


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_register_plugin_with_broken_setup_to_existing_nanny_raises(c, s, a):
    with pytest.raises(RuntimeError, match="test error"):
        with captured_logger("distributed.nanny", level=logging.ERROR) as caplog:
            await c.register_plugin(BrokenSetupPlugin(), name="TestPlugin1")
    logs = caplog.getvalue()
    assert "TestPlugin1 failed to setup" in logs
    assert "test error" in logs


@gen_cluster(client=True, nthreads=[])
async def test_plugin_with_broken_setup_on_new_nanny_logs(c, s):
    await c.register_plugin(BrokenSetupPlugin(), name="TestPlugin1")

    with captured_logger("distributed.nanny", level=logging.ERROR) as caplog:
        async with Nanny(s.address):
            pass
    logs = caplog.getvalue()
    assert "TestPlugin1 failed to setup" in logs
    assert "test error" in logs

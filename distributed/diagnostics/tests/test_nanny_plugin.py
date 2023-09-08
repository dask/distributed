from __future__ import annotations

import pytest

from distributed import Nanny, NannyPlugin
from distributed.utils_test import gen_cluster


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
    with pytest.warns(UserWarning, match="`NannyPlugin` as a worker plugin"):
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
    with pytest.warns(DeprecationWarning, match="duck-typed.*NannyPlugin"):
        await c.register_worker_plugin(DuckPlugin(), nanny=True)
    assert len(a.plugins) == n_existing_plugins + 1
    assert a.foo == 123


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_register_idempotent_plugins(c, s, a):
    class IdempotentPlugin(NannyPlugin):
        def __init__(self, instance=None):
            self.name = "idempotentplugin"
            self.instance = instance

        def setup(self, nanny):
            if self.instance != "first":
                raise RuntimeError(
                    "Only the first plugin should be started when idempotent is set"
                )

    first = IdempotentPlugin(instance="first")
    await c.register_plugin(first, idempotent=True)
    assert "idempotentplugin" in a.plugins

    second = IdempotentPlugin(instance="second")
    await c.register_plugin(second, idempotent=True)
    assert "idempotentplugin" in a.plugins
    assert a.plugins["idempotentplugin"].instance == "first"


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_register_non_idempotent_plugins(c, s, a):
    class NonIdempotentPlugin(NannyPlugin):
        def __init__(self, instance=None):
            self.name = "nonidempotentplugin"
            self.instance = instance

    first = NonIdempotentPlugin(instance="first")
    await c.register_plugin(first, idempotent=False)
    assert "nonidempotentplugin" in a.plugins

    second = NonIdempotentPlugin(instance="second")
    await c.register_plugin(second, idempotent=False)
    assert "nonidempotentplugin" in a.plugins
    assert a.plugins["nonidempotentplugin"].instance == "second"

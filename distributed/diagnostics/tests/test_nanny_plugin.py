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
    with pytest.warns(UserWarning, match="nanny plugin as a worker plugin"):
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

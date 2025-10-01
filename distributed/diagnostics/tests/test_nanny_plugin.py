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
async def test_register_plugin_with_broken_setup_to_existing_nannies_raises(c, s, a):
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


class BrokenTeardownPlugin(NannyPlugin):
    def teardown(self, nanny):
        raise RuntimeError("test error")


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_unregister_nanny_plugin_with_broken_teardown_raises(c, s, a):
    await c.register_plugin(BrokenTeardownPlugin(), name="TestPlugin1")
    with pytest.raises(RuntimeError, match="test error"):
        with captured_logger("distributed.nanny", level=logging.ERROR) as caplog:
            await c.unregister_worker_plugin("TestPlugin1", nanny=True)
    logs = caplog.getvalue()
    assert "TestPlugin1 failed to teardown" in logs
    assert "test error" in logs


@gen_cluster(client=True, nthreads=[])
async def test_nanny_plugin_with_broken_teardown_logs_on_close(c, s):
    await c.register_plugin(BrokenTeardownPlugin(), name="TestPlugin1")

    with captured_logger("distributed.nanny", level=logging.ERROR) as caplog:
        async with Nanny(s.address):
            pass
    logs = caplog.getvalue()
    assert "TestPlugin1 failed to teardown" in logs
    assert "test error" in logs

@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_has_nanny_plugin_by_name(c, s, a):
    """Test checking if nanny plugin is registered using string name"""
    
    class DuckPlugin(NannyPlugin):
        name = "duck-plugin"
        
        def setup(self, nanny):
            nanny.foo = 123
        
        def teardown(self, nanny):
            pass
    
    # Check non-existent plugin
    assert not await c.has_plugin("duck-plugin")
    
    # Register plugin
    await c.register_plugin(DuckPlugin())
    assert a.foo == 123
    
    # Check using string name
    assert await c.has_plugin("duck-plugin")
    
    # Unregister and check again
    await c.unregister_worker_plugin("duck-plugin", nanny=True)
    assert not await c.has_plugin("duck-plugin")


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_has_nanny_plugin_by_object(c, s, a):
    """Test checking if nanny plugin is registered using plugin object"""
    
    class DuckPlugin(NannyPlugin):
        name = "duck-plugin"
        
        def setup(self, nanny):
            nanny.bar = 456
        
        def teardown(self, nanny):
            pass
    
    plugin = DuckPlugin()
    
    # Check before registration
    assert not await c.has_plugin(plugin)
    
    # Register and check
    await c.register_plugin(plugin)
    assert a.bar == 456
    assert await c.has_plugin(plugin)
    
    # Unregister and check
    await c.unregister_worker_plugin("duck-plugin", nanny=True)
    assert not await c.has_plugin(plugin)


@gen_cluster(client=True, nthreads=[("", 1), ("", 1)], Worker=Nanny)
async def test_has_nanny_plugin_multiple_nannies(c, s, a, b):
    """Test checking nanny plugin with multiple nannies"""
    
    class DuckPlugin(NannyPlugin):
        name = "duck-plugin"
        
        def setup(self, nanny):
            nanny.multi = "setup"
        
        def teardown(self, nanny):
            pass
    
    # Check before registration
    assert not await c.has_plugin("duck-plugin")
    
    # Register plugin (should propagate to all nannies)
    await c.register_plugin(DuckPlugin())
    
    # Verify both nannies have the plugin
    assert a.multi == "setup"
    assert b.multi == "setup"
    
    # Check plugin is registered
    assert await c.has_plugin("duck-plugin")

@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_has_nanny_plugin_custom_name_override(c, s, a):
    """Test nanny plugin registered with custom name different from class name"""
    
    class DuckPlugin(NannyPlugin):
        name = "duck-plugin"
        
        def setup(self, nanny):
            nanny.custom = "test"
        
        def teardown(self, nanny):
            pass
    
    plugin = DuckPlugin()
    
    # Register with custom name (overriding the class name attribute)
    await c.register_plugin(plugin, name="custom-override")
    
    # Check with custom name works
    assert await c.has_plugin("custom-override")
    
    # Original name won't work since we overrode it
    assert not await c.has_plugin("duck-plugin")


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_has_nanny_plugin_list_check(c, s, a):
    """Test checking multiple nanny plugins at once"""
    
    class IdempotentPlugin(NannyPlugin):
        name = "idempotentplugin"
        
        def setup(self, nanny):
            pass
        
        def teardown(self, nanny):
            pass
    
    class NonIdempotentPlugin(NannyPlugin):
        name = "nonidempotentplugin"
        
        def setup(self, nanny):
            pass
        
        def teardown(self, nanny):
            pass
    
    # Check multiple before registration
    result = await c.has_plugin(["idempotentplugin", "nonidempotentplugin", "nonexistent"])
    assert result == {
        "idempotentplugin": False,
        "nonidempotentplugin": False,
        "nonexistent": False,
    }
    
    # Register first plugin
    await c.register_plugin(IdempotentPlugin())
    result = await c.has_plugin(["idempotentplugin", "nonidempotentplugin"])
    assert result == {"idempotentplugin": True, "nonidempotentplugin": False}
    
    # Register second plugin
    await c.register_plugin(NonIdempotentPlugin())
    result = await c.has_plugin(["idempotentplugin", "nonidempotentplugin"])
    assert result == {"idempotentplugin": True, "nonidempotentplugin": True}

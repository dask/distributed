from __future__ import annotations

import asyncio

import pytest

from distributed import Nanny, Scheduler, SchedulerPlugin, Worker, get_worker
from distributed.protocol.pickle import dumps
from distributed.utils_test import captured_logger, gen_cluster, gen_test, inc


@gen_cluster(client=True)
async def test_simple(c, s, a, b):
    class Counter(SchedulerPlugin):
        def start(self, scheduler):
            self.scheduler = scheduler
            scheduler.add_plugin(self, name="counter")
            self.count = 0

        def transition(self, key, start, finish, *args, stimulus_id, **kwargs):
            assert stimulus_id is not None
            if start == "processing" and finish == "memory":
                self.count += 1

    counter = Counter()
    counter.start(s)
    assert counter in s.plugins.values()

    assert counter.count == 0

    x = c.submit(inc, 1)
    y = c.submit(inc, x)
    z = c.submit(inc, y)

    await z

    assert counter.count == 3
    s.remove_plugin("counter")
    assert counter not in s.plugins

    with pytest.raises(ValueError, match="Could not find plugin 'counter'") as e:
        s.remove_plugin("counter")


@gen_cluster(nthreads=[])
async def test_add_remove_worker(s):
    events = []

    class MyPlugin(SchedulerPlugin):
        name = "MyPlugin"

        def add_worker(self, worker, scheduler):
            assert scheduler is s
            events.append(("add_worker", worker))

        def remove_worker(self, worker, scheduler, *, stimulus_id, **kwargs):
            assert scheduler is s
            assert stimulus_id is not None
            events.append(("remove_worker", worker))

    plugin = MyPlugin()
    s.add_plugin(plugin)
    assert events == []

    a = Worker(s.address)
    b = Worker(s.address)
    await a
    await b
    await a.close()
    await b.close()

    assert events == [
        ("add_worker", a.address),
        ("add_worker", b.address),
        ("remove_worker", a.address),
        ("remove_worker", b.address),
    ]

    events[:] = []
    s.remove_plugin(plugin.name)
    async with Worker(s.address):
        pass
    assert events == []


@gen_cluster(nthreads=[])
async def test_remove_worker_renamed_kwargs_allowed(s):
    events = []

    class MyPlugin(SchedulerPlugin):
        name = "MyPlugin"

        def remove_worker(self, worker, scheduler, **kwds):
            assert scheduler is s
            events.append(("remove_worker", worker))

    plugin = MyPlugin()
    s.add_plugin(plugin)
    assert events == []

    a = Worker(s.address)
    await a
    await a.close()

    assert events == [
        ("remove_worker", a.address),
    ]

    events[:] = []
    s.remove_plugin(plugin.name)
    async with Worker(s.address):
        pass
    assert events == []


@gen_cluster(nthreads=[])
async def test_remove_worker_without_kwargs_deprecated(s):
    events = []

    class DeprecatedPlugin(SchedulerPlugin):
        name = "DeprecatedPlugin"

        def remove_worker(self, worker, scheduler):
            assert scheduler is s
            events.append(("remove_worker", worker))

    plugin = DeprecatedPlugin()
    with pytest.warns(
        FutureWarning,
        match="The signature of `SchedulerPlugin.remove_worker` now requires `\\*\\*kwargs`",
    ):
        s.add_plugin(plugin)
    assert events == []

    a = Worker(s.address)
    await a
    await a.close()

    assert events == [
        ("remove_worker", a.address),
    ]

    events[:] = []
    s.remove_plugin(plugin.name)
    async with Worker(s.address):
        pass
    assert events == []


@gen_cluster(nthreads=[])
async def test_async_add_remove_worker(s):
    events = []

    class MyPlugin(SchedulerPlugin):
        name = "MyPlugin"

        async def add_worker(self, worker, scheduler):
            assert scheduler is s
            events.append(("add_worker", worker))

        async def remove_worker(self, worker, scheduler, **kwargs):
            assert scheduler is s
            events.append(("remove_worker", worker))

    plugin = MyPlugin()
    s.add_plugin(plugin)
    assert events == []

    async with Worker(s.address) as a:
        async with Worker(s.address) as b:
            pass

    assert len(events) == 4
    assert set(events) == {
        ("add_worker", a.address),
        ("add_worker", b.address),
        ("remove_worker", a.address),
        ("remove_worker", b.address),
    }

    events[:] = []
    s.remove_plugin(plugin.name)
    async with Worker(s.address):
        pass
    assert events == []


@gen_cluster(nthreads=[])
async def test_async_and_sync_add_remove_worker(s):
    events = []

    class MyAsyncPlugin(SchedulerPlugin):
        def __init__(self, name):
            super().__init__()
            self.name = name
            self.in_remove_worker = asyncio.Event()
            self.block_remove_worker = asyncio.Event()

        async def add_worker(self, scheduler, worker):
            assert scheduler is s

            await asyncio.sleep(0)
            events.append((self.name, "add_worker", worker))

        async def remove_worker(self, scheduler, worker, **kwargs):
            assert scheduler is s
            self.in_remove_worker.set()
            await self.block_remove_worker.wait()
            events.append((self.name, "remove_worker", worker))

    class MySyncPlugin(SchedulerPlugin):
        def __init__(self, name):
            self.name = name

        def add_worker(self, worker, scheduler):
            assert scheduler is s
            events.append((self.name, "add_worker", worker))

        def remove_worker(self, worker, scheduler, **kwargs):
            assert scheduler is s
            events.append((self.name, "remove_worker", worker))

    sync_plugin_before = MySyncPlugin(name="before")
    s.add_plugin(sync_plugin_before)

    async_plugin = MyAsyncPlugin(name="async")
    s.add_plugin(async_plugin)

    sync_plugin_after = MySyncPlugin(name="after")
    s.add_plugin(sync_plugin_after)
    assert events == []

    async with Worker(s.address) as a:
        assert len(events) == 3

        # No ordering guarantees between these
        assert set(events[:2]) == {
            ("before", "add_worker", a.address),
            ("after", "add_worker", a.address),
        }
        # Async add_worker happens after sync add_worker,
        # but before code within the context manager is run
        assert events[2] == ("async", "add_worker", a.address)
        events.clear()

        async with Worker(s.address) as b:
            assert len(events) == 3
            # No ordering guarantees between these
            assert set(events[:2]) == {
                ("before", "add_worker", b.address),
                ("after", "add_worker", b.address),
            }
            # Async add_worker happens after sync add_worker,
            # but before code within the context manager is run
            assert events[2] == ("async", "add_worker", b.address)
            events.clear()

        await async_plugin.in_remove_worker.wait()
        assert len(events) == 2
        # No ordering guarantees between these
        assert set(events) == {
            ("before", "remove_worker", b.address),
            ("after", "remove_worker", b.address),
        }
        events.clear()

    assert len(events) == 2
    # No ordering guarantees between these
    assert set(events) == {
        ("before", "remove_worker", a.address),
        ("after", "remove_worker", a.address),
    }
    events.clear()

    async_plugin.block_remove_worker.set()
    await asyncio.sleep(0)
    # No ordering guarantees between these
    assert len(events) == 2
    assert set(events) == {
        ("async", "remove_worker", a.address),
        ("async", "remove_worker", b.address),
    }
    events.clear()


@gen_cluster(nthreads=[])
async def test_failing_async_add_remove_worker(s):
    class MyAsyncPlugin(SchedulerPlugin):
        name = "MyAsyncPlugin"

        def __init__(self) -> None:
            super().__init__()

        async def add_worker(self, scheduler, worker):
            assert scheduler is s
            await asyncio.sleep(0)
            raise RuntimeError("Async add_worker failed")

        async def remove_worker(self, scheduler, worker, **kwargs):
            assert scheduler is s
            await asyncio.sleep(0)
            raise RuntimeError("Async remove_worker failed")

    plugin = MyAsyncPlugin()
    s.add_plugin(plugin)
    with captured_logger("distributed.scheduler") as logger:
        async with Worker(s.address):
            while "add_worker failed" not in logger.getvalue():
                await asyncio.sleep(0)
            pass
        while "remove_worker failed" not in logger.getvalue():
            await asyncio.sleep(0)


@gen_cluster(nthreads=[])
async def test_failing_sync_add_remove_worker(s):
    class MySyncPlugin(SchedulerPlugin):
        name = "MySyncPlugin"

        def __init__(self) -> None:
            super().__init__()

        def add_worker(self, scheduler, worker):
            assert scheduler is s
            raise RuntimeError("Async add_worker failed")

        def remove_worker(self, scheduler, worker, **kwargs):
            assert scheduler is s
            raise RuntimeError("Async remove_worker failed")

    plugin = MySyncPlugin()
    s.add_plugin(plugin)
    with captured_logger("distributed.scheduler") as logger:
        async with Worker(s.address):
            assert "add_worker failed" in logger.getvalue()
        assert "remove_worker failed" in logger.getvalue()


@gen_test()
async def test_lifecycle():
    class LifeCycle(SchedulerPlugin):
        def __init__(self):
            self.history = []

        async def start(self, scheduler):
            self.scheduler = scheduler
            self.history.append("started")

        async def close(self):
            self.history.append("closed")

    plugin = LifeCycle()
    async with Scheduler(plugins=[plugin], dashboard_address=":0") as s:
        pass

    assert plugin.history == ["started", "closed"]
    assert plugin.scheduler is s


@gen_cluster(client=True)
async def test_register_plugin(c, s, a, b):
    class Dummy1(SchedulerPlugin):
        name = "Dummy1"

        def start(self, scheduler):
            scheduler.foo = "bar"

    assert not hasattr(s, "foo")
    await c.register_plugin(Dummy1())
    assert s.foo == "bar"

    with pytest.warns(UserWarning) as w:
        await c.register_plugin(Dummy1())
    assert "Scheduler already contains" in w[0].message.args[0]

    class Dummy2(SchedulerPlugin):
        name = "Dummy2"

        def start(self, scheduler):
            raise RuntimeError("raising in start method")

    n_plugins = len(s.plugins)
    with pytest.raises(RuntimeError, match="raising in start method"):
        await c.register_plugin(Dummy2())
    # total number of plugins should be unchanged
    assert n_plugins == len(s.plugins)


@gen_cluster(client=True)
async def test_register_scheduler_plugin_deprecated(c, s, a, b):
    class Dummy(SchedulerPlugin):
        name = "Dummy"

        def start(self, scheduler):
            scheduler.foo = "bar"

    assert not hasattr(s, "foo")
    with pytest.warns(
        DeprecationWarning, match="register_scheduler_plugin.*deprecated"
    ):
        await c.register_scheduler_plugin(Dummy())
        assert s.foo == "bar"


@gen_cluster(nthreads=[])
async def test_unregister_scheduler_plugin(s):
    class Plugin(SchedulerPlugin):
        def __init__(self):
            self.name = "plugin"

    plugin = Plugin()
    await s.register_scheduler_plugin(plugin=dumps(plugin), idempotent=False)
    assert "plugin" in s.plugins

    await s.unregister_scheduler_plugin(name="plugin")
    assert "plugin" not in s.plugins

    with pytest.raises(ValueError, match="Could not find plugin"):
        await s.unregister_scheduler_plugin(name="plugin")


@gen_cluster(client=True)
async def test_unregister_scheduler_plugin_from_client(c, s, a, b):
    class Plugin(SchedulerPlugin):
        name = "plugin"

    assert "plugin" not in s.plugins
    await c.register_plugin(Plugin())
    assert "plugin" in s.plugins

    await c.unregister_scheduler_plugin("plugin")
    assert "plugin" not in s.plugins

    with pytest.raises(ValueError, match="Could not find plugin"):
        await c.unregister_scheduler_plugin(name="plugin")


@gen_cluster(client=True)
async def test_log_event_plugin(c, s, a, b):
    class EventPlugin(SchedulerPlugin):
        async def start(self, scheduler: Scheduler) -> None:
            self.scheduler = scheduler
            self.scheduler._recorded_events = list()  # type: ignore

        def log_event(self, name, msg):
            self.scheduler._recorded_events.append((name, msg))

    await c.register_plugin(EventPlugin())

    def f():
        get_worker().log_event("foo", 123)

    await c.submit(f)

    assert ("foo", 123) in s._recorded_events


@gen_cluster(client=True)
async def test_register_plugin_on_scheduler(c, s, a, b):
    class MyPlugin(SchedulerPlugin):
        async def start(self, scheduler: Scheduler) -> None:
            scheduler._foo = "bar"  # type: ignore

    await s.register_scheduler_plugin(MyPlugin(), idempotent=False)

    assert s._foo == "bar"


@gen_cluster(client=True)
async def test_closing_errors_ok(c, s, a, b, capsys):
    class OK(SchedulerPlugin):
        async def before_close(self):
            print(123)

        async def close(self):
            print(456)

    class Bad(SchedulerPlugin):
        async def before_close(self):
            raise Exception("BEFORE_CLOSE")

        async def close(self):
            raise Exception("AFTER_CLOSE")

    await s.register_scheduler_plugin(OK(), idempotent=False)
    await s.register_scheduler_plugin(Bad(), idempotent=False)

    with captured_logger("distributed.scheduler") as logger:
        await s.close()

    out, err = capsys.readouterr()
    assert "123" in out
    assert "456" in out

    text = logger.getvalue()
    assert "BEFORE_CLOSE" in text
    text = logger.getvalue()
    assert "AFTER_CLOSE" in text


@gen_cluster(client=True)
async def test_update_graph_hook_simple(c, s, a, b):
    class UpdateGraph(SchedulerPlugin):
        def __init__(self) -> None:
            self.success = False

        def update_graph(  # type: ignore
            self,
            scheduler,
            client,
            keys,
            tasks,
            annotations,
            priority,
            dependencies,
            **kwargs,
        ) -> None:
            assert scheduler is s
            assert client == c.id
            # If new parameters are added we should add a test
            assert not kwargs
            assert keys == {"foo"}
            assert tasks == ["foo"]
            assert annotations == {}
            assert len(priority) == 1
            assert isinstance(priority["foo"], tuple)
            assert dependencies == {"foo": set()}
            self.success = True

    plugin = UpdateGraph()
    s.add_plugin(plugin, name="update-graph")

    await c.submit(inc, 5, key="foo")
    assert plugin.success


import dask
from dask import delayed


@gen_cluster(client=True)
async def test_update_graph_hook_complex(c, s, a, b):
    class UpdateGraph(SchedulerPlugin):
        def __init__(self) -> None:
            self.success = False

        def update_graph(  # type: ignore
            self,
            scheduler,
            client,
            keys,
            tasks,
            annotations,
            priority,
            dependencies,
            **kwargs,
        ) -> None:
            assert scheduler is s
            assert client == c.id
            # If new parameters are added we should add a test
            assert not kwargs
            assert keys == {"sum"}
            assert set(tasks) == {"sum", "f1", "f3", "f2"}
            assert annotations == {
                "global_annot": {k: 24 for k in tasks},
                "layer": {"f2": "explicit"},
                "len_key": {"f3": 2},
                "priority": {"f2": 13},
            }
            assert len(priority) == len(tasks), priority
            assert priority["f2"][0] == -13
            for k in keys:
                assert k in dependencies
            assert dependencies["f1"] == set()
            assert dependencies["sum"] == {"f1", "f3"}

            self.success = True

    plugin = UpdateGraph()
    s.add_plugin(plugin, name="update-graph")
    del_inc = delayed(inc)
    f1 = del_inc(1, dask_key_name="f1")
    with dask.annotate(layer="explicit", priority=13):
        f2 = del_inc(2, dask_key_name="f2")
    with dask.annotate(len_key=lambda x: len(x)):
        f3 = del_inc(f2, dask_key_name="f3")

    f4 = delayed(sum)([f1, f3], dask_key_name="sum")

    with dask.annotate(global_annot=24):
        await c.compute(f4)
    assert plugin.success


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_scheduler_plugin_in_register_worker_plugin_overrides(c, s, a):
    class DuckPlugin(SchedulerPlugin):
        def start(self, scheduler):
            scheduler.foo = 123

        def stop(self, scheduler):
            pass

    n_existing_plugins = len(s.plugins)
    assert not hasattr(s, "foo")
    with (
        pytest.warns(UserWarning, match="`SchedulerPlugin` as a worker plugin"),
        pytest.warns(DeprecationWarning, match="use `Client.register_plugin` instead"),
    ):
        await c.register_worker_plugin(DuckPlugin(), nanny=False)
    assert len(s.plugins) == n_existing_plugins + 1
    assert s.foo == 123


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_scheduler_plugin_in_register_worker_plugin_overrides_nanny(c, s, a):
    class DuckPlugin(SchedulerPlugin):
        def start(self, scheduler):
            scheduler.foo = 123

        def stop(self, scheduler):
            pass

    n_existing_plugins = len(s.plugins)
    assert not hasattr(s, "foo")
    with (
        pytest.warns(UserWarning, match="`SchedulerPlugin` as a nanny plugin"),
        pytest.warns(DeprecationWarning, match="use `Client.register_plugin` instead"),
    ):
        await c.register_worker_plugin(DuckPlugin(), nanny=True)
    assert len(s.plugins) == n_existing_plugins + 1
    assert s.foo == 123


@gen_cluster(client=True, nthreads=[])
async def test_register_idempotent_plugin(c, s):
    class IdempotentPlugin(SchedulerPlugin):
        def __init__(self, instance=None):
            self.name = "idempotentplugin"
            self.instance = instance
            self.idempotent = True

        def start(self, scheduler):
            if self.instance != "first":
                raise RuntimeError(
                    "Only the first plugin should be started when idempotent is set"
                )

    first = IdempotentPlugin(instance="first")
    await c.register_plugin(first)
    assert "idempotentplugin" in s.plugins

    second = IdempotentPlugin(instance="second")
    await c.register_plugin(second)
    assert "idempotentplugin" in s.plugins
    assert s.plugins["idempotentplugin"].instance == "first"


@gen_cluster(client=True, nthreads=[])
async def test_register_non_idempotent_plugin(c, s):
    class NonIdempotentPlugin(SchedulerPlugin):
        def __init__(self, instance=None):
            self.name = "nonidempotentplugin"
            self.instance = instance

    first = NonIdempotentPlugin(instance="first")
    await c.register_plugin(first)
    assert "nonidempotentplugin" in s.plugins

    second = NonIdempotentPlugin(instance="second")
    await c.register_plugin(second)
    assert "nonidempotentplugin" in s.plugins
    assert s.plugins["nonidempotentplugin"].instance == "second"

    third = NonIdempotentPlugin(instance="third")
    with pytest.warns(
        FutureWarning,
        match="`Scheduler.register_scheduler_plugin` now requires `idempotent`",
    ):
        await s.register_scheduler_plugin(
            plugin=dumps(third), name="nonidempotentplugin"
        )
    assert "nonidempotentplugin" in s.plugins
    assert s.plugins["nonidempotentplugin"].instance == "third"


@gen_cluster(client=True, nthreads=[])
async def test_register_plugin_with_idempotent_keyword_is_deprecated(c, s):
    class NonIdempotentPlugin(SchedulerPlugin):
        def __init__(self, instance=None):
            self.name = "nonidempotentplugin"
            self.instance = instance
            # We want to overrule this
            self.idempotent = True

    first = NonIdempotentPlugin(instance="first")
    with pytest.warns(FutureWarning, match="`idempotent` argument is deprecated"):
        await c.register_plugin(first, idempotent=False)
    assert "nonidempotentplugin" in s.plugins

    second = NonIdempotentPlugin(instance="second")
    with pytest.warns(FutureWarning, match="`idempotent` argument is deprecated"):
        await c.register_plugin(second, idempotent=False)
    assert "nonidempotentplugin" in s.plugins
    assert s.plugins["nonidempotentplugin"].instance == "second"

    class IdempotentPlugin(SchedulerPlugin):
        def __init__(self, instance=None):
            self.name = "idempotentplugin"
            self.instance = instance
            # We want to overrule this
            self.idempotent = False

        def start(self, scheduler):
            if self.instance != "first":
                raise RuntimeError(
                    "Only the first plugin should be started when idempotent is set"
                )

    first = IdempotentPlugin(instance="first")
    with pytest.warns(FutureWarning, match="`idempotent` argument is deprecated"):
        await c.register_plugin(first, idempotent=True)
    assert "idempotentplugin" in s.plugins

    second = IdempotentPlugin(instance="second")
    with pytest.warns(FutureWarning, match="`idempotent` argument is deprecated"):
        await c.register_plugin(second, idempotent=True)
    assert "idempotentplugin" in s.plugins
    assert s.plugins["idempotentplugin"].instance == "first"


@gen_cluster(nthreads=[])
async def test_register_idempotent_plugins_directly(s):
    class IdempotentPlugin(SchedulerPlugin):
        def __init__(self, instance=None):
            self.name = "idempotentplugin"
            self.instance = instance

        def start(self, scheduler):
            if self.instance != "first":
                raise RuntimeError(
                    "Only the first plugin should be started when idempotent is set"
                )

    first = IdempotentPlugin(instance="first")
    await s.register_scheduler_plugin(plugin=dumps(first), idempotent=True)
    assert "idempotentplugin" in s.plugins

    second = IdempotentPlugin(instance="second")
    await s.register_scheduler_plugin(plugin=dumps(second), idempotent=True)
    assert "idempotentplugin" in s.plugins
    assert s.plugins["idempotentplugin"].instance == "first"


@gen_cluster(nthreads=[])
async def test_register_non_idempotent_plugins_directly(s):
    class NonIdempotentPlugin(SchedulerPlugin):
        def __init__(self, instance=None):
            self.name = "nonidempotentplugin"
            self.instance = instance

    first = NonIdempotentPlugin(instance="first")
    await s.register_scheduler_plugin(plugin=dumps(first), idempotent=False)
    assert "nonidempotentplugin" in s.plugins

    second = NonIdempotentPlugin(instance="second")
    await s.register_scheduler_plugin(plugin=dumps(second), idempotent=False)
    assert "nonidempotentplugin" in s.plugins
    assert s.plugins["nonidempotentplugin"].instance == "second"

    third = NonIdempotentPlugin(instance="third")
    with pytest.warns(
        FutureWarning,
        match="`Scheduler.register_scheduler_plugin` now requires `idempotent`",
    ):
        await s.register_scheduler_plugin(plugin=dumps(third))
    assert "nonidempotentplugin" in s.plugins
    assert s.plugins["nonidempotentplugin"].instance == "third"

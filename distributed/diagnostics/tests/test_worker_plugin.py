import asyncio

import pytest

from distributed import Worker, WorkerPlugin
from distributed.utils_test import async_wait_for, gen_cluster, inc


class MyPlugin(WorkerPlugin):
    name = "MyPlugin"

    def __init__(self, data, expected_notifications=None):
        self.data = data
        self.expected_notifications = expected_notifications

    def setup(self, worker):
        assert isinstance(worker, Worker)
        self.worker = worker
        self.worker._my_plugin_status = "setup"
        self.worker._my_plugin_data = self.data

        self.observed_notifications = []

    def teardown(self, worker):
        self.worker._my_plugin_status = "teardown"

        if self.expected_notifications is not None:
            assert len(self.observed_notifications) == len(self.expected_notifications)
            for expected, real in zip(
                self.expected_notifications, self.observed_notifications
            ):
                assert expected == real

    def transition(self, key, start, finish, **kwargs):
        self.observed_notifications.append(
            {"key": key, "start": start, "finish": finish}
        )


@gen_cluster(client=True, nthreads=[])
async def test_create_with_client(c, s):
    await c.register_worker_plugin(MyPlugin(123))

    worker = await Worker(s.address, loop=s.loop)
    assert worker._my_plugin_status == "setup"
    assert worker._my_plugin_data == 123

    await worker.close()
    assert worker._my_plugin_status == "teardown"


@gen_cluster(client=True, nthreads=[])
async def test_remove_with_client(c, s):
    await c.register_worker_plugin(MyPlugin(123), name="foo")
    await c.register_worker_plugin(MyPlugin(546), name="bar")

    worker = await Worker(s.address, loop=s.loop)
    # remove the 'foo' plugin
    await c.unregister_worker_plugin("foo")
    assert worker._my_plugin_status == "teardown"

    # check that on the scheduler registered worker plugins we only have 'bar'
    assert len(s.worker_plugins) == 1
    assert "bar" in s.worker_plugins

    # check on the worker plugins that we only have 'bar'
    assert len(worker.plugins) == 1
    assert "bar" in worker.plugins

    # let's remove 'bar' and we should have none worker plugins
    await c.unregister_worker_plugin("bar")
    assert worker._my_plugin_status == "teardown"
    assert not s.worker_plugins
    assert not worker.plugins


@gen_cluster(client=True, nthreads=[])
async def test_remove_with_client_raises(c, s):
    await c.register_worker_plugin(MyPlugin(123), name="foo")

    worker = await Worker(s.address, loop=s.loop)
    with pytest.raises(ValueError, match="bar"):
        await c.unregister_worker_plugin("bar")


@gen_cluster(client=True, nthreads=[])
async def test_create_with_client_and_plugin_from_class(c, s):
    await c.register_worker_plugin(MyPlugin, data=456)

    worker = await Worker(s.address, loop=s.loop)
    assert worker._my_plugin_status == "setup"
    assert worker._my_plugin_data == 456

    # Give the plugin a new name so that it registers
    await c.register_worker_plugin(MyPlugin, name="new", data=789)
    assert worker._my_plugin_data == 789


@gen_cluster(client=True, worker_kwargs={"plugins": [MyPlugin(5)]})
async def test_create_on_construction(c, s, a, b):
    assert len(a.plugins) == len(b.plugins) == 1
    assert a._my_plugin_status == "setup"
    assert a._my_plugin_data == 5


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_normal_task_transitions_called(c, s, w):
    expected_notifications = [
        {"key": "task", "start": "released", "finish": "waiting"},
        {"key": "task", "start": "waiting", "finish": "ready"},
        {"key": "task", "start": "ready", "finish": "executing"},
        {"key": "task", "start": "executing", "finish": "memory"},
        {"key": "task", "start": "memory", "finish": "released"},
        {"key": "task", "start": "released", "finish": "forgotten"},
    ]

    plugin = MyPlugin(1, expected_notifications=expected_notifications)

    await c.register_worker_plugin(plugin)
    await c.submit(lambda x: x, 1, key="task")
    await async_wait_for(lambda: not w.tasks, timeout=10)


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_failing_task_transitions_called(c, s, w):
    def failing(x):
        raise Exception()

    expected_notifications = [
        {"key": "task", "start": "released", "finish": "waiting"},
        {"key": "task", "start": "waiting", "finish": "ready"},
        {"key": "task", "start": "ready", "finish": "executing"},
        {"key": "task", "start": "executing", "finish": "error"},
        {"key": "task", "start": "error", "finish": "released"},
        {"key": "task", "start": "released", "finish": "forgotten"},
    ]

    plugin = MyPlugin(1, expected_notifications=expected_notifications)

    await c.register_worker_plugin(plugin)

    with pytest.raises(Exception):
        await c.submit(failing, 1, key="task")


@gen_cluster(
    nthreads=[("127.0.0.1", 1)], client=True, worker_kwargs={"resources": {"X": 1}}
)
async def test_superseding_task_transitions_called(c, s, w):
    expected_notifications = [
        {"key": "task", "start": "released", "finish": "waiting"},
        {"key": "task", "start": "waiting", "finish": "constrained"},
        {"key": "task", "start": "constrained", "finish": "executing"},
        {"key": "task", "start": "executing", "finish": "memory"},
        {"key": "task", "start": "memory", "finish": "released"},
        {"key": "task", "start": "released", "finish": "forgotten"},
    ]

    plugin = MyPlugin(1, expected_notifications=expected_notifications)

    await c.register_worker_plugin(plugin)
    await c.submit(lambda x: x, 1, key="task", resources={"X": 1})
    await async_wait_for(lambda: not w.tasks, timeout=10)


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_dependent_tasks(c, s, w):
    dsk = {"dep": 1, "task": (inc, "dep")}

    expected_notifications = [
        {"key": "dep", "start": "released", "finish": "waiting"},
        {"key": "dep", "start": "waiting", "finish": "ready"},
        {"key": "dep", "start": "ready", "finish": "executing"},
        {"key": "dep", "start": "executing", "finish": "memory"},
        {"key": "task", "start": "released", "finish": "waiting"},
        {"key": "task", "start": "waiting", "finish": "ready"},
        {"key": "task", "start": "ready", "finish": "executing"},
        {"key": "task", "start": "executing", "finish": "memory"},
        {"key": "dep", "start": "memory", "finish": "released"},
        {"key": "task", "start": "memory", "finish": "released"},
        {"key": "task", "start": "released", "finish": "forgotten"},
        {"key": "dep", "start": "released", "finish": "forgotten"},
    ]

    plugin = MyPlugin(1, expected_notifications=expected_notifications)

    await c.register_worker_plugin(plugin)
    await c.get(dsk, "task", sync=False)
    await async_wait_for(lambda: not w.tasks, timeout=10)


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_empty_plugin(c, s, w):
    class EmptyPlugin:
        pass

    await c.register_worker_plugin(EmptyPlugin())


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_default_name(c, s, w):
    class MyCustomPlugin(WorkerPlugin):
        pass

    await c.register_worker_plugin(MyCustomPlugin())
    assert len(w.plugins) == 1
    assert next(iter(w.plugins)).startswith("MyCustomPlugin-")


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_release_key_deprecated(c, s, a):
    class ReleaseKeyDeprecated(WorkerPlugin):
        def __init__(self):
            self._called = False

        def release_key(self, key, state, cause, reason, report):
            # Ensure that the handler still works
            self._called = True
            assert state == "memory"
            assert key == "task"

        def teardown(self, worker):
            assert self._called
            return super().teardown(worker)

    await c.register_worker_plugin(ReleaseKeyDeprecated())

    with pytest.deprecated_call(
        match="The `WorkerPlugin.release_key` hook is depreacted"
    ):
        assert await c.submit(inc, 1, key="x") == 2
        while "x" in a.tasks:
            await asyncio.sleep(0.01)


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_assert_no_warning_no_overload(c, s, a):
    """Assert we do not receive a deprecation warning if we do not overload any
    methods
    """

    class Dummy(WorkerPlugin):
        pass

    with pytest.warns(None):
        await c.register_worker_plugin(Dummy())
        assert await c.submit(inc, 1, key="x") == 2
        while "x" in a.tasks:
            await asyncio.sleep(0.01)


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_WorkerPlugin_overwrite(c, s, w):
    class MyCustomPlugin(WorkerPlugin):
        name = "custom"

        def setup(self, worker):
            self.worker = worker
            self.worker.foo = 0

        def transition(self, *args, **kwargs):
            self.worker.foo = 123

        def teardown(self, worker):
            del self.worker.foo

    await c.register_worker_plugin(MyCustomPlugin)

    assert w.foo == 0

    await c.submit(inc, 0)
    assert w.foo == 123

    while s.tasks or w.tasks:
        await asyncio.sleep(0.01)

    class MyCustomPlugin(WorkerPlugin):
        name = "custom"

        def setup(self, worker):
            self.worker = worker
            self.worker.bar = 0

        def transition(self, *args, **kwargs):
            self.worker.bar = 456

        def teardown(self, worker):
            del self.worker.bar

    await c.register_worker_plugin(MyCustomPlugin)

    assert not hasattr(w, "foo")
    assert w.bar == 0

    await c.submit(inc, 0)
    assert w.bar == 456

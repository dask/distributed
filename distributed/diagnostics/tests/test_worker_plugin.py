import asyncio
from collections import namedtuple

import pytest

from distributed import Worker, WorkerPlugin
from distributed.utils_test import gen_cluster, inc


DELAY_BEFORE_TEARDOWN = 0.05


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
                assert type(expected) is type(real) and expected == real

    def transition(self, key, start, finish, **kwargs):
        self.observed_notifications.append(Transition(key, start, finish))

    def release_key(self, key, state, cause, reason, report):
        self.observed_notifications.append(ReleasedKey(key, state))

    def release_dep(self, dep, state, report):
        self.observed_notifications.append(ReleasedDep(dep, state))


Transition = namedtuple("Transition", "key, start, finish")
ReleasedKey = namedtuple("ReleasedKey", "key, state")
ReleasedDep = namedtuple("ReleasedDep", "dep, state")


@gen_cluster(client=True, nthreads=[])
async def test_create_with_client(c, s):
    await c.register_worker_plugin(MyPlugin(123))

    worker = await Worker(s.address, loop=s.loop)
    assert worker._my_plugin_status == "setup"
    assert worker._my_plugin_data == 123

    await worker.close()
    assert worker._my_plugin_status == "teardown"


@gen_cluster(client=True, worker_kwargs={"plugins": [MyPlugin(5)]})
async def test_create_on_construction(c, s, a, b):
    assert len(a.plugins) == len(b.plugins) == 1
    assert a._my_plugin_status == "setup"
    assert a._my_plugin_data == 5


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_normal_task_transitions_called(c, s, w):
    expected_notifications = [
        Transition("task", "waiting", "ready"),
        Transition("task", "ready", "executing"),
        Transition("task", "executing", "memory"),
        ReleasedKey("task", "memory"),
    ]

    plugin = MyPlugin(1, expected_notifications=expected_notifications)

    await c.register_worker_plugin(plugin)
    await c.submit(lambda x: x, 1, key="task")
    await asyncio.sleep(DELAY_BEFORE_TEARDOWN)


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_failing_task_transitions_called(c, s, w):
    def failing(x):
        raise Exception()

    expected_notifications = [
        Transition("task", "waiting", "ready"),
        Transition("task", "ready", "executing"),
        Transition("task", "executing", "error"),
    ]

    plugin = MyPlugin(1, expected_notifications=expected_notifications)

    await c.register_worker_plugin(plugin)

    with pytest.raises(Exception):
        await c.submit(failing, 1, key="task")


@gen_cluster(
    nthreads=[("127.0.0.1", 1)], client=True, worker_kwargs={"resources": {"X": 1}},
)
async def test_superseding_task_transitions_called(c, s, w):
    expected_notifications = [
        Transition("task", "waiting", "constrained"),
        Transition("task", "constrained", "executing"),
        Transition("task", "executing", "memory"),
        ReleasedKey("task", "memory"),
    ]

    plugin = MyPlugin(1, expected_notifications=expected_notifications)

    await c.register_worker_plugin(plugin)
    await c.submit(lambda x: x, 1, key="task", resources={"X": 1})
    await asyncio.sleep(DELAY_BEFORE_TEARDOWN)


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_release_dep_called(c, s, w):
    dsk = {
        "dep": 1,
        "task": (inc, "dep"),
    }

    expected_notifications = [
        Transition("dep", "waiting", "ready"),
        Transition("dep", "ready", "executing"),
        Transition("dep", "executing", "memory"),
        Transition("task", "waiting", "ready"),
        Transition("task", "ready", "executing"),
        Transition("task", "executing", "memory"),
        ReleasedKey("dep", "memory"),
        ReleasedDep("dep", "memory"),
        ReleasedKey("task", "memory"),
    ]

    plugin = MyPlugin(1, expected_notifications=expected_notifications)

    await c.register_worker_plugin(plugin)
    await c.get(dsk, "task", sync=False)
    await asyncio.sleep(DELAY_BEFORE_TEARDOWN)


@gen_cluster(nthreads=[("127.0.0.1", 1)], client=True)
async def test_empty_plugin(c, s, w):
    class EmptyPlugin:
        pass

    await c.register_worker_plugin(EmptyPlugin())

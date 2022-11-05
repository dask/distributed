from __future__ import annotations

import logging

import pytest

from distributed import Scheduler, SchedulerPlugin, Worker, get_worker
from distributed.utils_test import captured_logger, gen_cluster, gen_test, inc


@gen_cluster(client=True)
async def test_simple(c, s, a, b):
    class Counter(SchedulerPlugin):
        def start(self, scheduler):
            self.scheduler = scheduler
            scheduler.add_plugin(self, name="counter")
            self.count = 0

        def transition(self, key, start, finish, *args, **kwargs):
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

        def remove_worker(self, worker, scheduler):
            assert scheduler is s
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
async def test_async_add_remove_worker(s):
    events = []

    class MyPlugin(SchedulerPlugin):
        name = "MyPlugin"

        async def add_worker(self, worker, scheduler):
            assert scheduler is s
            events.append(("add_worker", worker))

        async def remove_worker(self, worker, scheduler):
            assert scheduler is s
            events.append(("remove_worker", worker))

    plugin = MyPlugin()
    s.add_plugin(plugin)
    assert events == []

    async with Worker(s.address) as a:
        async with Worker(s.address) as b:
            pass

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
async def test_register_scheduler_plugin(c, s, a, b):
    class Dummy1(SchedulerPlugin):
        name = "Dummy1"

        def start(self, scheduler):
            scheduler.foo = "bar"

    assert not hasattr(s, "foo")
    await c.register_scheduler_plugin(Dummy1())
    assert s.foo == "bar"

    with pytest.warns(UserWarning) as w:
        await c.register_scheduler_plugin(Dummy1())
    assert "Scheduler already contains" in w[0].message.args[0]

    class Dummy2(SchedulerPlugin):
        name = "Dummy2"

        def start(self, scheduler):
            raise RuntimeError("raising in start method")

    n_plugins = len(s.plugins)
    with pytest.raises(RuntimeError, match="raising in start method"):
        await c.register_scheduler_plugin(Dummy2())
    # total number of plugins should be unchanged
    assert n_plugins == len(s.plugins)


@gen_cluster(client=True, config={"distributed.scheduler.pickle": False})
async def test_register_scheduler_plugin_pickle_disabled(c, s, a, b):
    class Dummy1(SchedulerPlugin):
        def start(self, scheduler):
            scheduler.foo = "bar"

    n_plugins = len(s.plugins)
    with pytest.raises(ValueError) as excinfo:
        await c.register_scheduler_plugin(Dummy1())

    msg = str(excinfo.value)
    assert "disallowed from deserializing" in msg
    assert "distributed.scheduler.pickle" in msg

    assert n_plugins == len(s.plugins)


@gen_cluster(client=True)
async def test_log_event_plugin(c, s, a, b):
    class EventPlugin(SchedulerPlugin):
        async def start(self, scheduler: Scheduler) -> None:
            self.scheduler = scheduler
            self.scheduler._recorded_events = list()  # type: ignore

        def log_event(self, name, msg):
            self.scheduler._recorded_events.append((name, msg))

    await c.register_scheduler_plugin(EventPlugin())

    def f():
        get_worker().log_event("foo", 123)

    await c.submit(f)

    assert ("foo", 123) in s._recorded_events


@gen_cluster(client=True)
async def test_register_plugin_on_scheduler(c, s, a, b):
    class MyPlugin(SchedulerPlugin):
        async def start(self, scheduler: Scheduler) -> None:
            scheduler._foo = "bar"  # type: ignore

    await s.register_scheduler_plugin(MyPlugin())

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

    await s.register_scheduler_plugin(OK())
    await s.register_scheduler_plugin(Bad())

    with captured_logger(logging.getLogger("distributed.scheduler")) as logger:
        await s.close()

    out, err = capsys.readouterr()
    assert "123" in out
    assert "456" in out

    text = logger.getvalue()
    assert "BEFORE_CLOSE" in text
    text = logger.getvalue()
    assert "AFTER_CLOSE" in text

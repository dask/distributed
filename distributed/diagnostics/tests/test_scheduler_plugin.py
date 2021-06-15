import pytest

from distributed import Scheduler, SchedulerPlugin, Worker
from distributed.utils_test import gen_cluster, inc


@gen_cluster(client=True)
async def test_simple(c, s, a, b):
    class Counter(SchedulerPlugin):
        def start(self, scheduler):
            self.scheduler = scheduler
            scheduler.add_plugin(self)
            self.count = 0

        def transition(self, key, start, finish, *args, **kwargs):
            if start == "processing" and finish == "memory":
                self.count += 1

    counter = Counter()
    counter.start(s)
    assert counter in s.plugins

    assert counter.count == 0

    x = c.submit(inc, 1)
    y = c.submit(inc, x)
    z = c.submit(inc, y)

    await z

    assert counter.count == 3
    s.remove_plugin(counter)
    assert counter not in s.plugins


@gen_cluster(nthreads=[], client=False)
async def test_add_remove_worker(s):
    events = []

    class MyPlugin(SchedulerPlugin):
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
    s.remove_plugin(plugin)
    a = await Worker(s.address)
    await a.close()
    assert events == []


@gen_cluster(nthreads=[], client=False)
async def test_async_add_remove_worker(s):
    events = []

    class MyPlugin(SchedulerPlugin):
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
    s.remove_plugin(plugin)
    async with Worker(s.address):
        pass
    assert events == []


@pytest.mark.asyncio
async def test_lifecycle(cleanup):
    class LifeCycle(SchedulerPlugin):
        def __init__(self):
            self.history = []

        async def start(self, scheduler):
            self.scheduler = scheduler
            self.history.append("started")

        async def close(self):
            self.history.append("closed")

    plugin = LifeCycle()
    async with Scheduler(plugins=[plugin]) as s:
        pass

    assert plugin.history == ["started", "closed"]
    assert plugin.scheduler is s


@gen_cluster(client=True)
async def test_register_scheduler_plugin(c, s, a, b):
    class Dummy(SchedulerPlugin):
        def start(self, scheduler):
            scheduler.foo = "bar"

    class NeverRegistered(SchedulerPlugin):
        def start(self, scheduler):
            scheduler.baz = "foo"

    assert not hasattr(s, "foo")
    await c.register_scheduler_plugin(Dummy)
    assert s.foo == "bar"

    d = Dummy()
    start_n_plugins = len(s.plugins)
    await c.register_scheduler_plugin(d)
    middle_n_plugins = len(s.plugins)
    await c.unregister_scheduler_plugin(d)
    final_n_plugins = len(s.plugins)
    assert final_n_plugins == start_n_plugins
    assert middle_n_plugins == final_n_plugins + 1

    with pytest.raises(KeyError):
        never_registered = NeverRegistered()
        await c.unregister_scheduler_plugin(never_registered)

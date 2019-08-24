from distributed.utils_test import gen_cluster
from distributed import Worker


class MyPlugin:
    name = "MyPlugin"

    def __init__(self, data):
        self.data = data
        self.expected_results = []

    def setup(self, worker):
        assert isinstance(worker, Worker)
        self.worker = worker
        self.worker._my_plugin_status = "setup"
        self.worker._my_plugin_data = self.data

    def teardown(self, worker):
        assert isinstance(worker, Worker)
        self.worker._my_plugin_status = "teardown"

    def task_finished(self, result):
        if len(self.expected_results) > 0:
            assert result == self.expected_results.pop(0)

    def add_expected_result(self, result):
        self.expected_results.append(result)


@gen_cluster(client=True, nthreads=[])
def test_create_with_client(c, s):
    yield c.register_worker_plugin(MyPlugin(123))

    worker = yield Worker(s.address, loop=s.loop)
    assert worker._my_plugin_status == "setup"
    assert worker._my_plugin_data == 123

    yield worker.close()
    assert worker._my_plugin_status == "teardown"


@gen_cluster(client=True, worker_kwargs={"plugins": [MyPlugin(5)]})
def test_create_on_construction(c, s, a, b):
    assert len(a.plugins) == len(b.plugins) == 1
    assert a._my_plugin_status == "setup"
    assert a._my_plugin_data == 5


@gen_cluster(client=True, worker_kwargs={"plugins": [MyPlugin(5)]})
def test_idempotence_with_name(c, s, a, b):
    a._my_plugin_data = 100

    yield c.register_worker_plugin(MyPlugin(5))

    assert a._my_plugin_data == 100  # call above has no effect


@gen_cluster(client=True, worker_kwargs={"plugins": [MyPlugin(5)]})
def test_duplicate_with_no_name(c, s, a, b):
    assert len(a.plugins) == len(b.plugins) == 1

    plugin = MyPlugin(10)
    plugin.name = "other-name"

    yield c.register_worker_plugin(plugin)

    assert len(a.plugins) == len(b.plugins) == 2

    assert a._my_plugin_data == 10

    yield c.register_worker_plugin(plugin)
    assert len(a.plugins) == len(b.plugins) == 2

    yield c.register_worker_plugin(plugin, name="foo")
    assert len(a.plugins) == len(b.plugins) == 3


@gen_cluster(client=True)
def test_called_on_task_finished(c, s, a, b):
    plugin = MyPlugin(10)

    plugin.add_expected_result(20)
    plugin.add_expected_result(40)

    yield c.register_worker_plugin(plugin)
    yield c.submit(lambda x: x * 2, 10)
    yield c.submit(lambda x: x * 2, 20)

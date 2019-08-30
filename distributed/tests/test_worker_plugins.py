import pytest

from distributed.utils_test import gen_cluster
from distributed import Worker


class MyPlugin:
    name = "MyPlugin"

    def __init__(self, data):
        self.data = data
        self.expected_args = []
        self.expected_exceptions = []
        self.expected_results = []

    def setup(self, worker):
        assert isinstance(worker, Worker)
        self.worker = worker
        self.worker._my_plugin_status = "setup"
        self.worker._my_plugin_data = self.data

    def teardown(self, worker):
        assert isinstance(worker, Worker)
        self.worker._my_plugin_status = "teardown"

    def task_started(self, worker, key, args, kwargs):
        assert isinstance(worker, Worker)

        if len(self.expected_args) > 0:
            _args, _kwargs = self.expected_args.pop(0)
            if _args is not None:
                assert args == _args
            if _kwargs is not None:
                assert kwargs == _kwargs

    def task_finished(self, worker, key, result):
        assert isinstance(worker, Worker)

        if len(self.expected_results) > 0:
            assert result == self.expected_results.pop(0)

    def task_failed(self, worker, key, exception):
        assert isinstance(worker, Worker)

        if len(self.expected_exceptions) > 0:
            assert isinstance(exception, self.expected_exceptions.pop())

    def expect_task_returns(self, result):
        self.expected_results.append(result)

    def expect_task_called_with(self, args=(), kwargs=None):
        self.expected_args.append((args, kwargs))

    def expect_exception(self, exception_class):
        self.expected_exceptions.append(exception_class)


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
def test_failing_task(c, s, a, b):
    class MyException(Exception):
        pass

    def failing_task(x):
        raise MyException()

    plugin = MyPlugin(10)

    plugin.expect_exception(MyException)

    yield c.register_worker_plugin(plugin)
    with pytest.raises(Exception):
        yield c.submit(failing_task, None)


@gen_cluster(client=True)
def test_called_on_task_started(c, s, a, b):
    plugin = MyPlugin(10)

    plugin.expect_task_called_with(args=(10,))
    plugin.expect_task_called_with(args=(20,))

    yield c.register_worker_plugin(plugin)
    yield c.submit(lambda x: x, 10)
    yield c.submit(lambda x: x, 20)


@gen_cluster(client=True)
def test_called_on_task_finished(c, s, a, b):
    plugin = MyPlugin(10)

    plugin.expect_task_returns(20)
    plugin.expect_task_returns(40)

    yield c.register_worker_plugin(plugin)
    yield c.submit(lambda x: x * 2, 10)
    yield c.submit(lambda x: x * 2, 20)

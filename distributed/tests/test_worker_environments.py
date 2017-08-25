import pytest
from tornado import gen

from distributed import WorkerEnvironment, Worker, Client
from distributed.metrics import time
from distributed.utils_test import gen_cluster, cluster, loop  # noqa

class MyEnv(WorkerEnvironment):
    """Test worker environment"""
    data = 0

    def condition(self):
        return True

    def setup(self):
        self.data = 1

    def teardown(self):
        self.data = -1


@gen_cluster(client=True)
def test_environment_register(c, s, a, b):
    # Register environment
    yield c._environment_register('myenv', MyEnv())
    assert len(s.worker_environments) == 1 and 'myenv' in s.worker_environments
    assert len(s.environment_workers['myenv']) == 2
    assert 'myenv' in a.environments
    assert a.environments['myenv'].data == 1
    assert 'myenv' in b.environments
    assert b.environments['myenv'].data == 1

    # Register another
    yield c._environment_register('never_passes', condition=lambda: False)
    assert len(s.worker_environments) == 2 and 'never_passes' in s.worker_environments
    assert len(s.environment_workers['never_passes']) == 0

    # Add a worker
    w = Worker(s.ip, s.port, loop=s.loop, ncores=1)
    yield w._start(0)
    start = time()
    while 'myenv' not in w.environments:
        yield gen.sleep(0.01)
        assert time() - start < 5

    assert w.address in s.environment_workers['myenv']
    assert len(s.environment_workers['never_passes']) == 0

    # Remove worker
    yield w._close()
    start = time()
    while not w.status == 'closed':
        yield gen.sleep(0.01)
        assert time() - start < 5

    assert w.address not in s.environment_workers['myenv']


@gen_cluster(client=True)
def test_register_condition_for_worker_raises(c, s, a, b):
    r = c._environment_register('myenv', lambda: True)
    with pytest.raises(TypeError) as m:
        r.result()

    assert m.match("Did you mean")


@gen_cluster(client=True)
def test_register_functional(c, s, a, b):
    yield c._environment_register('myenv', condition=lambda: True)
    assert len(s.worker_environments) == 1 and 'myenv' in s.worker_environments
    assert len(s.environment_workers['myenv']) == 2
    assert 'myenv' in a.environments
    assert 'myenv' in b.environments


def test_register_raises(loop):
    with pytest.raises(ZeroDivisionError):
        with cluster() as (s, [a, b]):
            with Client(s['address'], loop=loop) as c:

                c.environment_register("zero-division",
                                       condition=lambda: 1 / 0)

def test_register_twice_raises(loop):
    e1 = MyEnv()
    e2 = MyEnv()
    e2.data = 10  # make the two unequal
    with cluster() as (s, [a, b]):
        with Client(s['address'], loop=loop) as c:
            c.environment_register('env', environment=e1)

            # registering the same one is fine
            c.environment_register('env', environment=e1)

            with pytest.raises(ValueError):
                c.environment_register('env', environment=e2)


@gen_cluster(client=True)
def test_deregister_environment(c, s, a, b):
    yield c._environment_register('myenv', MyEnv())
    # sanity check before real test
    assert len(s.worker_environments) == 1 and 'myenv' in s.worker_environments

    yield c._environment_deregister('myenv')
    assert len(s.environment_workers['myenv']) == 0
    # TODO: what all should we clean up when the number of workers drops to zero?
    assert a.environments['myenv'].data == -1
    assert b.environments['myenv'].data == -1

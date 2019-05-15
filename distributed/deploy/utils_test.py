from ..client import Client

import pytest


class ClusterTest(object):
    Cluster = None
    kwargs = {}

    def setUp(self):
        self.cluster = self.Cluster(2, scheduler_port=0, **self.kwargs)
        self.client = Client(self.cluster.scheduler_address)

    def tearDown(self):
        self.client.close()
        self.cluster.close()

    @pytest.mark.xfail()
    def test_cores(self):
        info = self.client.scheduler_info()
        assert len(self.client.ncores()) == 2

    def test_submit(self):
        future = self.client.submit(lambda x: x + 1, 1)
        assert future.result() == 2

    def test_start_worker(self):
        a = self.client.ncores()
        w = self.cluster.start_worker(ncores=3)
        b = self.client.ncores()

        assert len(b) == 1 + len(a)
        assert any(v == 3 for v in b.values())

        self.cluster.stop_worker(w)

        c = self.client.ncores()
        assert c == a

    def test_context_manager(self):
        with self.Cluster(**self.kwargs) as c:
            with Client(c) as e:
                assert e.ncores()

    def test_no_workers(self):
        with self.Cluster(0, scheduler_port=0, **self.kwargs):
            pass

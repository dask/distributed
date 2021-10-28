from unittest.mock import MagicMock

from distributed.deploy.spec import (
    ProcessInterface,
    SpecCluster,
    Status,
    close_clusters,
)


def test_address_default_none():
    p = ProcessInterface()
    assert p.address is None


def test_child_address_persists():
    class Child(ProcessInterface):
        def __init__(self, address=None):
            self.address = address
            super().__init__()

    c = Child()
    assert c.address is None
    c = Child("localhost")
    assert c.address == "localhost"


def test_close_clusters(monkeypatch):
    def mock_cluster(status: Status, shutdown_on_close: bool = True):
        mock = MagicMock(spec=SpecCluster)
        mock.status = status
        mock.shutdown_on_close = shutdown_on_close
        return mock

    closed_cluster = mock_cluster(Status.closed)
    skipped_cluster = mock_cluster(Status.running, shutdown_on_close=False)
    running_cluster = mock_cluster(Status.running)

    monkeypatch.setattr(SpecCluster, "_instances", {running_cluster, closed_cluster})

    close_clusters()

    closed_cluster.sync.assert_not_called()
    skipped_cluster.sync.assert_not_called()
    running_cluster.sync.assert_called_once_with(
        running_cluster.close, asynchronous=False, callback_timeout=10
    )

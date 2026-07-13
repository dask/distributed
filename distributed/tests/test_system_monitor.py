from __future__ import annotations

from time import sleep

import pytest

import dask

from distributed.system_monitor import SystemMonitor


def test_SystemMonitor():
    sm = SystemMonitor()

    # __init__ calls update()
    a = sm.recent()
    assert any(v > 0 for v in a.values())

    sleep(0.01)
    b = sm.update()
    assert set(a) == set(b)

    for name in (
        "cpu",
        "memory",
        "time",
        "host_net_io.read_bps",
        "host_net_io.write_bps",
    ):
        assert all(v >= 0 for v in sm.quantities[name])

    assert all(len(q) == 2 for q in sm.quantities.values())

    assert "cpu" in repr(sm)


def test_maxlen_zero():
    """maxlen is floored to 1 otherwise recent() would not work"""
    sm = SystemMonitor(maxlen=0)
    sm.update()
    sm.update()
    assert len(sm.quantities["memory"]) == 1
    assert sm.recent()["memory"] == sm.quantities["memory"][-1]


def test_count():
    sm = SystemMonitor(maxlen=5)
    assert sm.count == 1
    sm.update()
    assert sm.count == 2

    for _ in range(10):
        sm.update()

    assert sm.count == 12
    for v in sm.quantities.values():
        assert len(v) == 5


def test_range_query():
    sm = SystemMonitor(maxlen=5)

    assert all(len(v) == 1 for v in sm.range_query(0).values())
    assert all(len(v) == 0 for v in sm.range_query(123).values())

    sm.update()
    sm.update()
    sm.update()

    assert all(len(v) == 4 for v in sm.range_query(0).values())
    assert all(len(v) == 3 for v in sm.range_query(1).values())

    for _ in range(10):
        sm.update()

    assert all(len(v) == 4 for v in sm.range_query(10).values())
    assert all(len(v) == 5 for v in sm.range_query(0).values())


def test_disk_config():
    sm = SystemMonitor()
    a = sm.update()
    assert "host_disk_io.read_bps" in a

    sm = SystemMonitor(monitor_disk_io=False)
    a = sm.update()
    assert "host_disk_io.read_bps" not in a

    with dask.config.set({"distributed.admin.system-monitor.disk": False}):
        sm = SystemMonitor()
        a = sm.update()
        assert "host_disk_io.read_bps" not in a


def test_host_cpu():
    sm = SystemMonitor()
    a = sm.update()
    assert "host_cpu.user" not in a

    sm = SystemMonitor(monitor_host_cpu=True)
    a = sm.update()
    assert "host_cpu.user" in a

    with dask.config.set({"distributed.admin.system-monitor.host-cpu": True}):
        sm = SystemMonitor()
        a = sm.update()
        assert "host_cpu.user" in a


def test_gil_contention():
    pytest.importorskip("gilknocker")

    # Default enabled if gilknocker installed
    sm = SystemMonitor()
    a = sm.update()
    assert "gil_contention" in a

    assert sm._gilknocker.is_running
    sm.close()
    sm.close()  # Idempotent
    assert not sm._gilknocker.is_running

    sm = SystemMonitor(monitor_gil_contention=False)
    a = sm.update()
    assert "gil_contention" not in a

    assert dask.config.get("distributed.admin.system-monitor.gil.enabled")
    with dask.config.set({"distributed.admin.system-monitor.gil.enabled": False}):
        sm = SystemMonitor()
        a = sm.update()
        assert "gil_contention" not in a


def test_windows_fast_net_io_counters():
    import sys

    if sys.platform != "win32":
        pytest.skip("Windows only test")

    from distributed._windows_net_io import _fast_net_io_counters

    res1 = _fast_net_io_counters()
    assert hasattr(res1, "bytes_recv")
    assert hasattr(res1, "bytes_sent")
    assert isinstance(res1.bytes_recv, int)
    assert isinstance(res1.bytes_sent, int)
    assert res1.bytes_recv >= 0
    assert res1.bytes_sent >= 0

    res2 = _fast_net_io_counters()
    assert res2.bytes_recv >= res1.bytes_recv
    assert res2.bytes_sent >= res1.bytes_sent


def test_windows_fast_net_io_counters_fallback(monkeypatch):
    import sys

    if sys.platform != "win32":
        pytest.skip("Windows only test")

    import distributed._windows_net_io
    from distributed._windows_net_io import fast_net_io_counters

    def mock_fast_net_io_counters():
        raise RuntimeError("Simulated ctypes error")

    monkeypatch.setattr(
        distributed._windows_net_io, "_fast_net_io_counters", mock_fast_net_io_counters
    )

    # Calling fast_net_io_counters should fall back to psutil without raising
    import psutil

    expected = psutil.net_io_counters()
    res = fast_net_io_counters()

    # Check that it returns a valid net_io_counters namedtuple or similar from psutil
    assert hasattr(res, "bytes_recv")
    assert hasattr(res, "bytes_sent")

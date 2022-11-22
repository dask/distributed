from __future__ import annotations

from time import sleep

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

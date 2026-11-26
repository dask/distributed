from __future__ import annotations

import pytest

pytest.importorskip("bokeh")

import sys
from types import SimpleNamespace

from bokeh.models import ColumnDataSource, Model

from distributed.dashboard.components.scheduler import Hardware, Occupancy
from distributed.dashboard.components.shared import (
    Processing,
    ProfilePlot,
    ProfileTimePlot,
)
from distributed.dashboard.utils import get_title
from distributed.utils_test import gen_cluster, slowinc


@pytest.mark.parametrize("Component", [Processing])
def test_basic(Component):
    c = Component()
    assert isinstance(c.source, ColumnDataSource)
    assert isinstance(c.root, Model)


def test_occupancy_update():
    worker = SimpleNamespace(address="tcp://worker", occupancy=0)
    scheduler = SimpleNamespace(
        workers={worker.address: worker},
        idle=[],
        saturated=[],
        total_nthreads=1,
    )
    plot = Occupancy(scheduler)

    plot.update()
    assert get_title(plot.root) == "Occupancy"

    worker.occupancy = 1
    plot.update()
    assert get_title(plot.root).startswith("Occupancy -- total time")


@gen_cluster(client=True, nthreads=[])
async def test_hardware_update(c, s):
    plot = Hardware(s)
    plot.disk_data = {"size": ["1 MiB"], "bandwidth": [1]}
    plot.memory_data = {"size": ["1 MiB"], "bandwidth": [1]}
    plot.network_data = {"size": ["1 MiB"], "bandwidth": [1]}

    plot.update()

    assert get_title(plot.memory_figure) == "Memory Bandwidth"
    assert get_title(plot.disk_figure) == "Disk Bandwidth"
    assert get_title(plot.network_figure) == "Network Bandwidth"


@pytest.mark.skipif(sys.version_info.minor == 11, reason="Profiler disabled")
@gen_cluster(
    client=True,
    clean_kwargs={"threads": False},
    config={"distributed.worker.profile.enabled": True},
)
async def test_profile_plot(c, s, a, b):
    p = ProfilePlot()
    assert not p.source.data["left"]
    while not len(p.source.data["left"]):
        await c.submit(slowinc, 1, pure=False, delay=0.1)
        p.update(a.profile_recent)


@pytest.mark.skipif(sys.version_info.minor == 11, reason="Profiler disabled")
@gen_cluster(
    client=True,
    clean_kwargs={"threads": False},
    config={
        "distributed.worker.profile.enabled": True,
        "distributed.worker.profile.interval": "10ms",
        "distributed.worker.profile.cycle": "50ms",
    },
)
async def test_profile_time_plot(c, s, a, b):
    from bokeh.io import curdoc

    sp = ProfileTimePlot(s, doc=curdoc())
    assert "disabled" not in sp.subtitle.text
    sp.trigger_update()

    ap = ProfileTimePlot(a, doc=curdoc())
    assert "disabled" not in sp.subtitle.text
    ap.trigger_update()

    assert len(sp.source.data["left"]) == 0
    assert len(ap.source.data["left"]) == 0

    await c.gather(c.map(slowinc, range(10), delay=0.05))

    ap.trigger_update()
    sp.trigger_update()


@gen_cluster(
    client=True,
    clean_kwargs={"threads": False},
    config={
        "distributed.worker.profile.enabled": False,
        "distributed.worker.profile.interval": "10ms",
        "distributed.worker.profile.cycle": "50ms",
    },
)
async def test_profile_time_plot_disabled(c, s, a, b):
    from bokeh.io import curdoc

    sp = ProfileTimePlot(s, doc=curdoc())
    assert "disabled" in sp.subtitle.text
    sp.trigger_update()

    ap = ProfileTimePlot(a, doc=curdoc())
    assert "disabled" in sp.subtitle.text
    ap.trigger_update()

    assert len(sp.source.data["left"]) == 0
    assert len(ap.source.data["left"]) == 0

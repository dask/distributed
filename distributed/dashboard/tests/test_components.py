import asyncio

import pytest

pytest.importorskip("bokeh")

from bokeh.models import ColumnDataSource, Model

from distributed.dashboard.components.shared import (
    Processing,
    ProfilePlot,
    ProfileTimePlot,
)
from distributed.utils_test import gen_cluster, slowinc


@pytest.mark.parametrize("Component", [Processing])
def test_basic(Component):
    c = Component()
    assert isinstance(c.source, ColumnDataSource)
    assert isinstance(c.root, Model)


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


@gen_cluster(client=True, clean_kwargs={"threads": False})
async def test_profile_time_plot(c, s, a, b):
    from bokeh.io import curdoc

    sp = ProfileTimePlot(s, doc=curdoc())
    sp.trigger_update()

    ap = ProfileTimePlot(a, doc=curdoc())
    ap.trigger_update()

    assert not len(sp.source.data["left"])
    assert not len(ap.source.data["left"])

    await c.gather(c.map(slowinc, range(10), delay=0.05))
    ap.trigger_update()
    sp.trigger_update()
    await asyncio.sleep(0.05)

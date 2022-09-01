from __future__ import annotations

import contextlib
import re
from operator import add
from unittest import mock

import pytest
from packaging.version import parse as parse_version
from tlz import valmap

from distributed.client import wait
from distributed.utils_test import dec, gen_cluster, gen_tls_cluster, inc, throws
from distributed.worker import dumps_task

ipywidgets = pytest.importorskip("ipywidgets")

from ipykernel.comm import Comm
from ipywidgets import Widget

from distributed.diagnostics.progressbar import (
    MultiProgressWidget,
    ProgressWidget,
    progress,
)

#################
# Utility stuff #
#################

# Taken from ipywidgets/widgets/tests/test_interaction.py
#            https://github.com/ipython/ipywidgets
# Licensed under Modified BSD.  Copyright IPython Development Team.  See:
#   https://github.com/ipython/ipywidgets/blob/master/COPYING.md


class DummyComm(Comm):
    comm_id = "a-b-c-d"

    def open(self, *args, **kwargs):
        pass

    def send(self, *args, **kwargs):
        pass

    def close(self, *args, **kwargs):
        pass


_DISPLAY_ATTR = (
    "_repr_mimebundle_"
    if parse_version(ipywidgets.__version__) >= parse_version("8.0.0")
    else "_ipython_display_"
)


def _comm_default(self):
    return DummyComm()


@contextlib.contextmanager
def mock_widget():
    with mock.patch(
        f"ipywidgets.Widget.{_DISPLAY_ATTR}", side_effect=NotImplementedError
    ):
        assert not hasattr(Widget, "_comm_default")
        Widget._comm_default = _comm_default
        try:
            yield
        finally:
            del Widget._comm_default


def f(**kwargs):
    pass


# End code taken from ipywidgets

#####################
# Distributed stuff #
#####################


@mock_widget()
@gen_cluster(client=True)
async def test_progressbar_widget(c, s, a, b):
    x = c.submit(inc, 1)
    y = c.submit(inc, x)
    z = c.submit(inc, y)
    await wait(z)

    progress = ProgressWidget([z.key], scheduler=s.address, complete=True)
    await progress.listen()

    assert progress.bar.value == 1.0
    assert "3 / 3" in progress.bar_text.value

    progress = ProgressWidget([z.key], scheduler=s.address)
    await progress.listen()


@mock_widget()
@gen_cluster(client=True)
async def test_multi_progressbar_widget(c, s, a, b):
    x1 = c.submit(inc, 1)
    x2 = c.submit(inc, x1)
    x3 = c.submit(inc, x2)
    y1 = c.submit(dec, x3)
    y2 = c.submit(dec, y1)
    e = c.submit(throws, y2)
    other = c.submit(inc, 123)
    await wait([other, e])

    p = MultiProgressWidget([e.key], scheduler=s.address, complete=True)
    await p.listen()

    assert p.bars["inc"].value == 1.0
    assert p.bars["dec"].value == 1.0
    assert p.bars["throws"].value == 0.0
    assert "3 / 3" in p.bar_texts["inc"].value
    assert "2 / 2" in p.bar_texts["dec"].value
    assert "0 / 1" in p.bar_texts["throws"].value

    assert p.bars["inc"].bar_style == "success"
    assert p.bars["dec"].bar_style == "success"
    assert p.bars["throws"].bar_style == "danger"

    assert p.status == "error"
    assert "Exception" in p.elapsed_time.value

    try:
        throws(1)
    except Exception as e:
        assert repr(e) in p.elapsed_time.value

    capacities = [
        int(re.search(r"\d+ / \d+", row.children[0].value).group().split(" / ")[1])
        for row in p.bar_widgets.children
    ]
    assert sorted(capacities, reverse=True) == capacities


@mock_widget()
@gen_cluster()
async def test_multi_progressbar_widget_after_close(s, a, b):
    s.update_graph(
        tasks=valmap(
            dumps_task,
            {
                "x-1": (inc, 1),
                "x-2": (inc, "x-1"),
                "x-3": (inc, "x-2"),
                "y-1": (dec, "x-3"),
                "y-2": (dec, "y-1"),
                "e": (throws, "y-2"),
                "other": (inc, 123),
            },
        ),
        keys=["e"],
        dependencies={
            "x-2": {"x-1"},
            "x-3": {"x-2"},
            "y-1": {"x-3"},
            "y-2": {"y-1"},
            "e": {"y-2"},
        },
    )

    p = MultiProgressWidget(["x-1", "x-2", "x-3"], scheduler=s.address)
    await p.listen()

    assert "x" in p.bars


@mock_widget()
def test_values(client):
    L = [client.submit(inc, i) for i in range(5)]
    wait(L)
    p = MultiProgressWidget(L)
    client.sync(p.listen)
    assert set(p.bars) == {"inc"}
    assert p.status == "finished"
    assert p.comm.closed()
    assert "5 / 5" in p.bar_texts["inc"].value
    assert p.bars["inc"].value == 1.0

    x = client.submit(throws, 1)
    p = MultiProgressWidget([x])
    client.sync(p.listen)
    assert p.status == "error"


@mock_widget()
def test_progressbar_done(client):
    L = [client.submit(inc, i) for i in range(5)]
    wait(L)
    p = ProgressWidget(L)
    client.sync(p.listen)
    assert p.status == "finished"
    assert p.bar.value == 1.0
    assert p.bar.bar_style == "success"
    assert "Finished" in p.elapsed_time.value

    f = client.submit(throws, L)
    wait([f])

    p = ProgressWidget([f])
    client.sync(p.listen)
    assert p.status == "error"
    assert p.bar.value == 0.0
    assert p.bar.bar_style == "danger"
    assert "Exception" in p.elapsed_time.value

    try:
        throws(1)
    except Exception as e:
        assert repr(e) in p.elapsed_time.value


@mock_widget()
def test_progressbar_cancel(client):
    import time

    L = [client.submit(lambda: time.sleep(0.3), i) for i in range(5)]
    p = ProgressWidget(L)
    client.sync(p.listen)
    L[-1].cancel()
    wait(L[:-1])
    assert p.status == "error"
    assert p.bar.value == 0  # no tasks finish before cancel is called


@mock_widget()
@gen_cluster()
async def test_multibar_complete(s, a, b):
    s.update_graph(
        tasks=valmap(
            dumps_task,
            {
                "x-1": (inc, 1),
                "x-2": (inc, "x-1"),
                "x-3": (inc, "x-2"),
                "y-1": (dec, "x-3"),
                "y-2": (dec, "y-1"),
                "e": (throws, "y-2"),
                "other": (inc, 123),
            },
        ),
        keys=["e"],
        dependencies={
            "x-2": {"x-1"},
            "x-3": {"x-2"},
            "y-1": {"x-3"},
            "y-2": {"y-1"},
            "e": {"y-2"},
        },
    )

    p = MultiProgressWidget(["e"], scheduler=s.address, complete=True)
    await p.listen()

    assert p._last_response["all"] == {"x": 3, "y": 2, "e": 1}
    assert all(b.value == 1.0 for k, b in p.bars.items() if k != "e")
    assert "3 / 3" in p.bar_texts["x"].value
    assert "2 / 2" in p.bar_texts["y"].value


@mock_widget()
def test_fast(client):
    L = client.map(inc, range(100))
    L2 = client.map(dec, L)
    L3 = client.map(add, L, L2)
    p = progress(L3, multi=True, complete=True, notebook=True)
    client.sync(p.listen)
    assert set(p._last_response["all"]) == {"inc", "dec", "add"}


@mock_widget()
@gen_cluster(client=True, client_kwargs={"serializers": ["msgpack"]})
async def test_serializers(c, s, a, b):
    x = c.submit(inc, 1)
    y = c.submit(inc, x)
    z = c.submit(inc, y)
    await wait(z)

    progress = ProgressWidget([z], scheduler=s.address, complete=True)
    await progress.listen()

    assert progress.bar.value == 1.0
    assert "3 / 3" in progress.bar_text.value


@mock_widget()
@gen_tls_cluster(client=True)
async def test_tls(c, s, a, b):
    x = c.submit(inc, 1)
    y = c.submit(inc, x)
    z = c.submit(inc, y)
    await wait(z)

    progress = ProgressWidget([z], scheduler=s.address, complete=True)
    await progress.listen()

    assert progress.bar.value == 1.0
    assert "3 / 3" in progress.bar_text.value

from __future__ import annotations

import re
from operator import add

import pytest

from dask.utils import key_split

from distributed.client import wait
from distributed.spans import span
from distributed.utils_test import dec, gen_cluster, gen_tls_cluster, inc, throws

pytest.importorskip("ipywidgets")

from distributed.diagnostics.progressbar import (
    MultiProgressWidget,
    ProgressWidget,
    progress,
)


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


def test_progressbar_cancel(client):
    import time

    L = [client.submit(lambda: time.sleep(0.3), i) for i in range(5)]
    p = ProgressWidget(L)
    client.sync(p.listen)
    L[-1].cancel()
    wait(L[:-1])
    assert p.status == "error"
    assert p.bar.value == 0  # no tasks finish before cancel is called


@gen_cluster(client=True)
async def test_multibar_complete(c, s, a, b):
    x1 = c.submit(inc, 1, key="x-1")
    x2 = c.submit(inc, x1, key="x-2")
    x3 = c.submit(inc, x2, key="x-3")
    y1 = c.submit(dec, x3, key="y-1")
    y2 = c.submit(dec, y1, key="y-2")
    e = c.submit(throws, y2, key="e")
    other = c.submit(inc, 123, key="other")
    await other.cancel()

    p = MultiProgressWidget([e.key], scheduler=s.address, complete=True)
    await p.listen()

    assert p._last_response["all"] == {"x": 3, "y": 2, "e": 1}
    assert all(b.value == 1.0 for k, b in p.bars.items() if k != "e")
    assert "3 / 3" in p.bar_texts["x"].value
    assert "2 / 2" in p.bar_texts["y"].value


def test_fast(client):
    L = client.map(inc, range(100))
    L2 = client.map(dec, L)
    L3 = client.map(add, L, L2)
    p = progress(L3, multi=True, complete=True, notebook=True)
    client.sync(p.listen)
    assert set(p._last_response["all"]) == {"inc", "dec", "add"}


def test_multibar_with_spans(client):
    """Test progress(group_by='spans'"""
    with span("span 1"):
        L = client.map(inc, range(100))
    with span("span 2"):
        L2 = client.map(dec, L)
    with span("span 3"):
        L3 = client.map(add, L, L2)
    with span("other span"):
        _ = client.submit(inc, 123)
    e = client.submit(throws, L3)

    p = progress(e, complete=True, multi=True, notebook=True, group_by="spans")
    client.sync(p.listen)

    # keys are tuples of (group_name, group_id), just get names
    bar_items = {k[0]: v.value for k, v in p.bars.items()}
    bar_texts = {k[0]: v.value for k, v in p.bar_texts.items()}
    bar_labels = {k[0]: v.value for k, v in p.bar_labels.items()}

    assert bar_items == {"span 1": 1, "span 2": 1, "span 3": 1, "default": 0}
    assert bar_texts.keys() == {"span 1", "span 2", "span 3", "default"}
    assert all("100 / 100" in v for k, v in bar_texts.items() if k != "default")
    assert bar_labels.keys() == {"span 1", "span 2", "span 3", "default"}
    assert all(f">{k}<" in v for k, v in bar_labels.items())


def test_multibar_func_warns(client):
    """Deprecate `func`, use `group_by`"""
    L = client.map(inc, range(100))
    L2 = client.map(dec, L)
    L3 = client.map(add, L, L2)

    # ensure default value if nothing is set
    p = MultiProgressWidget(L3)
    assert p.group_by == key_split

    with pytest.warns(
        DeprecationWarning, match="`func` is deprecated, use `group_by` instead"
    ):
        MultiProgressWidget(L3, func="foo")


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

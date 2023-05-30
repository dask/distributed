from __future__ import annotations

from dask import delayed

from distributed.spans import span
from distributed.utils_test import async_poll_for, gen_cluster, inc


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_spans(c, s, a):
    x = delayed(inc)(1)  # Default span
    with span("my workflow"):
        with span("p1"):
            y = x + 1

        @span("p2")
        def f(i):
            return i * 2

        z = f(y)

    zp = c.persist(z)
    assert await c.compute(zp) == 6

    ext = s.extensions["spans"]

    assert s.tasks[x.key].annotations == {}
    assert s.tasks[y.key].annotations == {"span": ("my workflow", "p1")}
    assert s.tasks[z.key].annotations == {"span": ("my workflow", "p2")}

    assert a.state.tasks[x.key].annotations == {}
    assert a.state.tasks[y.key].annotations == {"span": ("my workflow", "p1")}
    assert a.state.tasks[z.key].annotations == {"span": ("my workflow", "p2")}

    assert ext.spans.keys() == {
        ("default",),
        ("my workflow",),
        ("my workflow", "p1"),
        ("my workflow", "p2"),
    }
    for k, sp in ext.spans.items():
        assert sp.id == k

    default = ext.spans["default",]
    mywf = ext.spans["my workflow",]
    p1 = ext.spans["my workflow", "p1"]
    p2 = ext.spans["my workflow", "p2"]

    assert default.children == set()
    assert mywf.children == {p1, p2}
    assert p1.children == set()
    assert p2.children == set()

    assert str(default) == "Span('default',)"
    assert str(p1) == "Span('my workflow', 'p1')"
    assert ext.root_spans == {"default": default, "my workflow": mywf}
    assert ext.spans_search_by_tag["my workflow"] == {mywf, p1, p2}

    assert s.tasks[x.key].annotations == {}
    assert s.tasks[y.key].annotations["span"] == ("my workflow", "p1")

    # Test that spans survive their tasks
    del zp
    await async_poll_for(lambda: not s.tasks, timeout=5)
    assert ext.spans.keys() == {
        ("default",),
        ("my workflow",),
        ("my workflow", "p1"),
        ("my workflow", "p2"),
    }


@gen_cluster(client=True)
async def test_submit(c, s, a, b):
    x = c.submit(inc, 1, key="x")
    with span("foo"):
        y = c.submit(inc, 2, key="y")
    assert await x == 2
    assert await y == 3

    assert "span" not in s.tasks["x"].annotations
    assert s.tasks["y"].annotations["span"] == ("foo",)
    assert s.extensions["spans"].spans.keys() == {("default",), ("foo",)}


@gen_cluster(client=True)
async def test_multiple_tags(c, s, a, b):
    with span("foo", "bar"):
        x = c.submit(inc, 1, key="x")
    assert await x == 2

    assert s.tasks["x"].annotations["span"] == ("foo", "bar")
    assert s.extensions["spans"].spans_search_by_tag.keys() == {"foo", "bar"}


@gen_cluster(client=True, scheduler_kwargs={"extensions": {}})
async def test_no_extension(c, s, a, b):
    x = c.submit(inc, 1, key="x")
    assert await x == 2
    assert "spans" not in s.extensions
    assert s.tasks["x"].annotations == {}

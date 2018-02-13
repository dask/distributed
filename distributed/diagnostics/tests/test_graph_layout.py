from distributed.utils_test import gen_cluster, inc
from distributed.diagnostics import GraphLayout


@gen_cluster(client=True)
def test_basic(c, s, a, b):
    gl = GraphLayout(s)
    futures = c.map(inc, range(5))
    total = c.submit(sum, futures)

    yield total

    assert len(gl.x) == len(gl.y) == 6
    assert all(gl.x[f.key] == 0 for f in futures)
    assert gl.x[total.key] == 1
    assert min(gl.y.values()) < gl.y[total.key] < max(gl.y.values())


@gen_cluster(client=True)
def test_construct_after_call(c, s, a, b):
    futures = c.map(inc, range(5))
    total = c.submit(sum, futures)

    yield total

    gl = GraphLayout(s)

    assert len(gl.x) == len(gl.y) == 6
    assert all(gl.x[f.key] == 0 for f in futures)
    assert gl.x[total.key] == 1
    assert min(gl.y.values()) < gl.y[total.key] < max(gl.y.values())


@gen_cluster(client=True)
def test_colors(c, s, a, b):
    gl = GraphLayout(s)
    futures = c.map(inc, range(5))
    total = c.submit(sum, futures)
    del futures

    yield total

    assert gl.color_updates

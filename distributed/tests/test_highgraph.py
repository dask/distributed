import ast

import dask
import dask.array as da
from dask.highlevelgraph import BasicLayer
from dask.blockwise import Blockwise
from dask.core import flatten
from distributed.diagnostics import SchedulerPlugin
from distributed.utils_test import gen_cluster
from numpy.testing import assert_array_equal


@gen_cluster(client=True)
async def test_annotations(c, s, a, b):
    def fn(k):
        return k[1] * 5 + k[2]

    class TestPlugin(SchedulerPlugin):
        def __init__(self):
            self.correct_priorities = 0
            self.correct_resources = 0

        def update_graph(
            self, scheduler, dsk=None, keys=None, restrictions=None, **kwargs
        ):
            for k, a in kwargs["annotations"].items():
                if "priority" in a:
                    p = fn(ast.literal_eval(k))
                    self.correct_priorities += int(p == a["priority"])

                if "resource" in a:
                    self.correct_resources += int("widget" == a["resource"])

    plugin = TestPlugin()
    s.add_plugin(plugin)

    assert plugin in s.plugins

    with dask.annotate(priority=fn):
        A = da.ones((10, 10), chunks=(2, 2))

    with dask.annotate(resource="widget"):
        B = A + 1

    # TODO: replace with client.compute when it correctly supports HLG transmission
    ret = c.get(B.__dask_graph__(), list(flatten(B.__dask_keys__())), sync=False)

    for r in await c.gather(ret):
        assert_array_equal(r, 2)

    assert isinstance(B.__dask_graph__().layers[A.name], BasicLayer)
    assert isinstance(B.__dask_graph__().layers[B.name], Blockwise)

    assert plugin.correct_priorities == 25
    assert plugin.correct_resources == 25

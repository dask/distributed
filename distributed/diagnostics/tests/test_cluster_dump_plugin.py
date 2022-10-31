from __future__ import annotations

from distributed.cluster_dump import DumpArtefact
from distributed.diagnostics.cluster_dump import ClusterDump
from distributed.utils_test import assert_story, gen_cluster, inc


@gen_cluster(client=True)
async def test_cluster_dump_plugin(c, s, *workers, tmp_path):
    dump_file = tmp_path / "cluster_dump.msgpack.gz"
    await c.register_scheduler_plugin(ClusterDump(str(dump_file)), name="cluster-dump")
    plugin = s.plugins["cluster-dump"]
    assert plugin.scheduler is s

    f1 = c.submit(inc, 1)
    f2 = c.submit(inc, f1)

    assert (await f2) == 3
    await s.close()

    dump = DumpArtefact.from_url(str(dump_file))
    assert_story(
        s.story(f1.key, f2.key), dump.scheduler_story(f1.key, f2.key, datetimes=False)
    )
    assert len(dump["workers"]) == len(workers)

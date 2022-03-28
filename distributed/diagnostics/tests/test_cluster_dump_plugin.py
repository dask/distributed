from distributed.cluster_dump import DumpArtefact
from distributed.diagnostics.cluster_dump import ClusterDump
from distributed.utils_test import gen_cluster, inc


@gen_cluster(client=True)
async def test_cluster_dump_plugin(c, s, *workers, tmp_path):
    dump_file = tmp_path / "cluster_dump.msgpack.gz"

    plugin = ClusterDump(s, str(dump_file))
    s.add_plugin(plugin)

    f1 = c.submit(inc, 1)
    f2 = c.submit(inc, f1)

    assert (await f2) == 3
    await s.close(close_workers=True)

    dump = DumpArtefact.from_url(str(dump_file))
    assert {f1.key, f2.key} == set(dump.scheduler_story(f1.key, f2.key).keys())
    assert {f1.key, f2.key} == set(dump.worker_story(f1.key, f2.key).keys())

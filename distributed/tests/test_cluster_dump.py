from __future__ import annotations

import asyncio
from pathlib import Path

import fsspec
import msgpack
import pytest
import yaml

import distributed
from distributed.cluster_dump import DumpArtefact, _tuple_to_list, write_state
from distributed.utils_test import assert_story, gen_cluster, gen_test, inc


@pytest.mark.parametrize(
    "input, expected",
    [
        ([1, 2, 3], [1, 2, 3]),
        ((1, 2, 3), [1, 2, 3]),
        ({"x": (1, (2,))}, {"x": [1, [2]]}),
        ("foo", "foo"),
    ],
)
def test_tuple_to_list(input, expected):
    assert _tuple_to_list(input) == expected


async def get_state():
    return {"foo": "bar", "list": ["a"], "tuple": (1, "two", 3)}


@gen_test()
async def test_write_state_msgpack(tmp_path):
    path = str(tmp_path / "bar")
    await write_state(get_state, path, "msgpack")

    with fsspec.open(f"{path}.msgpack.gz", "rb", compression="gzip") as f:
        readback = msgpack.load(f)
        assert readback == _tuple_to_list(await get_state())


@gen_test()
async def test_write_state_yaml(tmp_path):
    path = str(tmp_path / "bar")
    await write_state(get_state, path, "yaml")

    with open(f"{path}.yaml") as f:
        readback = yaml.safe_load(f)
        assert readback == _tuple_to_list(await get_state())
        f.seek(0)
        assert "!!python/tuple" not in f.read()


def blocked_inc(x, event):
    event.wait()
    return x + 1


@gen_cluster(client=True)
async def test_cluster_dump_state(c, s, a, b, tmp_path):
    filename = tmp_path / "dump"
    futs = c.map(inc, range(2))
    fut_keys = {f.key for f in futs}
    await c.gather(futs)

    event = distributed.Event()
    blocked_fut = c.submit(blocked_inc, 1, event)
    await asyncio.sleep(0.05)
    await c.dump_cluster_state(filename, format="msgpack")

    scheduler_tasks = list(s.tasks.values())
    worker_tasks = [t for w in (a, b) for t in w.state.tasks.values()]

    smem_tasks = [t for t in scheduler_tasks if t.state == "memory"]
    wmem_tasks = [t for t in worker_tasks if t.state == "memory"]

    assert len(smem_tasks) == 2
    assert len(wmem_tasks) == 2

    sproc_tasks = [t for t in scheduler_tasks if t.state == "processing"]
    wproc_tasks = [t for t in worker_tasks if t.state == "executing"]

    assert len(sproc_tasks) == 1
    assert len(wproc_tasks) == 1

    await c.gather(event.set(), blocked_fut)

    dump = DumpArtefact.from_url(f"{filename}.msgpack.gz")

    smem_keys = {t["key"] for t in dump.scheduler_tasks_in_state("memory")}
    wmem_keys = {t["key"] for t in dump.worker_tasks_in_state("memory")}

    assert smem_keys == fut_keys
    assert smem_keys == {t.key for t in smem_tasks}
    assert wmem_keys == fut_keys
    assert wmem_keys == {t.key for t in wmem_tasks}

    sproc_keys = {t["key"] for t in dump.scheduler_tasks_in_state("processing")}
    wproc_keys = {t["key"] for t in dump.worker_tasks_in_state("executing")}

    assert sproc_keys == {t.key for t in sproc_tasks}
    assert wproc_keys == {t.key for t in wproc_tasks}

    sall_keys = {t["key"] for t in dump.scheduler_tasks_in_state()}
    wall_keys = {t["key"] for t in dump.worker_tasks_in_state()}

    assert fut_keys | {blocked_fut.key} == sall_keys
    assert fut_keys | {blocked_fut.key} == wall_keys

    # Mapping API works
    assert "transition_log" in dump["scheduler"]
    assert "log" in dump["workers"][a.address]
    assert len(dump) == 3


@gen_cluster(client=True)
async def test_cluster_dump_story(c, s, a, b, tmp_path):
    filename = tmp_path / "dump"
    f1 = c.submit(inc, 0, key="f1")
    f2 = c.submit(inc, 1, key="f2")
    await c.gather([f1, f2])
    await c.dump_cluster_state(filename, format="msgpack")

    dump = DumpArtefact.from_url(f"{filename}.msgpack.gz")

    story = dump.scheduler_story("f1", "f2")
    assert story.keys() == {"f1", "f2"}

    for k, task_story in story.items():
        assert_story(task_story, s.story(k))

    story = dump.worker_story("f1", "f2")
    assert story.keys() == {"f1", "f2"}
    for k, task_story in story.items():
        assert_story(task_story, a.state.story(k) + b.state.story(k))


@gen_cluster(client=True)
async def test_cluster_dump_to_yamls(c, s, a, b, tmp_path):
    futs = c.map(inc, range(2))
    await c.gather(futs)

    event = distributed.Event()
    blocked_fut = c.submit(blocked_inc, 1, event)
    filename = tmp_path / "dump"
    await asyncio.sleep(0.05)
    await c.dump_cluster_state(filename, format="msgpack")
    await event.set()
    await blocked_fut

    dump = DumpArtefact.from_url(f"{filename}.msgpack.gz")
    yaml_path = Path(tmp_path / "dump")
    dump.to_yamls(yaml_path)

    scheduler_files = {
        "events.yaml",
        "extensions.yaml",
        "general.yaml",
        "task_groups.yaml",
        "tasks.yaml",
        "transition_log.yaml",
        "workers.yaml",
    }

    scheduler_yaml_path = yaml_path / "scheduler"
    expected = {scheduler_yaml_path / f for f in scheduler_files}
    assert expected == set(scheduler_yaml_path.iterdir())

    worker_files = {
        "config.yaml",
        "general.yaml",
        "log.yaml",
        "logs.yaml",
        "tasks.yaml",
    }

    for worker in (a, b):
        worker_yaml_path = yaml_path / worker.id
        expected = {worker_yaml_path / f for f in worker_files}
        assert expected == set(worker_yaml_path.iterdir())

    # Internal dictionary state compaction
    # has not been destructive of the original dictionary
    assert "id" in dump["scheduler"]
    assert "address" in dump["scheduler"]

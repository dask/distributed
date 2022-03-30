import pytest

from dask.core import flatten

from distributed.utils_test import assert_story, gen_cluster


@gen_cluster(client=True)
async def test_stimuli_update_graph_hlg(c, s, a, b):
    da = pytest.importorskip("dask.array")

    A = da.ones((10, 10), chunks=(10, 10))

    key = str(next(flatten(A.__dask_keys__())))

    coro = c.compute(A)
    await coro
    await c.close()

    assert_story(
        s.story(key),
        [
            (key, "released", "waiting", {key: "processing"}),
            (key, "waiting", "processing", {}),
            (
                key,
                "processing",
                "memory",
                {coro.key: "processing"},
            ),
            (
                coro.key,
                "processing",
                "memory",
                {key: "released"},
            ),
            (
                key,
                "memory",
                "released",
                {},
            ),
        ],
    )

    stimuli = [
        "update-graph-hlg",
        "update-graph-hlg",
        "handle-task-finished",
        "handle-task-finished",
        "handle-task-finished",
        "remove-client",
        "remove-client",
    ]

    stories = s.story(key)
    assert len(stories) == len(stimuli)

    for stimulus_id, story in zip(stimuli, stories):
        assert story[-2].startswith(stimulus_id)

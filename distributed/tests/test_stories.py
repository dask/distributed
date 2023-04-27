from __future__ import annotations

import pytest

import dask

from distributed import Worker
from distributed.comm import CommClosedError
from distributed.utils_test import (
    NO_AMM,
    assert_story,
    assert_valid_story,
    gen_cluster,
    inc,
)


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_scheduler_story_stimulus_success(c, s, a):
    f = c.submit(inc, 1)
    key = f.key

    await f

    stories = s.story(key)

    stimulus_ids = {s[-2] for s in stories}
    # Two events
    # - Compute
    # - Success
    assert len(stimulus_ids) == 2
    assert_story(
        stories,
        [
            (key, "released", "waiting", {key: "processing"}),
            (key, "waiting", "processing", {}),
            (key, "processing", "memory", {}),
        ],
    )

    await c.close()

    stories_after_close = s.story(key)
    assert len(stories_after_close) > len(stories)

    stimulus_ids = {s[-2] for s in stories_after_close}
    # One more event
    # - Forget / Release / Free since client closed
    assert len(stimulus_ids) == 3


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_scheduler_story_stimulus_retry(c, s, a):
    def task():
        assert dask.config.get("foo")

    with dask.config.set(foo=False):
        f = c.submit(task)
        with pytest.raises(AssertionError):
            await f

    with dask.config.set(foo=True):
        await f.retry()
        await f

    story = s.story(f.key)
    stimulus_ids = {s[-2] for s in story}
    # Four events
    # - Compute
    # - Erred
    # - Compute / Retry
    # - Success
    assert len(stimulus_ids) == 4

    assert_story(
        story,
        [
            # Erred transitions via released
            (f.key, "processing", "erred", {}),
            (f.key, "erred", "released", {}),
            (f.key, "released", "waiting", {f.key: "processing"}),
        ],
    )


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_client_story(c, s, a):
    f = c.submit(inc, 1)
    assert await f == 2
    story = await c.story(f.key)

    # Every event should be prefixed with it's origin
    # This changes the format compared to default scheduler / worker stories
    prefixes = set()
    stripped_story = list()
    for msg in story:
        prefixes.add(msg[0])
        stripped_story.append(msg[1:])
    assert prefixes == {"scheduler", a.address}

    assert_valid_story(stripped_story, ordered_timestamps=False)

    # If it's a well formed story, we can sort by the last element which is a
    # timestamp and compare the two lists.
    assert sorted(stripped_story, key=lambda msg: msg[-1]) == sorted(
        s.story(f.key) + a.state.story(f.key), key=lambda msg: msg[-1]
    )


class WorkerBrokenStory(Worker):
    async def get_story(self, *args, **kw):
        raise CommClosedError


@gen_cluster(client=True, Worker=WorkerBrokenStory)
@pytest.mark.parametrize("on_error", ["ignore", "raise"])
async def test_client_story_failed_worker(c, s, a, b, on_error):
    f = c.submit(inc, 1)
    coro = c.story(f.key, on_error=on_error)
    await f

    if on_error == "raise":
        with pytest.raises(CommClosedError):
            await coro
    elif on_error == "ignore":
        story = await coro
        assert story
        assert len(story) > 1
    else:
        raise ValueError(on_error)


@gen_cluster(client=True, config=NO_AMM)
async def test_worker_story_with_deps(c, s, a, b):
    """
    Assert that the structure of the story does not change unintentionally and
    expected subfields are actually filled
    """
    dep = c.submit(inc, 1, workers=[a.address], key="dep")
    res = c.submit(inc, dep, workers=[b.address], key="res")
    await res

    story = a.state.story("res")
    assert story == []

    # Story now includes randomized stimulus_ids and timestamps.
    story = b.state.story("res")
    stimulus_ids = {ev[-2].rsplit("-", 1)[0] for ev in story}
    assert stimulus_ids == {"compute-task", "gather-dep-success", "task-finished"}
    # This is a simple transition log
    expected = [
        ("res", "compute-task", "released"),
        ("res", "released", "waiting", "waiting", {"dep": "fetch"}),
        ("res", "waiting", "ready", "ready", {"res": "executing"}),
        ("res", "ready", "executing", "executing", {}),
        ("res", "put-in-memory"),
        ("res", "executing", "memory", "memory", {}),
    ]
    assert_story(story, expected, strict=True)

    story = b.state.story("dep")
    stimulus_ids = {ev[-2].rsplit("-", 1)[0] for ev in story}
    assert stimulus_ids == {"compute-task", "gather-dep-success"}
    expected = [
        ("dep", "ensure-task-exists", "released"),
        ("dep", "released", "fetch", "fetch", {}),
        ("gather-dependencies", a.address, {"dep"}),
        ("dep", "fetch", "flight", "flight", {}),
        ("request-dep", a.address, {"dep"}),
        ("receive-dep", a.address, {"dep"}),
        ("dep", "put-in-memory"),
        ("dep", "flight", "memory", "memory", {"res": "ready"}),
    ]
    assert_story(story, expected, strict=True)

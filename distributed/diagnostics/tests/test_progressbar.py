from __future__ import annotations

from time import sleep

import pytest
from tornado.ioloop import IOLoop

from distributed.diagnostics.progressbar import TextProgressBar, progress
from distributed.metrics import time
from distributed.utils_test import div, gen_cluster, inc


def test_text_progressbar(capsys, client):
    futures = client.map(inc, range(10))
    p = TextProgressBar(futures, interval=0.01, complete=True, loop=client.loop)
    client.gather(futures)

    start = time()
    while p.status != "finished":
        sleep(0.01)
        assert time() - start < 5

    check_bar_completed(capsys)
    assert p._last_response == {"all": 10, "remaining": 0, "status": "finished"}
    assert p.comm.closed()


@gen_cluster(client=True)
async def test_TextProgressBar_error(c, s, a, b):
    x = c.submit(div, 1, 0)

    progress = TextProgressBar([x.key], scheduler=s.address, start=False, interval=0.01)
    await progress.listen()

    assert progress.status == "error"
    assert progress.comm.closed()

    progress = TextProgressBar([x.key], scheduler=s.address, start=False, interval=0.01)
    await progress.listen()
    assert progress.status == "error"
    assert progress.comm.closed()


@gen_cluster()
async def test_TextProgressBar_empty(s, a, b, capsys):
    progress = TextProgressBar([], scheduler=s.address, start=False, interval=0.01)
    await progress.listen()

    assert progress.status == "finished"
    check_bar_completed(capsys)


def check_bar_completed(capsys, width=40):
    out, err = capsys.readouterr()
    # trailing newline so grab next to last line for final state of bar
    bar, percent, time = (i.strip() for i in out.split("\r")[-2].split("|"))
    assert bar == "[" + "#" * width + "]"
    assert percent == "100% Completed"


def test_progress_function(client, capsys):
    f = client.submit(lambda: 1)
    g = client.submit(lambda: 2)

    progress([[f], [[g]]], notebook=False, loop=client.loop)
    check_bar_completed(capsys)

    progress(f, loop=client.loop)
    check_bar_completed(capsys)


def test_progress_function_w_kwargs(client, capsys):
    f = client.submit(lambda: 1)
    g = client.submit(lambda: 2)

    progress(f, interval="20ms", loop=client.loop)
    check_bar_completed(capsys)


def test_progress_function_warns(client):
    with pytest.warns(DeprecationWarning, match="`func` is deprecated"):
        progress(None, func="prefix")


def test_progress_function_raises():
    with pytest.raises(ValueError, match="`group_by` should be "):
        progress(None, group_by="incorrect")


@gen_cluster(client=True, nthreads=[])
async def test_deprecated_loop_properties(c, s):
    class ExampleTextProgressBar(TextProgressBar):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.loop = self.io_loop = IOLoop.current()

    with pytest.warns(DeprecationWarning) as warninfo:
        ExampleTextProgressBar(client=c, keys=[], start=False, loop=IOLoop.current())

    assert [(w.category, *w.message.args) for w in warninfo] == [
        (DeprecationWarning, "setting the loop property is deprecated")
    ]

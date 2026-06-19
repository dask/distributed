from __future__ import annotations

import logging
from collections import deque
from typing import Literal

import dask
from dask.utils import format_time, key_split, parse_timedelta

from distributed.client import Client, default_client
from distributed.diagnostics.plugin import SchedulerPlugin
from distributed.diagnostics.progress_stream import color_of
from distributed.metrics import time

logger = logging.getLogger(__name__)


class TaskStreamPlugin(SchedulerPlugin):
    name = "task-stream"

    def __init__(self, scheduler, maxlen=None):
        if maxlen is None:
            maxlen = max(
                dask.config.get(
                    "distributed.scheduler.dashboard.status.task-stream-length"
                ),
                dask.config.get(
                    "distributed.scheduler.dashboard.tasks.task-stream-length"
                ),
            )
        self.buffer = deque(maxlen=maxlen)
        self.scheduler = scheduler
        self.index = 0

    def transition(self, key, start, finish, *args, **kwargs):
        if start == "processing" and finish in ("memory", "erred"):
            assert kwargs["startstops"]
            kwargs["key"] = key
            self.buffer.append(kwargs)
            self.index += 1

    def collect(self, start=None, stop=None, count=None, start_index=None):
        # ``start_index`` selects records by their position in the monotonically
        # increasing append counter (``self.index``) rather than by wall-clock
        # time. This is immune to clock differences and latency between the
        # client and the workers, which can otherwise cause time-based ``start``
        # boundaries to drop tasks that have already completed.
        if start_index is not None:
            buffer_start = start_index - (self.index - len(self.buffer))
            buffer_start = max(0, min(buffer_start, len(self.buffer)))
            return [self.buffer[i] for i in range(buffer_start, len(self.buffer))]

        def bisect(target, left, right):
            while left != right:
                mid = (left + right) // 2
                stop = max(
                    startstop["stop"] for startstop in self.buffer[mid]["startstops"]
                )
                if stop < target:
                    left = mid + 1
                else:
                    right = mid
            return left

        if isinstance(start, str):
            start = time() - parse_timedelta(start)
        if start is not None:
            start = bisect(start, 0, len(self.buffer))

        if isinstance(stop, str):
            stop = time() - parse_timedelta(stop)
        if stop is not None:
            stop = bisect(stop, 0, len(self.buffer))

        if count is not None:
            if start is None and stop is None:
                stop = len(self.buffer)
                start = stop - count
            elif start is None and stop is not None:
                start = stop - count
            elif start is not None and stop is None:
                stop = start + count

        if stop is None:
            stop = len(self.buffer)
        if start is None:
            start = 0

        start = max(0, start)
        stop = min(stop, len(self.buffer))

        return [self.buffer[i] for i in range(start, stop)]

    def rectangles(self, istart, istop=None, workers=None, start_boundary=0):
        msgs = []
        diff = self.index - len(self.buffer)
        if istop is None:
            istop = self.index
        for i in range(max(0, (istart or 0) - diff), istop - diff if istop else istop):
            msg = self.buffer[i]
            msgs.append(msg)

        return rectangles(msgs, workers=workers, start_boundary=start_boundary)


def rectangles(msgs, workers=None, start_boundary=0):
    if workers is None:
        workers = {}

    L_start = []
    L_duration = []
    L_duration_text = []
    L_key = []
    L_name = []
    L_color = []
    L_alpha = []
    L_worker = []
    L_worker_thread = []
    L_y = []

    for msg in msgs:
        key = msg["key"]
        name = key_split(key)
        startstops = msg.get("startstops", [])
        try:
            worker_thread = f"{msg['worker']}-{msg['thread']}"
        except Exception:
            continue

        if worker_thread not in workers:
            workers[worker_thread] = len(workers) / 2

        for startstop in startstops:
            if startstop["start"] < start_boundary:
                continue
            color = colors[startstop["action"]]
            if type(color) is not str:
                color = color(msg)

            L_start.append((startstop["start"] + startstop["stop"]) / 2 * 1000)
            L_duration.append(1000 * (startstop["stop"] - startstop["start"]))
            L_duration_text.append(format_time(startstop["stop"] - startstop["start"]))
            L_key.append(key)
            L_name.append(prefix[startstop["action"]] + name)
            L_color.append(color)
            L_alpha.append(alphas[startstop["action"]])
            L_worker.append(msg["worker"])
            L_worker_thread.append(worker_thread)
            L_y.append(workers[worker_thread])

    return {
        "start": L_start,
        "duration": L_duration,
        "duration_text": L_duration_text,
        "key": L_key,
        "name": L_name,
        "color": L_color,
        "alpha": L_alpha,
        "worker": L_worker,
        "worker_thread": L_worker_thread,
        "y": L_y,
    }


def color_of_message(msg):
    if msg["status"] == "OK":
        split = key_split(msg["key"])
        return color_of(split)
    else:
        return "black"


colors = {
    "transfer": "red",
    "disk-write": "orange",
    "disk-read": "orange",
    "deserialize": "gray",
    "compute": color_of_message,
}


alphas = {
    "transfer": 0.4,
    "compute": 1,
    "deserialize": 0.4,
    "disk-write": 0.4,
    "disk-read": 0.4,
}


prefix = {
    "transfer": "transfer-",
    "disk-write": "disk-write-",
    "disk-read": "disk-read-",
    "deserialize": "deserialize-",
    "compute": "",
}


async def _get_task_stream_impl(
    client,
    start=None,
    stop=None,
    count=None,
    plot=False,
    filename="task-stream.html",
    bokeh_resources=None,
    start_index=None,
):
    """Asynchronous implementation of Client.get_task_stream and of the
    get_task_stream context manager
    """
    msgs = await client.scheduler.get_task_stream(
        start=start, stop=stop, count=count, start_index=start_index
    )
    if not plot:
        return msgs

    from distributed.dashboard.components.scheduler import task_stream_figure

    rects = rectangles(msgs)
    source, figure = task_stream_figure(sizing_mode="stretch_both")
    source.data.update(rects)
    if plot == "save":
        from bokeh.plotting import output_file, save

        output_file(filename=filename, title="Dask Task Stream")
        save(figure, filename=filename, resources=bokeh_resources)
    return (msgs, figure)


class get_task_stream:
    """
    Collect task stream within a context block

    This provides diagnostic information about every task that was run during
    the time when this block was active.

    This must be used as a context manager.

    Parameters
    ----------
    plot: boolean, str
        If true then also return a Bokeh figure
        If plot == 'save' then save the figure to a file
    filename: str (optional)
        The filename to save to if you set ``plot='save'``

    Examples
    --------
    >>> with get_task_stream() as ts:
    ...     x.compute()
    >>> ts.data
    [...]

    Get back a Bokeh figure and optionally save to a file

    >>> with get_task_stream(plot='save', filename='task-stream.html') as ts:
    ...    x.compute()
    >>> ts.figure
    <Bokeh Figure>

    To share this file with others you may wish to upload and serve it online.
    A common way to do this is to upload the file as a gist, and then serve it
    on https://raw.githack.com ::

       $ python -m pip install gist
       $ gist task-stream.html
       https://gist.github.com/8a5b3c74b10b413f612bb5e250856ceb

    You can then navigate to that site, click the "Raw" button to the right of
    the ``task-stream.html`` file, and then provide that URL to
    https://raw.githack.com .  This process should provide a sharable link that
    others can use to see your task stream plot.

    See Also
    --------
    Client.get_task_stream: Function version of this context manager
    """

    data: list[dict]

    def __init__(
        self,
        client: Client | None = None,
        plot: bool | Literal["save"] = False,
        filename: str = "task-stream.html",
    ):
        self.data = []
        self._plot = plot
        self._filename = filename
        self.figure = None
        self.client = client or default_client()
        self._start_index = None

    def __enter__(self):
        return self.client.sync(self.__aenter__)

    def __exit__(self, exc_type, exc_value, traceback):
        return self.client.sync(self.__aexit__, exc_type, exc_value, traceback)

    async def __aenter__(self):
        """Record the scheduler's task-stream cursor on entry and collect
        everything appended after it on exit. Using the monotonic index
        instead of a wall-clock boundary avoids dropping tasks when there is
        latency or clock skew between the client and the workers.
        """
        self._start_index = await self.client.scheduler.get_task_stream_index()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        res = await _get_task_stream_impl(
            self.client,
            start_index=self._start_index,
            plot=self._plot,
            filename=self._filename,
        )
        if self._plot:
            data, self.figure = res
        else:
            data = res
        self.data.extend(data)

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime

from tornado.ioloop import PeriodicCallback
from typing import TYPE_CHECKING

from .plugin import SchedulerPlugin

if TYPE_CHECKING:
    from ..client import Client  # circular dependency


class MemorySampler:
    """Sample cluster-wide memory usage every <interval> seconds.

    **Usage**

    .. code-block:: python

       client = Client()
       ms = MemorySampler()

       with ms.sample("run 1"):
           <run first workflow>
       with ms.sample("run 2"):
           <run second workflow>
       ...
       ms.plot()

    or with an asynchronous client:

    .. code-block:: python

       client = await Client(asynchronous=True)
       ms = MemorySampler()

       async with ms.sample("run 1"):
           <run first workflow>
       async with ms.sample("run 2"):
           <run second workflow>
       ...
       ms.plot()

    Parameters
    ==========
    client: Client, optional
        client used to connect to the scheduler.
        default: use the global client
    measure: str, optional
        One of the measures from :class:`distributed.scheduler.MemoryState`.
        default: "process"
    interval: float, optional
        sampling interval, in seconds.
        default: 0.5
    """

    client: Client
    samples: dict[str, list[tuple[datetime, int]]]
    plugin: MemorySamplerPlugin

    def __init__(
        self, *, client: Client | None = None, measure: str = "process", interval=0.5
    ):
        from ..client import get_client

        self.client = client or get_client()
        self.samples = {}
        self.plugin = MemorySamplerPlugin(measure, interval)

    def sample(self, label: str):
        """Context manager that records memory usage in the cluster.
        This is synchronous if the client is synchronous and
        asynchronous if the client is asynchronous.
        """
        if self.client.asynchronous:
            return self._sample_async(label)
        else:
            return self._sample_sync(label)

    @contextmanager
    def _sample_sync(self, label: str):
        self.client.register_scheduler_plugin(self.plugin)
        try:
            yield
        finally:
            samples = self.client.run_on_scheduler(
                self.plugin.unregister_and_get_samples
            )
            self.samples[label] = samples

    @asynccontextmanager
    async def _sample_async(self, label: str):
        await self.client.register_scheduler_plugin(self.plugin)
        try:
            yield
        finally:
            samples = await self.client.run_on_scheduler(
                self.plugin.unregister_and_get_samples
            )
            self.samples[label] = samples

    def to_pandas(self):
        """Return the data series as a pandas.Dataframe.
        The timeseries are resampled and aligned to each other.
        """
        import pandas as pd

        ss = {}
        for (label, s_list) in self.samples.items():
            if not s_list:
                continue
            s = pd.DataFrame(s_list).set_index(0)[1]
            s.index -= s.index[0]
            if len(self.samples) > 1:
                s = s.resample(f"{self.plugin.interval / 5}S").ffill()
            ss[label] = s

        return pd.DataFrame(ss)

    def plot(self, **kwargs):
        """Plot data series collected so far

        Parameters
        ==========
        kwargs
            Passed verbatim to pandas.DataFrame.plot()
        """
        df = self.to_pandas() / 2 ** 30
        return df.plot(
            xlabel="time",
            ylabel="Cluster memory (GiB)",
            **kwargs,
        )


class MemorySamplerPlugin(SchedulerPlugin):
    """Server side of MemorySampler"""

    name: str
    measure: str
    interval: float
    samples: list[tuple[datetime, int]]
    pc: PeriodicCallback

    def __init__(self, measure: str, interval: float):
        self.name = f"MemorySampler-{uuid.uuid4()}"
        self.measure = measure
        self.interval = interval
        self.samples = []

    async def start(self, scheduler):
        def sample():
            self.samples.append(
                (datetime.now(), getattr(scheduler.memory, self.measure))
            )

        self.pc = PeriodicCallback(sample, self.interval * 1000)
        self.pc.start()

    async def stop(self):
        self.pc.stop()

    def unregister_and_get_samples(self, dask_scheduler):
        """Remove self from scheduler and return the samples. This method is meant to be
        invoked through Client.run_on_scheduler.
        """
        self = dask_scheduler.plugins[self.name]
        dask_scheduler.remove_plugin(self.name)
        self.pc.stop()
        return self.samples

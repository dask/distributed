from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Collection, Iterator
from contextlib import asynccontextmanager, contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Any, Literal, cast

from distributed.compatibility import PeriodicCallback

if TYPE_CHECKING:
    # Optional runtime dependencies
    import pandas as pd

    # Circular dependencies
    from distributed.client import Client
    from distributed.scheduler import Scheduler


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
    """

    # {label: [[timestamp, measure 1 value, measure 2 value, ...], ...]}
    samples: dict[str, list[list[float]]]
    # {label: [measure 1 name, 1 measure name, ...]
    measures: dict[str, list[str]]

    def __init__(self):
        self.samples = {}
        self.measures = {}

    def sample(
        self,
        label: str | None = None,
        *,
        client: Client | None = None,
        measure: str | Collection[str] = ("managed", "unmanaged", "spilled"),
        interval: float = 0.5,
    ) -> Any:
        """Context manager that records memory usage in the cluster.
        This is synchronous if the client is synchronous and
        asynchronous if the client is asynchronous.

        The samples are recorded in ``self.samples[<label>]``.

        Parameters
        ==========
        label: str, optional
            Tag to record the samples under in the self.samples dict.
            Default: automatically generate a unique label
        client: Client, optional
            client used to connect to the scheduler.
            Default: use the global client
        measure: str or Collection[str], optional
            One or more measures from :class:`distributed.scheduler.MemoryState`.
            Default: sample managed, unmanaged, and spilled memory
        interval: float, optional
            sampling interval, in seconds.
            Default: 0.5
        """
        if not client:
            from distributed.client import get_client

            client = get_client()

        measures = [measure] if isinstance(measure, str) else list(measure)

        if not label:
            for i in range(len(self.samples) + 1):
                label = f"Samples {i}"
                if label not in self.samples:
                    break
            assert label

        self.samples[label] = []
        self.measures[label] = measures

        if client.asynchronous:
            return self._sample_async(client, label, measures, interval)
        else:
            return self._sample_sync(client, label, measures, interval)

    @contextmanager
    def _sample_sync(
        self, client: Client, label: str, measures: list[str], interval: float
    ) -> Iterator[None]:
        key = client.sync(
            client.scheduler.memory_sampler_start,
            client=client.id,
            measures=measures,
            interval=interval,
        )
        try:
            yield
        finally:
            samples = client.sync(client.scheduler.memory_sampler_stop, key=key)
            self.samples[label] = samples

    @asynccontextmanager
    async def _sample_async(
        self, client: Client, label: str, measures: list[str], interval: float
    ) -> AsyncIterator[None]:
        key = await client.scheduler.memory_sampler_start(
            client=client.id, measures=measures, interval=interval
        )
        try:
            yield
        finally:
            samples = await client.scheduler.memory_sampler_stop(key=key)
            self.samples[label] = samples

    def to_pandas(self, *, align: bool = True) -> pd.DataFrame:
        """Return the data series as a pandas.Dataframe.

        Parameters
        ==========
        align : bool, optional
            If True (the default), change the absolute timestamps into seconds from the
            first sample of each series, so that different series can be visualized side
            by side. If False, use absolute timestamps.
        """
        import numpy as np
        import pandas as pd

        dfs = []
        for label, s_list in self.samples.items():
            assert s_list  # There's always at least one sample
            df = pd.DataFrame(s_list).set_index(0)
            df.index = pd.to_datetime(df.index, unit="s")
            df.columns = pd.MultiIndex.from_tuples(
                [(label, measure) for measure in self.measures[label]],
                names=["label", "measure"],
            )
            if align:
                # convert datetime to timedelta from the first sample
                df.index -= cast(pd.Timestamp, df.index[0])
            dfs.append(df)

        df = pd.concat(dfs, axis=1).sort_index()
        # Forward-fill NaNs in the middle of a series created either by overlapping
        # sampling time range or by align=True. Do not ffill series beyond their
        # last sample.
        df = df.ffill().where(~pd.isna(df.bfill()))

        if align:
            df.index = np.round(df.index.total_seconds(), 2)  # type: ignore
        return df

    def plot(
        self,
        *,
        align: bool = True,
        kind: Literal["line", "area-h", "area-v"] = "area-h",
        sharex: Literal[False, "none", "all", "row", "col"] = "all",
        sharey: Literal[False, "none", "all", "row", "col"] = "all",
        **kwargs: Any,
    ) -> Any:
        """Plot data series collected so far

        Parameters
        ==========
        align : bool (optional)
            See :meth:`~distributed.diagnostics.MemorySampler.to_pandas`
        kind: str (optional)
            line
                all measures from all sample sets are superimposed on the same plot,
                not stacked.
            area-h
                stacked graphs, one sample set per subplot, aligned horizontally
            area-v
                stacked graphs, one sample set per subplot, aligned vertically
                (there's no difference from area-h if you have only one sample set).
        sharex, sharey: (optional)
            Align axes between subplots. See :meth:`matplotlib.pyplot.subplots`
            Default: align both time and memory among all plots.
        kwargs
            Passed verbatim to :meth:`pandas.DataFrame.plot`
        """
        import matplotlib.pyplot as plt
        import pandas as pd

        df = self.to_pandas(align=align) / 2**30

        n = len(self.samples)
        if n > 1 and kind == "area-h":
            fig, axes = plt.subplots(ncols=n, sharex=sharex, sharey=sharey)
        elif n > 1 and kind == "area-v":
            fig, axes = plt.subplots(nrows=n, sharex=sharex, sharey=sharey)
        else:
            [title] = self.samples
            df = cast(pd.DataFrame, df[title])
            df.columns.name = None
            return df.plot(
                kind="area" if kind in ("area-h", "area-v") else kind,  # type: ignore
                xlabel="time",
                ylabel="Cluster memory (GiB)",
                title=title,
                **kwargs,
            )

        for i, title in enumerate(self.samples):
            df[title].plot(ax=axes[i], kind="area", title=title, **kwargs)


class MemorySamplerExtension:
    """Scheduler extension - server side of MemorySampler"""

    scheduler: Scheduler
    # {unique key: [[timestamp, nbytes, nbytes, ...], ...]}
    samples: dict[str, list[list[float]]]

    def __init__(self, scheduler: Scheduler):
        self.scheduler = scheduler
        self.scheduler.extensions["memory_sampler"] = self
        self.scheduler.handlers["memory_sampler_start"] = self.start
        self.scheduler.handlers["memory_sampler_stop"] = self.stop
        self.samples = {}

    def start(self, client: str, measures: list[str], interval: float) -> str:
        """Start periodically sampling memory"""
        mem = self.scheduler.memory
        for measure in measures:
            assert not measure.startswith("_")
            assert isinstance(getattr(mem, measure), int)

        key = str(uuid.uuid4())
        self.samples[key] = []

        def sample():
            if client in self.scheduler.clients:
                ts = datetime.now().timestamp()
                mem = self.scheduler.memory
                nbytes = [getattr(mem, measure) for measure in measures]
                self.samples[key].append([ts] + nbytes)
            else:
                self.stop(key)

        pc = PeriodicCallback(sample, interval * 1000)
        self.scheduler.periodic_callbacks["MemorySampler-" + key] = pc
        pc.start()

        # Immediately collect the first sample; this also ensures there's always at
        # least one sample
        sample()

        return key

    def stop(self, key: str) -> list[list[float]]:
        """Stop sampling and return the samples"""
        pc = self.scheduler.periodic_callbacks.pop("MemorySampler-" + key)
        pc.stop()
        return self.samples.pop(key)

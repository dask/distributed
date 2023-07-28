from __future__ import annotations

import contextlib
import logging
import os
import subprocess
import time
import uuid
from collections.abc import Sequence
from typing import Any, Literal

from toolz.itertoolz import partition

from distributed import get_client
from distributed.worker import Worker

try:
    import memray
except ImportError:
    raise ImportError("You have to install memray to use this module.")

logger = logging.getLogger(__name__)


class DaskMemray:
    def __init__(
        self,
        directory: str = "memray-profiles",
        workers: int | None | list[str] = None,
        report_args: Sequence[str]
        | Literal[False] = ("flamegraph", "--temporal", "--leaks"),
        fetch_reports_parallel: bool | int = True,
        **memray_kwargs: Any,
    ) -> None:
        """Generate a memray profile on the workers and download the generated report.

        Parameters
        ----------
        directory : str
            The directory to save the reports to.
        workers : int | None | list[str]
            The workers to profile. If int, the first n workers will be used.
            If None, all workers will be used.
            If list[str], the workers with the given addresses will be used.
        report_args : tuple[str]
            Particularly for native_traces=True, the reports have to be
            generated on the same host using the same python interpeter as the
            profile was generated. Otherwise, native traces will yield unusable
            results. Therefore, we're generating the reports on the workers and
            download them afterwards. You can modify the report generation by
            providing additional arguments and we will generate the reports as::

                memray *report_args -f <filename> -o <filename>.html

            If ther raw data should be fetched instad of the report, set this to
            False.

        fetch_reports_parallel : bool | int
            Fetching results is sometimes slow and it's sometimes not desired to
            wait for all workers to finish before receiving the first reports.
            This controls how many workers are fetched concurrently.

                int: Number of workers to fetch concurrently
                True: All workers concurrently
                False: One worker at a time

        **memray_kwargs
            Keyword arguments to be passed to memray.Tracker, e.g.
            {"native_traces": True}
        """
        self.directory = directory
        self.client = get_client()
        self.memray_kwargs = memray_kwargs
        self.report_args = report_args
        self.fetch_reports_parallel = fetch_reports_parallel
        if not workers or isinstance(workers, int):
            workers_returned = sorted(self.client.run(lambda: None))
            nworkers = len(workers_returned)
            if isinstance(workers, int):
                nworkers = workers
            workers = workers_returned[:nworkers]
        self.workers = list(workers)
        self.filename = uuid.uuid4().hex

    def __enter__(self) -> None:
        assert all(
            self.client.run(
                _run_memray, filename=self.filename, **self.memray_kwargs
            ).values()
        )
        # Sleep for a brief moment such that we get
        # a clear profiling signal when everything starts
        time.sleep(0.1)

    def __exit__(self, *args: Any) -> None:
        os.makedirs(self.directory, exist_ok=True)

        client = get_client()
        if self.fetch_reports_parallel is True:
            fetch_parallel = len(self.workers)
        elif self.fetch_reports_parallel is False:
            fetch_parallel = 1
        else:
            fetch_parallel = self.fetch_reports_parallel

        for w in partition(fetch_parallel, self.workers):
            try:
                profiles = client.run(
                    _fetch_profile,
                    filename=self.filename,
                    report_args=self.report_args,
                    workers=w,
                )
                for worker_addr, profile in profiles.items():
                    path = f"{self.directory}/{worker_addr.replace('/','')}.html"
                    with open(
                        path,
                        "wb",
                    ) as fd:
                        fd.write(profile)

            except Exception:
                logger.exception(
                    "Exception during report downloading from worker %s", w
                )


def _run_memray(dask_worker: Worker, filename: str, **kwargs: Any) -> bool:
    if hasattr(dask_worker, "_memray"):
        dask_worker._memray.close()

    if os.path.exists(filename):
        os.remove(filename)

    dask_worker._memray = contextlib.ExitStack()  # type: ignore[attr-defined]
    dask_worker._memray.enter_context(  # type: ignore[attr-defined]
        memray.Tracker(filename, native_traces=True, **kwargs)
    )

    return True


def _fetch_profile(
    dask_worker: Worker, filename: str, report_args: Sequence[str] | Literal[False]
) -> bytes:
    if not hasattr(dask_worker, "_memray"):
        return b""
    dask_worker._memray.close()
    del dask_worker._memray

    if not report_args:
        with open(filename, "rb") as fd:
            return fd.read()

    report_filename = filename + ".html"
    if not report_args[0] == "memray":
        report_args = ["memray"] + list(report_args)
    assert "-f" not in report_args, "Cannot provide filename for report generation"
    assert (
        "-o" not in report_args
    ), "Cannot provide output filename for report generation"
    report_args = list(report_args) + ["-f", filename, "-o", report_filename]
    subprocess.run(report_args)
    with open(report_filename, "rb") as fd:
        return fd.read()

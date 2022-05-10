"""Encapsulated manager for in-memory tasks on a worker.

This module covers:
- spill/unspill data depending on the 'distributed.worker.memory.target' threshold
- spill/unspill data depending on the 'distributed.worker.memory.spill' threshold
- pause/unpause the worker depending on the 'distributed.worker.memory.pause' threshold
- kill the worker depending on the 'distributed.worker.memory.terminate' threshold

This module does *not* cover:
- Changes in behaviour in Worker, Scheduler, task stealing, Active Memory Manager, etc.
  caused by the Worker being in paused status
- Worker restart after it's been killed
- Scheduler-side heuristics regarding memory usage, e.g. the Active Memory Manager

See also:
- :mod:`distributed.spill`, which implements the spill-to-disk mechanism and is wrapped
  by this module. Unlike this module, :mod:`distributed.spill` is agnostic to the
  Worker.
- :mod:`distributed.active_memory_manager`, which runs on the scheduler side
"""
from __future__ import annotations

import asyncio
import logging
import os
import sys
import warnings
from collections.abc import Callable, MutableMapping
from contextlib import suppress
from functools import partial
from typing import TYPE_CHECKING, Any, Container, Literal, cast

import psutil
from tornado.ioloop import PeriodicCallback

import dask.config
from dask.system import CPU_COUNT
from dask.utils import format_bytes, parse_bytes, parse_timedelta

from distributed import system
from distributed.core import Status
from distributed.metrics import monotonic
from distributed.spill import ManualEvictProto, SpillBuffer
from distributed.utils import log_errors
from distributed.utils_perf import ThrottledGC

if TYPE_CHECKING:
    # Circular imports
    from distributed.nanny import Nanny
    from distributed.worker import Worker

logger = logging.getLogger(__name__)


class WorkerMemoryManager:
    data: MutableMapping[str, Any]  # {task key: task payload}
    memory_limit: int | None
    memory_target_fraction: float | Literal[False]
    memory_spill_fraction: float | Literal[False]
    memory_pause_fraction: float | Literal[False]
    max_spill: int | Literal[False]
    memory_monitor_interval: float
    _memory_monitoring: bool
    _throttled_gc: ThrottledGC

    def __init__(
        self,
        worker: Worker,
        *,
        memory_limit: str | float = "auto",
        # This should be None most of the times, short of a power user replacing the
        # SpillBuffer with their own custom dict-like
        data: (
            MutableMapping[str, Any]  # pre-initialised
            | Callable[[], MutableMapping[str, Any]]  # constructor
            | tuple[
                Callable[..., MutableMapping[str, Any]], dict[str, Any]
            ]  # (constructor, kwargs to constructor)
            | None  # create internally
        ) = None,
        # Deprecated parameters; use dask.config instead
        memory_target_fraction: float | Literal[False] | None = None,
        memory_spill_fraction: float | Literal[False] | None = None,
        memory_pause_fraction: float | Literal[False] | None = None,
    ):
        self.memory_limit = parse_memory_limit(memory_limit, worker.nthreads)

        self.memory_target_fraction = _parse_threshold(
            "distributed.worker.memory.target",
            "memory_target_fraction",
            memory_target_fraction,
        )
        self.memory_spill_fraction = _parse_threshold(
            "distributed.worker.memory.spill",
            "memory_spill_fraction",
            memory_spill_fraction,
        )
        self.memory_pause_fraction = _parse_threshold(
            "distributed.worker.memory.pause",
            "memory_pause_fraction",
            memory_pause_fraction,
        )

        max_spill = dask.config.get("distributed.worker.memory.max-spill")
        self.max_spill = False if max_spill is False else parse_bytes(max_spill)

        if isinstance(data, MutableMapping):
            self.data = data
        elif callable(data):
            self.data = data()
        elif isinstance(data, tuple):
            self.data = data[0](**data[1])
        elif self.memory_limit and (
            self.memory_target_fraction or self.memory_spill_fraction
        ):
            if self.memory_target_fraction:
                target = int(
                    self.memory_limit
                    * (self.memory_target_fraction or self.memory_spill_fraction)
                )
            else:
                target = sys.maxsize
            self.data = SpillBuffer(
                os.path.join(worker.local_directory, "storage"),
                target=target,
                max_spill=self.max_spill,
            )
        else:
            self.data = {}

        self._memory_monitoring = False

        self.memory_monitor_interval = parse_timedelta(
            dask.config.get("distributed.worker.memory.monitor-interval"),
            default=False,
        )
        assert isinstance(self.memory_monitor_interval, (int, float))

        if self.memory_limit and (
            self.memory_spill_fraction is not False
            or self.memory_pause_fraction is not False
        ):
            assert self.memory_monitor_interval is not None
            pc = PeriodicCallback(
                # Don't store worker as self.worker to avoid creating a circular
                # dependency. We could have alternatively used a weakref.
                # FIXME annotations: https://github.com/tornadoweb/tornado/issues/3117
                partial(self.memory_monitor, worker),  # type: ignore
                self.memory_monitor_interval * 1000,
            )
            worker.periodic_callbacks["memory_monitor"] = pc

        self._throttled_gc = ThrottledGC(logger=logger)

    @log_errors
    async def memory_monitor(self, worker: Worker) -> None:
        """Track this process's memory usage and act accordingly.
        If process memory rises above the spill threshold (70%), start dumping data to
        disk until it goes below the target threshold (60%).
        If process memory rises above the pause threshold (80%), stop execution of new
        tasks.
        """
        if self._memory_monitoring:
            return
        self._memory_monitoring = True
        try:
            # Don't use psutil directly; instead read from the same API that is used
            # to send info to the Scheduler (e.g. for the benefit of Active Memory
            # Manager) and which can be easily mocked in unit tests.
            memory = worker.monitor.get_process_memory()
            self._maybe_pause_or_unpause(worker, memory)
            await self._maybe_spill(worker, memory)
        finally:
            self._memory_monitoring = False

    def _maybe_pause_or_unpause(self, worker: Worker, memory: int) -> None:
        if self.memory_pause_fraction is False:
            return

        assert self.memory_limit
        frac = memory / self.memory_limit
        # Pause worker threads if above 80% memory use
        if frac > self.memory_pause_fraction:
            # Try to free some memory while in paused state
            self._throttled_gc.collect()
            if worker.status == Status.running:
                logger.warning(
                    "Worker is at %d%% memory usage. Pausing worker.  "
                    "Process memory: %s -- Worker memory limit: %s",
                    int(frac * 100),
                    format_bytes(memory),
                    format_bytes(self.memory_limit)
                    if self.memory_limit is not None
                    else "None",
                )
                worker.status = Status.paused
        elif worker.status == Status.paused:
            logger.warning(
                "Worker is at %d%% memory usage. Resuming worker. "
                "Process memory: %s -- Worker memory limit: %s",
                int(frac * 100),
                format_bytes(memory),
                format_bytes(self.memory_limit)
                if self.memory_limit is not None
                else "None",
            )
            worker.status = Status.running

    async def _maybe_spill(self, worker: Worker, memory: int) -> None:
        if self.memory_spill_fraction is False:
            return

        # SpillBuffer or a duct-type compatible MutableMapping which offers the
        # fast property and evict() methods. Dask-CUDA uses this.
        if not hasattr(self.data, "fast") or not hasattr(self.data, "evict"):
            return
        data = cast(ManualEvictProto, self.data)

        assert self.memory_limit
        frac = memory / self.memory_limit
        if frac <= self.memory_spill_fraction:
            return

        total_spilled = 0
        logger.debug(
            "Worker is at %.0f%% memory usage. Start spilling data to disk.",
            frac * 100,
        )
        # Implement hysteresis cycle where spilling starts at the spill threshold and
        # stops at the target threshold. Normally that here the target threshold defines
        # process memory, whereas normally it defines reported managed memory (e.g.
        # output of sizeof() ). If target=False, disable hysteresis.
        target = self.memory_limit * (
            self.memory_target_fraction or self.memory_spill_fraction
        )
        count = 0
        need = memory - target
        last_checked_for_pause = last_yielded = monotonic()

        while memory > target:
            if not data.fast:
                logger.warning(
                    "Unmanaged memory use is high. This may indicate a memory leak "
                    "or the memory may not be released to the OS; see "
                    "https://distributed.dask.org/en/latest/worker-memory.html#memory-not-released-back-to-the-os "
                    "for more information. "
                    "-- Unmanaged memory: %s -- Worker memory limit: %s",
                    format_bytes(memory),
                    format_bytes(self.memory_limit),
                )
                break

            weight = data.evict()
            if weight == -1:
                # Failed to evict:
                # disk full, spill size limit exceeded, or pickle error
                break

            total_spilled += weight
            count += 1

            memory = worker.monitor.get_process_memory()
            if total_spilled > need and memory > target:
                # Issue a GC to ensure that the evicted data is actually
                # freed from memory and taken into account by the monitor
                # before trying to evict even more data.
                self._throttled_gc.collect()
                memory = worker.monitor.get_process_memory()

            now = monotonic()

            # Spilling may potentially take multiple seconds; we may pass the pause
            # threshold in the meantime.
            if now - last_checked_for_pause > self.memory_monitor_interval:
                self._maybe_pause_or_unpause(worker, memory)
                last_checked_for_pause = now

            # Increase spilling aggressiveness when the fast buffer is filled with a lot
            # of small values. This artificially chokes the rest of the event loop -
            # namely, the reception of new data from other workers. While this is
            # somewhat of an ugly hack,  DO NOT tweak this without a thorough cycle of
            # stress testing. See: https://github.com/dask/distributed/issues/6110.
            if now - last_yielded > 0.5:
                await asyncio.sleep(0)
                last_yielded = monotonic()

        if count:
            logger.debug(
                "Moved %d tasks worth %s to disk",
                count,
                format_bytes(total_spilled),
            )

    def _to_dict(self, *, exclude: Container[str] = ()) -> dict:
        info = {
            k: v
            for k, v in self.__dict__.items()
            if not k.startswith("_") and k != "data" and k not in exclude
        }
        info["data"] = list(self.data)
        return info


class NannyMemoryManager:
    memory_limit: int | None
    memory_terminate_fraction: float | Literal[False]
    memory_monitor_interval: float | None
    _last_terminated_pid: int

    def __init__(
        self,
        nanny: Nanny,
        *,
        memory_limit: str | float = "auto",
    ):
        self.memory_limit = parse_memory_limit(memory_limit, nanny.nthreads)
        self.memory_terminate_fraction = dask.config.get(
            "distributed.worker.memory.terminate"
        )
        self.memory_monitor_interval = parse_timedelta(
            dask.config.get("distributed.worker.memory.monitor-interval"),
            default=False,
        )
        assert isinstance(self.memory_monitor_interval, (int, float))
        self._last_terminated_pid = -1

        if self.memory_limit and self.memory_terminate_fraction is not False:
            pc = PeriodicCallback(
                partial(self.memory_monitor, nanny),
                self.memory_monitor_interval * 1000,
            )
            nanny.periodic_callbacks["memory_monitor"] = pc

    def memory_monitor(self, nanny: Nanny) -> None:
        """Track worker's memory. Restart if it goes above terminate fraction."""
        if nanny.status != Status.running:
            return  # pragma: nocover
        if nanny.process is None or nanny.process.process is None:
            return  # pragma: nocover
        process = nanny.process.process
        try:
            proc = nanny._psutil_process
            memory = proc.memory_info().rss
        except (ProcessLookupError, psutil.NoSuchProcess, psutil.AccessDenied):
            return  # pragma: nocover

        if process.pid in (self._last_terminated_pid, None):
            # We already sent SIGTERM to the worker, but its handler is still running
            # since the previous iteration of the memory_monitor - for example, it
            # may be taking a long time deleting all the spilled data from disk.
            return
        self._last_terminated_pid = -1

        if memory / self.memory_limit > self.memory_terminate_fraction:
            logger.warning(
                f"Worker {nanny.worker_address} (pid={process.pid}) exceeded "
                f"{self.memory_terminate_fraction * 100:.0f}% memory budget. "
                "Restarting...",
            )
            self._last_terminated_pid = process.pid
            process.terminate()


def parse_memory_limit(
    memory_limit: str | float, nthreads: int, total_cores: int = CPU_COUNT
) -> int | None:
    if memory_limit is None:
        return None

    if memory_limit == "auto":
        memory_limit = int(system.MEMORY_LIMIT * min(1, nthreads / total_cores))
    with suppress(ValueError, TypeError):
        memory_limit = float(memory_limit)
        if isinstance(memory_limit, float) and memory_limit <= 1:
            memory_limit = int(memory_limit * system.MEMORY_LIMIT)

    if isinstance(memory_limit, str):
        memory_limit = parse_bytes(memory_limit)
    else:
        memory_limit = int(memory_limit)

    assert isinstance(memory_limit, int)
    if memory_limit == 0:
        return None
    return min(memory_limit, system.MEMORY_LIMIT)


def _parse_threshold(
    config_key: str,
    deprecated_param_name: str,
    deprecated_param_value: float | Literal[False] | None,
) -> float | Literal[False]:
    if deprecated_param_value is not None:
        warnings.warn(
            f"Parameter {deprecated_param_name} has been deprecated and will be "
            f"removed in a future version; please use dask config key {config_key} "
            "instead",
            FutureWarning,
        )
        return deprecated_param_value
    return dask.config.get(config_key)


def _warn_deprecated(w: Nanny | Worker, name: str) -> None:
    warnings.warn(
        f"The `{type(w).__name__}.{name}` attribute has been moved to "
        f"`{type(w).__name__}.memory_manager.{name}",
        FutureWarning,
    )


class DeprecatedMemoryManagerAttribute:
    name: str

    def __set_name__(self, owner: type, name: str) -> None:
        self.name = name

    def __get__(self, instance: Nanny | Worker | None, _):
        if instance is None:
            # This is triggered by Sphinx
            return None  # pragma: nocover
        _warn_deprecated(instance, self.name)
        return getattr(instance.memory_manager, self.name)

    def __set__(self, instance: Nanny | Worker, value) -> None:
        _warn_deprecated(instance, self.name)
        setattr(instance.memory_manager, self.name, value)


class DeprecatedMemoryMonitor:
    def __get__(self, instance: Nanny | Worker | None, owner):
        if instance is None:
            # This is triggered by Sphinx
            return None  # pragma: nocover
        _warn_deprecated(instance, "memory_monitor")
        return partial(instance.memory_manager.memory_monitor, instance)

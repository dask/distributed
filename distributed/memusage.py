"""Task-level memory profiling.

Usage:

    plugin = MemoryUsagePlugin("/tmp/memory_usage.csv")
    scheduler.add_plugin(plugin)
"""

from __future__ import absolute_import

import os
import csv
from time import sleep
from threading import Lock, Thread
from collections import defaultdict
from psutil import Process

from .diagnostics.plugin import SchedulerPlugin
from .client import Client


__all__ = ["MemoryUsagePlugin"]


def _process_memory():
    """Return process memory usage, in MB.

    We include memory used by subprocesses.
    """
    proc = Process(os.getpid())
    return sum([
        p.memory_info().rss / (1024 * 1024)
        for p in [proc] + list(proc.children(recursive=True))
    ])


class _WorkerMemory(object):
    """Track memory usage by each worker."""

    def __init__(self, scheduler_address):
        # This is a little silly: we're opening client to this process... not
        # sure how else to do it though.
        self._client = Client(scheduler_address)
        self._lock = Lock()
        self._worker_memory = defaultdict(list)

    def start(self):
        """Start the thread."""
        t = Thread(target=self._fetch_memory, name="WorkerMemory")
        t.setDaemon(True)
        t.start()

    def _fetch_memory(self):
        """Retrieve worker memory every 10ms."""
        while True:
            worker_to_mem = self._client.run(_process_memory)
            with self._lock:
                for worker, mem in worker_to_mem.items():
                    self._worker_memory[worker].append(mem)
            sleep(0.01)

    def memory_for_task(self, worker_address):
        """The worker finished its previous task.

        Return its memory usage and then reset it.
        """
        with self._lock:
            result = self._worker_memory[worker_address]
            del self._worker_memory[worker_address]
            return result


class MemoryUsagePlugin(SchedulerPlugin):
    """Record max and min memory usage for a task.

    Assumptions:

    * One task per process: each process has a single thread running a single
      task at a time.

    Limitations:

    * Statistical profiling at 10ms resolution.
    """
    def __init__(self, csv_path):
        SchedulerPlugin.__init__(self)
        f = open(os.path.join(csv_path), "w", buffering=1)
        self._csv = csv.writer(f)
        self._csv.writerow(["task_key", "min_memory_mb", "max_memory_mb"])
        self._worker_memory = None

    def reset(self, scheduler):
        if self._worker_memory is None:
            self._worker_memory = _WorkerMemory(scheduler.address)
            self._worker_memory.start()

    def transition(self, key, start, finish, *args, **kwargs):
        """Called by the Scheduler every time a task changes status."""
        # If the task finished, record its memory usage:
        if start == "processing" and finish in ("memory", "erred"):
            worker_address = kwargs["worker"]
            memory_usage = self._worker_memory.memory_for_task(worker_address)
            max_memory_usage = max(memory_usage)
            min_memory_usage = min(memory_usage)
            self._csv.writerow([key, min_memory_usage, max_memory_usage])

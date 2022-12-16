from __future__ import annotations

from prometheus_client import Counter, Gauge, Histogram, Summary

tasks = Gauge(
    "dask_worker_tasks",
    "Current number of tasks at worker",
    ["state"],
)

compute = Summary(
    "dask_worker_compute",
    "Number of tasks computed and total time spent computing tasks since the latest "
    "worker restart",
    unit="seconds",
)

latency = Histogram(
    "dask_worker_latency",
    "Worker<->Scheduler latency since the previous Prometheus poll",
    unit="seconds",
)

spill_bytes = Summary(
    "dask_worker_spill",
    "Total amount of memory and disk accesses caused by managed data "
    "since the latest worker restart",
    ["activity"],
    unit="bytes",
)
# cache hit ratio = memory_read / (memory_read + disk_read)
spill_bytes.labels("memory_read")
spill_bytes.labels("disk_read")
spill_bytes.labels("disk_write")

spill_time = Counter(
    "dask_worker_spill",
    "Total time spent spilling/unspilling since the latest worker restart",
    ["activity"],
    unit="seconds",
)
spill_time.labels("pickle")
spill_time.labels("disk_write")
spill_time.labels("disk_read")
spill_time.labels("unpickle")

# This duplicates spill_time; however the breakdown is different
evloop_blocked = Histogram(
    "dask_worker_event_loop_blocked",
    "Total time during which the worker's event loop was blocked "
    "by spill/unspill activity since the latest worker reset",
    ["cause"],
    unit="seconds",
)
evloop_blocked.labels("disk-write-target")
evloop_blocked.labels("disk-write-spill")
evloop_blocked.labels("disk-read-execute")
evloop_blocked.labels("disk-read-get-data")

transfers_count = Gauge(
    "dask_worker_transfers",
    "Current number of peer workers engaged in data transfers",
    ["direction"],
    unit="count",
)
transfers_bytes = Gauge(
    "dask_worker_transfers",
    "Current number of bytes worth of data transfers with peer workers",
    ["direction"],
    unit="bytes",
)
transfers_total = Summary(
    "dask_worker_transfers_total",
    "Incoming/outgoing data transfers since the latest worker restart",
    ["direction"],
    unit="bytes",
)
for metric in (transfers_count, transfers_bytes, transfers_total):
    metric.labels("incoming")
    metric.labels("outgoing")

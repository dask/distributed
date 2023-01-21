.. When modifying the contents of the first two sections of this page, please adjust the corresponding page in the dask.dask documentation accordingly.


Prometheus monitoring
=====================

Prometheus_ is a widely popular tool for monitoring and alerting a wide variety of
systems. A distributed cluster offers a number of Prometheus metrics if the
prometheus_client_ package is installed. The metrics are exposed in Prometheus'
text-based format at the ``/metrics`` endpoint on both schedulers and workers.


Available metrics
-----------------

Apart from the metrics exposed per default by the prometheus_client_, schedulers and
workers expose a number of Dask-specific metrics.


Scheduler metrics
^^^^^^^^^^^^^^^^^

The scheduler exposes the following metrics about itself:

dask_scheduler_clients
    Number of clients connected
dask_scheduler_desired_workers
    Number of workers scheduler needs for task graph
dask_scheduler_workers
    Number of workers known by scheduler
dask_scheduler_tasks
    Number of tasks known by scheduler
dask_scheduler_tasks_suspicious_total
    Total number of times a task has been marked suspicious
dask_scheduler_tasks_forgotten_total
    Total number of processed tasks no longer in memory and already removed from the
    scheduler job queue.

    .. note::
       Task groups on the scheduler which have all tasks in the forgotten state are not
       included.

dask_scheduler_tasks_time_spent_compute_seconds_total
    Total time (per prefix) spent computing tasks
dask_scheduler_tasks_time_spent_transfer_seconds_total
    Total time (per prefix) spent transferring
dask_scheduler_tasks_nbytes_bytes_total
    The total number of bytes that this task prefix has produced
dask_scheduler_prefix_state_totals_total
    Accumulated count of task prefix in each state
dask_scheduler_tick_count_total
    Total number of ticks observed since the server started
dask_scheduler_tick_duration_maximum_seconds
    Maximum tick duration observed since Prometheus last scraped metrics.
    If this is significantly higher than what's configured in
    ``distributed.admin.tick.interval`` (default: 20ms), it highlights a blocked event
    loop, which in turn hampers timely task execution and network comms.


Semaphore metrics
^^^^^^^^^^^^^^^^^

The following metrics about :class:`~distributed.Semaphore` objects are available on the
scheduler:

dask_semaphore_max_leases
    Maximum leases allowed per semaphore.

    .. note::
       This will be constant for each semaphore during its lifetime.

dask_semaphore_active_leases
    Amount of currently active leases per semaphore
dask_semaphore_pending_leases
    Amount of currently pending leases per semaphore
dask_semaphore_acquire_total
    Total number of leases acquired per semaphore
dask_semaphore_release_total
    Total number of leases released per semaphore

    .. note::
       If a semaphore is closed while there are still leases active, this count will not
       equal ``dask_semaphore_acquire_total`` after execution.

dask_semaphore_average_pending_lease_time_s
    Exponential moving average of the time it took to acquire a lease per semaphore

    .. note::
        This only includes time spent on scheduler side, it does not include time spent
        on communication.

    .. note::
       This average is calculated based on order of leases instead of time of lease
       acquisition.


Work-stealing metrics
^^^^^^^^^^^^^^^^^^^^^

If :doc:`work-stealing` is enabled, the scheduler exposes these metrics:

dask_stealing_request_count_total
    Total number of stealing requests
dask_stealing_request_cost_total
    Total cost of stealing requests


Worker metrics
^^^^^^^^^^^^^^

The worker exposes these metrics about itself:

dask_worker_tasks
    Number of tasks at worker
dask_worker_threads
    Number of worker threads
dask_worker_latency_seconds
    Latency of worker connection
dask_worker_memory_bytes
    Memory breakdown
dask_worker_transfer_incoming_bytes
    Total size of open data transfers from other workers
dask_worker_transfer_incoming_count
    Number of open data transfers from other workers
dask_worker_transfer_incoming_count_total
    Total number of data transfers from other workers since the worker was started
dask_worker_transfer_outgoing_bytes
    Size of open data transfers to other workers
dask_worker_transfer_outgoing_bytes_total
    Total size of open data transfers to other workers since the worker was started
dask_worker_transfer_outgoing_count
    Number of open data transfers to other workers
dask_worker_transfer_outgoing_count_total
    Total number of data transfers to other workers since the worker was started
dask_worker_concurrent_fetch_requests
    **Deprecated:** This metric has been renamed to
    ``dask_worker_transfer_incoming_count``.
dask_worker_tick_count_total
    Total number of ticks observed since the server started
dask_worker_tick_duration_maximum_seconds
    Maximum tick duration observed since Prometheus last scraped metrics.
    If this is significantly higher than what's configured in
    ``distributed.admin.tick.interval`` (default: 20ms), it highlights a blocked event
    loop, which in turn hampers timely task execution and network comms.
dask_worker_event_loop_blocked_time_max_seconds
    Maximum number of seconds the event loop was continuously frozen by known causes
    since Prometheus last scraped metrics. This metric is broken down by cause.

    .. note::
       This is highly correlated with ``dask_worker_tick_duration_maximum_seconds``,
       with the difference that the above also includes unknown causes. On the other
       hand, blockages shorter than the tick interval may not be (fully) captured by
       ``dask_worker_tick_duration_maximum_seconds``.

dask_worker_event_loop_blocked_time_seconds_total
    Total number of seconds the event loop was frozen by known causes since the worker
    was started
dask_worker_spill_bytes_total
    Total size of spilled/unspilled data since the worker was started;
    in other words, cumulative disk I/O that is attributable to spill activity.
    This includes a ``memory_read`` measure, which allows to derive cache hit ratio::

        cache hit ratio = memory_read / (memory_read + disk_read)

dask_worker_spill_count_total
    Total number of spilled/unspilled keys since the worker was started;
    in other words, cumulative disk accesses that are attributable to spill activity.
    This includes a ``memory_read`` measure, which allows to derive cache hit ratio::

        cache hit ratio = memory_read / (memory_read + disk_read)

dask_worker_spill_time_seconds_total
    Total amount of time that was spent spilling/unspilling since the worker was
    started.

    .. note::
       Total vs. total, this metric matches
       ``dask_worker_event_loop_blocked_time_seconds_total``. However, it's broken down
       by activity (pickle, write, read, unpickle), while the above is broken down by
       cause (target threshold, spill threshold, local task execution, remote task
       execution).

If the crick_ package is installed, the worker additionally exposes:

dask_worker_tick_duration_median_seconds
    Median tick duration at worker
dask_worker_task_duration_median_seconds
    Median task runtime at worker
dask_worker_transfer_bandwidth_median_bytes
    Bandwidth for transfer at worker


.. _Prometheus: https://prometheus.io
.. _prometheus_client: https://github.com/prometheus/client_python
.. _crick: https://github.com/dask/crick

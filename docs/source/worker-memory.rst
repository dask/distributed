Worker Memory Management
========================
For cluster-wide memory-management, see :doc:`memory`.

Workers are given a target memory limit to stay under with the
command line ``--memory-limit`` keyword or the ``memory_limit=`` Python
keyword argument, which sets the memory limit per worker processes launched
by dask worker ::

    $ dask worker tcp://scheduler:port --memory-limit=auto  # TOTAL_MEMORY * min(1, nthreads / total_nthreads)
    $ dask worker tcp://scheduler:port --memory-limit="4 GiB"  # four gigabytes per worker process.

Workers use a few different heuristics to keep memory use beneath this limit:

Spilling based on managed memory
--------------------------------
Every time the worker finishes a task, it estimates the size in bytes that the result
costs to keep in memory using the ``sizeof`` function.  This function defaults to
:func:`sys.getsizeof` for arbitrary objects, which uses the standard Python
``__sizeof__`` protocol, but also has special-cased implementations for common data
types like NumPy arrays and Pandas dataframes. The sum of the ``sizeof`` of all data
tracked by Dask is called :ref:`managed memory <memtypes>`.

When the managed memory exceeds 60% of the memory limit (*target threshold*), the worker
will begin to dump the least recently used data to disk. By default, it writes to the
OS's temporary directory (``/tmp`` in Linux); you can control this location
with the ``--local-directory`` keyword::

   $ dask worker tcp://scheduler:port --memory-limit="4 GiB" --local-directory /scratch

That data is still available and will be read back from disk when necessary. On the
diagnostic dashboard status page, disk I/O will show up in the task stream plot as
orange blocks. Additionally, the memory plot in the upper left will show a section of
the bar colored in grey.

Spilling based on process memory
--------------------------------
The approach above can fail for a few reasons:

1.  Custom objects may not report their memory size accurately
2.  User functions may take up more RAM than expected
3.  Significant amounts of data may accumulate in network I/O buffers

To address this, we periodically monitor the :ref:`process memory <memtypes>` of the
worker every 200 ms. If the system reported memory use is above 70% of the target memory
usage (*spill threshold*), then the worker will start dumping unused data to disk, even
if internal ``sizeof`` recording hasn't yet reached the normal 60% threshold. This
more aggressive spilling will continue until process memory falls below 60%.

Pause worker
------------
At 80% :ref:`process memory <memtypes>` load, the worker's thread pool will stop
starting computation on additional tasks in the worker's queue. This gives time for the
write-to-disk functionality to take effect even in the face of rapidly accumulating
data. Currently executing tasks continue to run. Additionally, data transfers to/from
other workers are throttled to a bare minimum.

Kill Worker
-----------
At 95% :ref:`process memory <memtypes>` load (*terminate threshold*), a worker's nanny
process will terminate it. Tasks will be cancelled mid-execution and rescheduled
elsewhere; all unique data on the worker will be lost and will need to be recomputed.
This is to avoid having our worker job being terminated by an external watchdog (like
Kubernetes, YARN, Mesos, SGE, etc..).  After termination, the nanny will restart the
worker in a fresh state.

Thresholds configuration
------------------------
These values can be configured by modifying the ``~/.config/dask/distributed.yaml``
file:

.. code-block:: yaml

   distributed:
     worker:
      # Fractions of worker process memory at which we take action to avoid memory
      # blowup. Set any of the values to False to turn off the behavior entirely.
       memory:
         target: 0.60     # fraction of managed memory where we start spilling to disk
         spill: 0.70      # fraction of process memory where we start spilling to disk
         pause: 0.80      # fraction of process memory at which we pause worker threads
         terminate: 0.95  # fraction of process memory at which we terminate the worker

Using the dashboard to monitor memory usage
-------------------------------------------
The dashboard (typically available on port 8787) shows a summary of the overall memory
usage on the cluster, as well as the individual usage on each worker. It provides
different memory readings:

.. _memtypes:

process
    Overall memory used by the worker process (RSS), as measured by the OS

managed
    Sum of the ``sizeof`` of all Dask data stored on the worker, excluding
    spilled data.

unmanaged
    Memory usage that Dask is not directly aware of. It is estimated by subtracting
    managed memory from the total process memory and typically includes:

    - The Python interpreter code, loaded modules, and global variables
    - Memory temporarily used by running tasks
    - Dereferenced Python objects that have not been garbage-collected yet
    - Unused memory that the Python memory allocator did not return to libc through
      `free`_ yet
    - Unused memory that the user-space libc `free`_ function did not release to the OS
      yet (see memory allocators below)
    - Memory fragmentation
    - Memory leaks

unmanaged recent
    Unmanaged memory that has appeared within the last 30 seconds. This is not included
    in the 'unmanaged' memory measure above. Ideally, this memory should be for the most
    part a temporary spike caused by tasks' heap use plus soon-to-be garbage collected
    objects.

    The time it takes for unmanaged memory to transition away from its "recent" state
    can be tweaked through the ``distributed.worker.memory.recent-to-old-time`` key in
    the ``~/.config/dask/distributed.yaml`` file. If your tasks typically run for longer
    than 30 seconds, it's recommended that you increase this setting accordingly.

    By default, :meth:`distributed.Client.rebalance` and
    :meth:`distributed.scheduler.Scheduler.rebalance` ignore unmanaged recent memory.
    This behaviour can also be tweaked using the Dask config - see the methods'
    documentation.

spilled
    managed memory that has been spilled to disk. This is not included in the 'managed'
    measure above. This measure reports the number of bytes actually spilled to disk,
    which may differ from the output of ``sizeof`` particularly in case of compression.

The sum of managed + unmanaged + unmanaged recent is equal by definition to the process
memory.

The color of the bars will change as a function of memory usage too:

blue
    The worker is operating as normal
orange
    The worker may be spilling data to disk
red
    The worker is paused or retiring
grey
    Data that has already been spilled to disk; this is in addition to process memory


.. _memtrim:

Memory not released back to the OS
----------------------------------
In many cases, high unmanaged memory usage or "memory leak" warnings on workers can be
misleading: a worker may not actually be using its memory for anything, but simply
hasn't returned that unused memory back to the operating system, and is hoarding it just
in case it needs the memory capacity again. This is not a bug in your code, nor in
Dask — it's actually normal behavior for all processes on Linux and MacOS, and is a
consequence of how the low-level memory allocator works (see below for details).

Because Dask makes decisions (spill-to-disk, pause, terminate,
:meth:`~distributed.Client.rebalance`) based on the worker's memory usage as reported by
the OS, and is unaware of how much of this memory is actually in use versus empty and
"hoarded", it can overestimate — sometimes significantly — how much memory the process
is using and think the worker is running out of memory when in fact it isn't.

More in detail: both the Linux and MacOS memory allocators try to avoid performing a
`brk`_ kernel call every time the application calls `free`_ by implementing a user-space
memory management system. Upon `free`_, memory can remain allocated in user space and
potentially reusable by the next `malloc`_ - which in turn won't require a kernel call
either. This is generally very desirable for C/C++ applications which have no memory
allocator of their own, as it can drastically boost performance at the cost of a larger
memory footprint. CPython however adds its own memory allocator on top, which reduces
the need for this additional abstraction (with caveats).

There are steps you can take to alleviate situations where worker memory is not released
back to the OS. These steps are discussed in the following sections.

Manually trim memory
~~~~~~~~~~~~~~~~~~~~
*Linux workers only*

It is possible to forcefully release allocated but unutilized memory as follows:

.. code-block:: python

    import ctypes

    def trim_memory() -> int:
        libc = ctypes.CDLL("libc.so.6")
        return libc.malloc_trim(0)

    client.run(trim_memory)

This should be only used as a one-off debugging experiment. Watch the dashboard while
running the above code. If unmanaged worker memory (on the "Bytes stored" plot)
decreases significantly after calling ``client.run(trim_memory)``, then move on to the
next section. Otherwise, you likely do have a memory leak.

Note that you should only run this `malloc_trim`_ if you are using the default glibc
memory allocator. When using a custom allocator such as `jemalloc`_ (see below), this
could cause unexpected behavior including segfaults. (If you don't know what this means,
you're probably using the default glibc allocator and are safe to run this).

Automatically trim memory
~~~~~~~~~~~~~~~~~~~~~~~~~
*Linux workers only*

To aggressively and automatically trim the memory in a production environment, you
should instead set the environment variable ``MALLOC_TRIM_THRESHOLD_`` (note the final
underscore) to 0 or a low number; see the `mallopt`_ man page for details. Reducing
this value will increase the number of syscalls, and as a consequence may degrade
performance.

.. note::
   The variable must be set before starting the ``dask worker`` process.

.. note::
   If using a :ref:`nanny`, the ``MALLOC_TRIM_THRESHOLD_`` environment variable
   will automatically be set to ``65536`` for the worker process which the nanny is
   monitoring. You can modify this behavior using the ``distributed.nanny.environ``
   configuration value.

jemalloc
~~~~~~~~
*Linux and MacOS workers*

Alternatively to the above, you may experiment with the `jemalloc`_ memory allocator, as
follows:

On Linux:

.. code-block:: bash

    conda install jemalloc
    LD_PRELOAD=$CONDA_PREFIX/lib/libjemalloc.so dask worker <...>

On macOS:

.. code-block:: bash

    conda install jemalloc
    DYLD_INSERT_LIBRARIES=$CONDA_PREFIX/lib/libjemalloc.dylib dask worker <...>

Alternatively on macOS, install globally with `homebrew`_:

.. code-block:: bash

    brew install jemalloc
    DYLD_INSERT_LIBRARIES=$(brew --prefix jemalloc)/lib/libjemalloc.dylib dask worker <...>

`jemalloc`_ offers a wealth of configuration settings; please refer to its
documentation.

Ignore process memory
~~~~~~~~~~~~~~~~~~~~~
If all else fails, you may want to stop Dask from using memory metrics from the OS (RSS)
in its decision-making:

.. code-block:: yaml

   distributed:
     worker:
       memory:
         rebalance:
           measure: managed_in_memory
         spill: false
         pause: false
         terminate: false

This of course will be problematic if you have a genuine issue with unmanaged memory,
e.g. memory leaks and/or suffer from heavy fragmentation.


User-defined managed memory containers
--------------------------------------
.. warning::
   This feature is intended for advanced users only; the built-in container for managed
   memory should fit the needs of most. If you're looking to dynamically spill CUDA
   device memory into host memory, you should use `dask-cuda`_.

The design described in the sections above stores data in the worker's RAM, with
automatic spilling to disk when the ``target`` or ``spill`` thresholds are passed.
If one desires a different behaviour, a ``data=`` parameter can be passed when
initializing the :class:`~distributed.worker.Worker` or
:class:`~distributed.nanny.Nanny`.
This optional parameter accepts any of the following values:

- an instance of ``MutableMapping[str, Any]``
- a callable which returns a ``MutableMapping[str, Any]``
- a tuple of

  - callable which returns a ``MutableMapping[str, Any]``
  - dict of keyword arguments to the callable

Doing so causes the Worker to ignore both the ``target`` and the ``spill`` thresholds.
However, if the object also supports the following duck-type API in addition to the
MutableMapping API, the ``spill`` threshold will remain active:

.. autoclass:: distributed.spill.ManualEvictProto
   :members:


.. _malloc: https://www.man7.org/linux/man-pages/man3/malloc.3.html
.. _free: https://www.man7.org/linux/man-pages/man3/free.3.html
.. _mallopt: https://man7.org/linux/man-pages/man3/mallopt.3.html
.. _malloc_trim: https://man7.org/linux/man-pages/man3/malloc_trim.3.html
.. _brk: https://www.man7.org/linux/man-pages/man2/brk.2.html
.. _jemalloc: http://jemalloc.net
.. _homebrew: https://brew.sh/
.. _dask-cuda: https://docs.rapids.ai/api/dask-cuda/stable/index.html

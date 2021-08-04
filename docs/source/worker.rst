Worker
======

Overview
--------

Workers provide two functions:

1.  Compute tasks as directed by the scheduler
2.  Store and serve computed results to other workers or clients

Each worker contains a ThreadPool that it uses to evaluate tasks as requested
by the scheduler.  It stores the results of these tasks locally and serves them
to other workers or clients on demand.  If the worker is asked to evaluate a
task for which it does not have all of the necessary data then it will reach
out to its peer workers to gather the necessary dependencies.

A typical conversation between a scheduler and two workers Alice and Bob may
look like the following::

   Scheduler -> Alice:  Compute ``x <- add(1, 2)``!
   Alice -> Scheduler:  I've computed x and am holding on to it!

   Scheduler -> Bob:    Compute ``y <- add(x, 10)``!
                        You will need x.  Alice has x.
   Bob -> Alice:        Please send me x.
   Alice -> Bob:        Sure.  x is 3!
   Bob -> Scheduler:    I've computed y and am holding on to it!


Storing Data
------------

Data is stored locally in a dictionary in the ``.data`` attribute that
maps keys to the results of function calls.

.. code-block:: python

   >>> worker.data
   {'x': 3,
    'y': 13,
    ...
    '(df, 0)': pd.DataFrame(...),
    ...
    }

This ``.data`` attribute is a ``MutableMapping`` that is typically a
combination of in-memory and on-disk storage with an LRU policy to move data
between them.


Thread Pool
-----------

Each worker sends computations to a thread in a
:class:`concurrent.futures.ThreadPoolExecutor`
for computation.  These computations occur in the same process as the Worker
communication server so that they can access and share data efficiently between
each other.  For the purposes of data locality all threads within a worker are
considered the same worker.

If your computations are mostly numeric in nature (for example NumPy and Pandas
computations) and release the GIL entirely then it is advisable to run
``dask-worker`` processes with many threads and one process.  This reduces
communication costs and generally simplifies deployment.

If your computations are mostly Python code and don't release the GIL then it
is advisable to run ``dask-worker`` processes with many processes and one
thread per process::

   $ dask-worker scheduler:8786 --nprocs 8 --nthreads 1

This will launch 8 worker processes each of which has its own
ThreadPoolExecutor of size 1.

If your computations are external to Python and long-running and don't release
the GIL then beware that while the computation is running the worker process
will not be able to communicate to other workers or to the scheduler.  This
situation should be avoided.  If you don't link in your own custom C/Fortran
code then this topic probably doesn't apply.


Command Line tool
-----------------

Use the ``dask-worker`` command line tool to start an individual worker. For
more details on the command line options, please have a look at the
`command line tools documentation
<https://docs.dask.org/en/latest/setup/cli.html#dask-worker>`_.


Internal Scheduling
-------------------

Internally tasks that come to the scheduler proceed through the following
pipeline as :py:class:`distributed.worker.TaskState` objects.  Tasks which
follow this path have a :py:attr:`distributed.worker.TaskState.runspec` defined
which instructs the worker how to execute them.

.. image:: images/worker-task-state.svg
    :alt: Dask worker task states

Data dependencies are also represented as
:py:class:`distributed.worker.TaskState` objects and follow a simpler path
through the execution pipeline.  These tasks do not have a
:py:attr:`distributed.worker.TaskState.runspec` defined and instead contain a
listing of workers to collect their result from.


.. image:: images/worker-dep-state.svg
    :alt: Dask worker dependency states

As tasks arrive they are prioritized and put into a heap.  They are then taken
from this heap in turn to have any remote dependencies collected.  For each
dependency we select a worker at random that has that data and collect the
dependency from that worker.  To improve bandwidth we opportunistically gather
other dependencies of other tasks that are known to be on that worker, up to a
maximum of 200MB of data (too little data and bandwidth suffers, too much data
and responsiveness suffers).  We use a fixed number of connections (around
10-50) so as to avoid overly-fragmenting our network bandwidth. In the event
that the network comms between two workers are saturated, a dependency task may
cycle between ``fetch`` and ``flight`` until it is successfully collected.

After all dependencies for a task are in memory we transition the task to the
ready state and put the task again into a heap of tasks that are ready to run.

We collect from this heap and put the task into a thread from a local thread
pool to execute.

Optionally, this task may identify itself as a long-running task (see
:doc:`Tasks launching tasks <task-launch>`), at which point it secedes from the
thread pool.

A task either errs or its result is put into memory.  In either case a response
is sent back to the scheduler.

Tasks slated for execution and tasks marked for collection from other workers
must follow their respective transition paths as defined above. The only
exceptions to this are when:

* A task is `stolen <work-stealing>`_, in which case a task which might have
  been collected will instead be executed on the thieving worker
* Scheduler intercession, in which the scheduler reassigns a task that was
  previously assigned to a separate worker to a new worker.  This most commonly
  occurs when a `worker dies <killed>`_ during computation.


.. _memman:

Memory Management
-----------------

Workers are given a target memory limit to stay under with the
command line ``--memory-limit`` keyword or the ``memory_limit=`` Python
keyword argument, which sets the memory limit per worker processes launched
by dask-worker ::

    $ dask-worker tcp://scheduler:port --memory-limit=auto  # TOTAL_MEMORY * min(1, nthreads / total_nthreads)
    $ dask-worker tcp://scheduler:port --memory-limit="4 GiB"  # four gigabytes per worker process.

Workers use a few different heuristics to keep memory use beneath this limit:

1.  At 60% of memory load (as estimated by ``sizeof``), spill least recently used data
    to disk
2.  At 70% of memory load (as reported by the OS), spill least recently used data to
    disk regardless of what is reported by ``sizeof``; this accounts for memory used by
    the python interpreter, modules, global variables, memory leaks, etc.
3.  At 80% of memory load (as reported by the OS), stop accepting new work on local
    thread pool
4.  At 95% of memory load (as reported by the OS), terminate and restart the worker

These values can be configured by modifying the ``~/.config/dask/distributed.yaml``
file:

.. code-block:: yaml

   distributed:
     worker:
       # Fractions of worker memory at which we take action to avoid memory blowup
       # Set any of the lower three values to False to turn off the behavior entirely
       memory:
         target: 0.60  # target fraction to stay below
         spill: 0.70  # fraction at which we spill to disk
         pause: 0.80  # fraction at which we pause worker threads
         terminate: 0.95  # fraction at which we terminate the worker


Spill data to disk
~~~~~~~~~~~~~~~~~~

Every time the worker finishes a task it estimates the size in bytes that the
result costs to keep in memory using the ``sizeof`` function.  This function
defaults to :func:`sys.getsizeof` for arbitrary objects, which uses the standard
Python ``__sizeof__`` protocol, but also has special-cased implementations for
common data types like NumPy arrays and Pandas dataframes.

When the sum of the number of bytes of the data in memory exceeds 60% of the
memory limit, the worker will begin to dump the least recently used data
to disk.  You can control this location with the ``--local-directory``
keyword.::

   $ dask-worker tcp://scheduler:port --memory-limit="4 GiB" --local-directory /scratch

That data is still available and will be read back from disk when necessary.
On the diagnostic dashboard status page disk I/O will show up in the task
stream plot as orange blocks. Additionally, the memory plot in the upper left
will become yellow and then red.


Monitor process memory load
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The approach above can fail for a few reasons:

1.  Custom objects may not report their memory size accurately
2.  User functions may take up more RAM than expected
3.  Significant amounts of data may accumulate in network I/O buffers

To address this we periodically monitor the memory of the worker process every
200 ms.  If the system reported memory use is above 70% of the target memory
usage then the worker will start dumping unused data to disk, even if internal
``sizeof`` recording hasn't yet reached the normal 60% limit.


Halt worker threads
~~~~~~~~~~~~~~~~~~~

At 80% load, the worker's thread pool will stop accepting new tasks.  This
gives time for the write-to-disk functionality to take effect even in the face
of rapidly accumulating data.


Kill Worker
~~~~~~~~~~~

At 95% memory load, a worker's nanny process will terminate it. This is to
avoid having our worker job being terminated by an external job scheduler (like
YARN, Mesos, SGE, etc..).  After termination, the nanny will restart the worker
in a fresh state.


Using the dashboard to monitor memory usage
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
The dashboard (typically available on port 8787) shows a summary of the overall memory
usage on the cluster, as well as the individual usage on each worker. It provides
different memory readings:

process
    Overall memory used by the worker process (RSS), as measured by the OS

managed
    This is the sum of the ``sizeof`` of all dask data stored on the worker, excluding
    spilled data.

unmanaged
    This is the memory usage that dask is not directly aware of. It is estimated by
    subtracting managed memory from the total process memory and typically includes:

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
    This behaviour can also be tweaked using the dask config - see the methods'
    documentation.

spilled
    managed memory that has been spilled to disk. This is not included in the 'managed'
    measure above.

The sum of managed + unmanaged + unmanaged recent is equal by definition to the process
memory.


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
   The variable must be set before starting the ``dask-worker`` process.

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
    LD_PRELOAD=$CONDA_PREFIX/lib/libjemalloc.so dask-worker <...>

On macOS:

.. code-block:: bash

    conda install jemalloc
    DYLD_INSERT_LIBRARIES=$CONDA_PREFIX/lib/libjemalloc.dylib dask-worker <...>

Alternatively on macOS, install globally with `homebrew`_:

.. code-block:: bash

    brew install jemalloc
    DYLD_INSERT_LIBRARIES=$(brew --prefix jemalloc)/lib/libjemalloc.dylib dask-worker <...>

`jemalloc`_ offers a wealth of configuration settings; please refer to its
documentation.

Ignore process memory
~~~~~~~~~~~~~~~~~~~~~
If all else fails, you may want to stop dask from using memory metrics from the OS (RSS)
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


.. _nanny:

Nanny
-----

Dask workers are by default launched, monitored, and managed by a small Nanny
process.

.. autoclass:: distributed.nanny.Nanny


API Documentation
-----------------

.. autoclass:: distributed.worker.TaskState
.. autoclass:: distributed.worker.Worker


.. _malloc: https://www.man7.org/linux/man-pages/man3/malloc.3.html
.. _free: https://www.man7.org/linux/man-pages/man3/free.3.html
.. _mallopt: https://man7.org/linux/man-pages/man3/mallopt.3.html
.. _malloc_trim: https://man7.org/linux/man-pages/man3/malloc_trim.3.html
.. _brk: https://www.man7.org/linux/man-pages/man2/brk.2.html
.. _jemalloc: http://jemalloc.net
.. _homebrew: https://brew.sh/
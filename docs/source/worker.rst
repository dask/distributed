Worker
======

Workers provide two functions:

1.  Workers compute tasks as directed by the scheduler
2.  Workers hold and serve computed results, both for each other and for the
    clients

Each worker contains a ThreadPool that it uses to evaluate tasks as requested
by the scheduler.  It stores the results of these tasks locally and serves them
to the scheduler or to other workers on demand.  If the worker is asked to
evaluate a task for which it does not have all of the necessary data then it
will reach out to its peer workers to gather the necessary dependencies.

A typical conversation between a scheduler and two workers Alice and Bob may
look like the following::

   Scheduler -> Alice:  Compute ``x <- add(1, 2)``!
   Alice -> Scheudler:  I've computed x and am holding on to it!

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

Spill Excess Data to Disk
-------------------------

Some workloads may produce more data at one time than there is available RAM on
the cluster.  In these cases Workers may choose to write excess values to disk.
This causes some performance degradation because writing to and reading from
disk is generally slower than accessing memory, but is better than running out
of memory entirely, which can cause the system to halt.

If the ``dask-worker --spill-bytes=NBYTES`` keyword is set during
initialization then the worker will store at most NBYTES of data (as measured
with ``sizeof``) in memory.  After that it will start storing least recently
used (LRU) data in a temporary directory.   Workers serialize data for writing
to disk with the same system used to write data on the wire, a combination of
``pickle`` and the default compressor.

Now whenever new data comes in it will push out old data until at most NBYTES
of data is in RAM.  If an old value is requested it will be read from disk,
possibly pushing other values down.

It is still possible to run out of RAM on a worker.  Here are a few possible
issues:

1.  The objects being stored take up more RAM than is stated with the
    `__sizeof__
    protocol<https://docs.python.org/3/library/sys.html#sys.getsizeof>`_.
    If you use custom classes then we encourage adding a faithful
    ``__sizeof__`` method to your class that returns an accurate accounting of
    the bytes used.
2.  Computations and communications may take up additional RAM not accounted
    for.  It is wise to have a suitable buffer of memory that can handle your
    most expensive function RAM-wise running as many times as there are active
    threads on the machine.
3.  It is possible to misjudge the amount of RAM on the machine.  Using the
    ``--spill-bytes=auto`` heuristic sets the value to 75% of the return value
    of ``psutil.virtual_memory().total``.


Thread Pool
-----------

Each worker sends computations to a thread in a
`concurrent.futures.ThreadPoolExecutor<https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ThreadPoolExecutor>`_
for computation


.. autoclass:: distributed.worker.Worker

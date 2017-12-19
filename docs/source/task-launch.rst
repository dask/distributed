Launch Tasks from Tasks
=======================

Sometimes it is convenient to launch tasks from other tasks.
For example you may not know what computations to run until you have the
results of some initial computations.

Motivating example
------------------

We want to download one piece of data and turn it into a list.  Then we want to
submit one task for every element of that list.  We don't know how long the
list will be until we have the data.

So we send off our original ``download_and_convert_to_list`` function, which
downloads the data and converts it to a list on one of our worker machines:

.. code-block:: python

   future = client.submit(download_and_convert_to_list, uri)

But now we need to submit new tasks for individual parts of this data.  We have
three options.

1.  Gather the data back to the local process and then submit new jobs from the
    local process
2.  Gather only enough information about the data back to the local process and
    submit jobs from the local process
3.  Submit a task to the cluster that will submit other tasks directly from
    that worker

Gather the data locally
-----------------------

If the data is not large then we can bring it back to the client to perform the
necessary logic on our local machine:

.. code-block:: python

   >>> data = future.result()                  # gather data to local process
   >>> data                                    # data is a list
   [...]

   >>> futures = e.map(process_element, data)  # submit new tasks on data
   >>> analysis = e.submit(aggregate, futures) # submit final aggregation task

This is straightforward and, if ``data`` is small then it is probably the
simplest, and therefore correct choice.  However, if ``data`` is large then we
have to choose another option.


Submit tasks from client
------------------------

We can run small functions on our remote data to determine enough to submit the
right kinds of tasks.  In the following example we compute the ``len`` function
on ``data`` remotely and then break up data into its various elements.

.. code-block:: python

   >>> n = client.submit(len, data)            # compute number of elements
   >>> n = n.result()                          # gather n (small) locally

   >>> from operator import getitem
   >>> elements = [client.submit(getitem, data, i) for i in range(n)]  # split data

   >>> futures = client.map(process_element, elements)
   >>> analysis = client.submit(aggregate, futures)

We compute the length remotely, gather back this very small result, and then
use it to submit more tasks to break up the data and process on the cluster.
This is more complex because we had to go back and forth a couple of times
between the cluster and the local process, but the data moved was very small,
and so this only added a few milliseconds to our total processing time.

Extended Example
~~~~~~~~~~~~~~~~

Computing the Fibonacci numbers creates involves a recursive function. When the
function is run, it calls itself using values it computed. We will use this as
an example throughout this documentation to illustrate different techniques of
submitting tasks from tasks.

.. code-block:: python

   In [1]: def fib(n):
      ...:     if n < 2:
      ...:         return n
      ...:     a = fib(n - 1)
      ...:     b = fib(n - 2)
      ...:     return a + b

   In [2]: fib(10)
   Out[2]: 55

We will use this example to show the different interfaces.

Submit tasks from worker
------------------------

*Note: this interface is new and experimental.  It may be changed without
warning in future versions.*

We can submit tasks from other tasks.  This allows us to make decisions while
on worker nodes.

To submit new tasks from a worker that worker must first create a new client
object that connects to the scheduler. There are three options for this:

1. ``dask.delayed`` and ``dask.compute``
2. ``get_client`` with ``secede`` and ``rejoin``
3. ``worker_client``

.. _Scheduling Policies: https://distributed.readthedocs.io/en/latest/scheduling-policies.html
.. _Scheduling State: https://distributed.readthedocs.io/en/latest/scheduling-state.html
.. _The compute function: https://dask.pydata.org/en/latest/scheduler-overview.html#the-compute-function

``dask.delayed``
~~~~~~~~~~~~~~~~

``dask.delayed`` behaves as normal: it submits the functions to the graph,
optimizes for less bandwidth/computation and gathers the results.
For more detail, see `dask.delayed`_.

.. code-block:: python

    In [1]: from distributed import Client
    In [2]: from dask import delayed, compute

    In [3]: # these features require the newer dask.distributed scheduler
    In [4]: client = Client()

    In [4]: @delayed
       ...: def fib(n):
       ...:     if n < 2:
       ...:         return n
       ...:     # We can use dask.delayed and dask.compute to launch
       ...:     # computation from within tasks
       ...:     a = fib(n-1)  # these calls are delayed
       ...:     b = fib(n-2)
       ...:     a, b = compute(a, b)  # execute both in parallel
       ...:     return a + b

    In [5]: fib(10).compute()
    Out[5]: 55

.. _dask.delayed: https://dask.pydata.org/en/latest/delayed.html

``get_client``
~~~~~~~~~~~~~~

``distributed.get_client`` allows individual workers to access the ``client``
that the scheduler uses:

.. code-block:: python

    In [1]: from distributed import Client, get_client, secede, rejoin

    In [2]: def fib(n):
       ...:     if n < 2:
       ...:         return n
       ...:     client = get_client()
       ...:     a_future = client.submit(fib, n - 1)
       ...:     b_future = client.submit(fib, n - 2)
       ...:     a, b = client.gather([a_future, b_future])
       ...:     return a + b

    In [3]: client = Client()

    In [4]: future = client.submit(fib, 10)

    In [5]: future.result()
    Out[5]: 55

This can deadlock the scheduler if too many nodes request jobs at once. To
correct that, we can use ``secede`` and ``rejoin``. These functions will remove
and rejoin the current node from the cluster respectively.

.. code-block:: python

    In [1]: def fib(n):
       ...:     if n < 2:
       ...:         return n
       ...:     client = get_client()
       ...:     a_future = client.submit(fib, n - 1)
       ...:     b_future = client.submit(fib, n - 2)
       ...:     secede()
       ...:     a, b = client.gather([a_future, b_future])
       ...:     rejoin()
       ...:     return a + b

Using ``secede`` can possibly have the worker move to a different node if it's
long running. Transferring state between nodes can be costly time-wise if
there's a lot of internal state (e.g., with a large matrix).

``worker_client``
~~~~~~~~~~~~~~~~~~~~~~

``worker_client`` is a convenience function to do this for you so that you
don't have to pass around connection information.  However you must use this
function ``worker_client`` as a context manager to ensure proper cleanup on the
worker.

.. code-block:: python

    In [1]: from distributed import worker_client

    In [2]: import time

    In [3]: def fib(n):
       ...:     time.sleep(1)  # to simulate a long running function
       ...:     if n < 2:
       ...:         return n
       ...:      with worker_client() as client:
       ...:          a_future = client.compute(fib, n - 1)
       ...:          b_future = client.compute(fib, n - 2)
       ...:          a, b = client.gather([a_future, b_future])
       ...:      return a + b

    In [4]: client = Client()

    In [5]: future = client.submit(fib, 10)

    In [6]: future.result()
    Out[6]: 55

This allows you to spawn tasks that themselves act as potentially long-running
clients, managing their own independent workloads.

Tasks that invoke ``worker_client`` are conservatively assumed to be *long
running*.  They can take a long time blocking, waiting for other tasks to
finish, gathering results, etc. In order to avoid having them take up
processing slots the following actions occur whenever a task invokes
``worker_client``.

1.  The thread on the worker running this function *secedes* from the thread
    pool and goes off on its own.  This allows the thread pool to populate that
    slot with a new thread and continue processing additional tasks without
    counting this long running task against its normal quota.
2.  The Worker sends a message back to the scheduler temporarily increasing its
    allowed number of tasks by one.  This likewise lets the scheduler allocate
    more tasks to this worker, not counting this long running task against it.

Because of this behavior you can happily launch long running control tasks that
manage worker-side clients happily, without fear of deadlocking the cluster.

Establishing a connection to the scheduler takes on the order of 10—20 ms and
so it is wise for computations that use this feature to be at least a few times
longer in duration than this.

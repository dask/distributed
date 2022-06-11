Managing Memory
===============

Dask.distributed stores the results of tasks in the distributed memory of the
worker nodes. The central scheduler tracks all data on the cluster and
determines when data should be freed. Completed results are usually cleared
from memory as quickly as possible in order to make room for more computation.
The result of a task is kept in memory if either of the following conditions
hold:

1. A client holds a future pointing to this task. The data should stay in RAM so that
   the client can gather the data on demand.
2. The task is necessary for ongoing computations that are working to produce
   the final results pointed to by futures. These tasks will be removed once
   no ongoing tasks require them.

When users hold Future objects or persisted collections (which contain many such Futures
inside their dask graph, typically accessible through their ``.dask`` attribute) they
pin those results to active memory. When the user deletes futures or collections from
their local Python process, the scheduler removes the associated data from distributed
RAM. Because of this relationship, distributed memory reflects the state of local
memory. A user may free distributed memory on the cluster by deleting persisted
collections in the local session.


Creating Futures
----------------

The following functions produce Futures:

.. currentmodule:: distributed.client

.. autosummary::
   Client.submit
   Client.map
   Client.compute
   Client.persist
   Client.scatter

The ``submit`` and ``map`` methods handle raw Python functions. The ``compute`` and
``persist`` methods handle Dask collections like arrays, bags, delayed values, and
dataframes. The ``scatter`` method sends data directly from the local process.


Persisting Collections
----------------------

Calls to ``Client.compute`` or ``Client.persist`` submit task graphs to the
cluster and return ``Future`` objects that point to particular output tasks.

Compute returns a single future per input; persist returns a copy of the
collection with each block or partition replaced by a single future. In short,
use ``persist`` to keep full collection on the cluster and use ``compute`` when
you want a small result as a single future.

Persist is more common and is often used as follows with collections:

.. code-block:: python

   >>> # Construct dataframe; no work happens
   >>> df = dd.read_csv(...)
   >>> df = df[df.x > 0]
   >>> df = df.assign(z = df.x + df.y)

   >>> # Pin data in distributed RAM; this triggers computation
   >>> df = client.persist(df)

   >>> # continue operating on df

*Note for Spark users: this differs from what you're accustomed to. Persist is
an immediate action. However, you'll get control back immediately as
computation occurs in the background.*

In this example we build a computation by parsing CSV data, filtering rows, and
then adding a new column. Up until this point all work is lazy; we've just
built up a recipe to perform the work as a graph in the ``df`` object.

When we call ``df = client.persist(df)``, we cut the graph off the ``df`` object,
send it up to the scheduler, receive ``Future`` objects in return and create a
new dataframe with a very shallow graph that points directly to these futures.
This happens more or less immediately (as long as it takes to serialize and
send the graph) and we can continue working on our new ``df`` object while the
cluster works to evaluate the graph in the background.


Difference with dask.compute
----------------------------
If a Client is set as the default scheduler, then ``dask.compute``, ``dask.persist``,
and the ``.compute`` and ``.persist`` methods of all dask collections will invoke
``Client.compute`` and ``Client.persist`` under the hood, unless a different scheduler
is explicitly specified. This happens by default whenever a new Client is created,
unless the user explicitly passes the ``set_as_default=False`` parameter to it.

There is however a difference: the operation ``client.compute(df)`` is asynchronous and
so differs from the traditional ``df.compute()`` method or ``dask.compute`` function,
which block until a result is available, do not persist any data on the cluster, and
bring the entire result back to the local machine, so it is unwise to use them on large
datasets, but can be very convenient for smaller results, particularly because they
return concrete results in a way that most other tools expect.

In other words, ``df.compute()`` is equivalent to ``client.compute(df).result()``.

Typically we use asynchronous methods like ``client.persist`` to set up large
collections and then use ``df.compute()`` for fast analyses.

.. code-block:: python

   >>> # df.compute()  # This is bad and would likely flood local memory
   >>> df = client.persist(df)    # This is good and asynchronously pins df
   >>> df.x.sum().compute()  # This is good because the result is small
   >>> future = client.compute(df.x.sum())  # This is also good but less intuitive


Clearing data
-------------

We remove data from distributed RAM by removing the collection from our local
process. Remote data is removed once all Futures pointing to that data are
removed from all client machines.

.. code-block:: python

   >>> del df  # Deleting local data often deletes remote data

If this is the only copy then this will likely trigger the cluster to delete
the data as well.

However, if we have multiple copies or other collections based on this one, then
we'll have to delete them all.

.. code-block:: python

   >>> df2 = df[df.x < 10]
   >>> del df  # would not delete data, because df2 still tracks the futures


Aggressively Clearing Data
--------------------------

To definitely remove a computation and all computations that depend on it you
can always ``cancel`` the futures/collection.

.. code-block:: python

   >>> client.cancel(df)  # kills df, df2, and every other dependent computation

Alternatively, if you want a clean slate, you can restart the cluster. This
clears all state and does a hard restart of all worker processes. It generally
completes in a few seconds.

.. code-block:: python

   >>> client.restart()


Client references
-----------------
Futures live on the cluster for as long as at least one Client holds a reference to
them. When the last Client holding a reference is shut down or crashes, then everything
that was referenced exclusively by it is pruned from the cluster. This is generally
desirable to prevent unclean client shutdowns from polluting the memory of long-running
clusters.

It is possible to prevent this behaviour in order to improve resilience or simply to be
able to shut down one's laptop while a computation runs overnight. See:

.. autosummary::
   distributed.Client.publish_dataset
   distributed.fire_and_forget


Resilience
----------

Results are not intentionally copied unless necessary for computations on other
worker nodes. Resilience is achieved through recomputation by maintaining the
provenance of any result. If a worker node goes down, the scheduler is able to
recompute all of its results. The complete graph for any desired Future is
maintained until no references to that future exist.

For more information see :doc:`Resilience <resilience>`.


Advanced techniques
-------------------

At first the result of a task is not intentionally copied, but only persists on
the node where it was originally computed or scattered. However, a result may be
copied to another worker node in the course of normal computation if that
result is required by another task that is intended to by run by a different
worker. This occurs if a task requires two pieces of data on different
machines (at least one must move) or through work stealing. In these cases it
is the policy for the second machine to maintain its redundant copy of the data.
This helps to organically spread around data that is in high demand.

However, advanced users may want to control the location, replication, and
balancing of data more directly throughout the cluster. They may know ahead of
time that certain data should be broadcast throughout the network or that their
data has become particularly imbalanced, or that they want certain pieces of
data to live on certain parts of their network. These considerations are not
usually necessary.

.. currentmodule:: distributed.client

.. autosummary::
   Client.rebalance
   Client.replicate
   Client.scatter


Worker memory management
------------------------
Memory usage can be optimized by configuring worker-side :doc:`worker-memory`.

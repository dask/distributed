.. _api:

API
===

.. currentmodule:: distributed

**Client**

The dask distributed client is the main entry point to working with a dask cluster. It offers
its own API for submitting and dealing with work, as well as getting information about the
cluster it is connected to. Simply creating a client may be all that a users needs to do,
since it will implicitly become the default engine for any dask ``compute()`` calls.

There are three main ways in which ``Client`` can be called:
- with the address of an existing scheduler, or scheduler information file; in this case,
  the cluster has been configured elsewhere (e.g., command line, helm)
- with a cluster instance which has been created in the current python session; in this case,
  you have control over the cluster object (which may point to Kubernetes, for example), and
  you may call methods on it to, for instance, scale.
- without any connection argument; in this case, a :class:`distributed.LocalCluster` will be
  created for you automatically, with defaults that are reasonable for a one-machine cluster.
  You can still override these default by passing any of the arguments accepted by ``LocalCluster``,
  see its own documentation. The created cluster instance will be available as the attribute
  ``.cluster``. Many dask examples have ``client = Client()`` a the top of a script/notebook
  for this reason.

The client connects to and submits computation to a Dask cluster (such as a :class:`distributed.LocalCluster`)

.. autosummary::
   Client

.. autoautosummary:: distributed.Client
   :methods:

.. currentmodule:: distributed

.. autosummary::
   worker_client
   get_worker
   get_client
   secede
   rejoin
   Reschedule

.. currentmodule:: distributed.recreate_tasks

.. autosummary::
   ReplayTaskClient.recreate_task_locally
   ReplayTaskClient.recreate_error_locally

.. currentmodule:: distributed


**Future**

.. autosummary::
   Future

.. autoautosummary:: distributed.Future
   :methods:

**Synchronization**

.. currentmodule:: distributed

.. autosummary::
   Event
   Lock
   MultiLock
   Semaphore
   Queue
   Variable


**Other**

.. autosummary::
   as_completed
   distributed.diagnostics.progressbar.progress
   wait
   fire_and_forget
   futures_of
   get_task_stream
   get_task_metadata
   performance_report


**Utilities**

.. autosummary::
   distributed.utils.Log
   distributed.utils.Logs


Asynchronous methods
--------------------

Most methods and functions can be used equally well within a blocking or
asynchronous environment using Tornado coroutines.  If used within a Tornado
IOLoop then you should yield or await otherwise blocking operations
appropriately.

You must tell the client that you intend to use it within an asynchronous
environment by passing the ``asynchronous=True`` keyword

.. code-block:: python

   # blocking
   client = Client()
   future = client.submit(func, *args)  # immediate, no blocking/async difference
   result = client.gather(future)  # blocking

   # asynchronous Python 2/3
   client = yield Client(asynchronous=True)
   future = client.submit(func, *args)  # immediate, no blocking/async difference
   result = yield client.gather(future)  # non-blocking/asynchronous

   # asynchronous Python 3
   client = await Client(asynchronous=True)
   future = client.submit(func, *args)  # immediate, no blocking/async difference
   result = await client.gather(future)  # non-blocking/asynchronous

The asynchronous variants must be run within a Tornado coroutine.  See the
:doc:`Asynchronous <asynchronous>` documentation for more information.


Client
------

.. currentmodule:: distributed

.. autoclass:: Client
   :members:

.. autoclass:: distributed.recreate_tasks.ReplayTaskClient
   :members:


Future
------

.. autoclass:: Future
   :members:


Synchronization
---------------

.. autoclass:: Event
   :members:
.. autoclass:: Lock
   :members:
.. autoclass:: MultiLock
   :members:
.. autoclass:: Semaphore
   :members:
.. autoclass:: Queue
   :members:
.. autoclass:: Variable
   :members:


Cluster
-------

Classes relevant for cluster creation and management. Other libraries
(like `dask-jobqueue`_, `dask-gateway`_, `dask-kubernetes`_, `dask-yarn`_ etc.)
provide additional cluster objects.

.. _dask-jobqueue: https://jobqueue.dask.org/
.. _dask-gateway: https://gateway.dask.org/
.. _dask-kubernetes: https://kubernetes.dask.org/
.. _dask-yarn: https://yarn.dask.org/en/latest/

.. autosummary::
   LocalCluster
   SpecCluster

.. autoclass:: LocalCluster
   :members:

.. autoclass:: SpecCluster
   :members:


Other
-----

.. autoclass:: as_completed
   :members:

.. autofunction:: distributed.diagnostics.progressbar.progress
.. autofunction:: wait
.. autofunction:: fire_and_forget
.. autofunction:: futures_of

.. currentmodule:: distributed

.. autofunction:: distributed.worker_client
.. autofunction:: distributed.get_worker
.. autofunction:: distributed.get_client
.. autofunction:: distributed.secede
.. autofunction:: distributed.rejoin
.. autoclass:: distributed.Reschedule
.. autoclass:: get_task_stream
.. autoclass:: get_task_metadata
.. autoclass:: performance_report


Utilities
---------

.. autoclass:: distributed.utils.Log
.. autoclass:: distributed.utils.Logs


Adaptive
--------

.. currentmodule:: distributed.deploy
.. autoclass:: Adaptive
   :members:

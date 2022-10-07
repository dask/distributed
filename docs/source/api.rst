.. _api:

API
===

.. currentmodule:: distributed

**Client**

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
   print
   warn
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
.. autofunction:: distributed.print
.. autofunction:: distributed.warn
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

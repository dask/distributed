.. _api:

API
===

.. currentmodule:: distributed

**Client**

.. autosummary::
   Client
   Client.call_stack
   Client.cancel
   Client.close
   Client.compute
   Client.gather
   Client.get
   Client.get_dataset
   Client.get_executor
   Client.get_metadata
   Client.get_scheduler_logs
   Client.get_worker_logs
   Client.get_task_stream
   Client.has_what
   Client.list_datasets
   Client.map
   Client.nthreads
   Client.persist
   Client.publish_dataset
   Client.profile
   Client.rebalance
   Client.replicate
   Client.restart
   Client.retry
   Client.run
   Client.run_on_scheduler
   Client.scatter
   Client.scheduler_info
   Client.write_scheduler_file
   Client.set_metadata
   Client.start_ipython_workers
   Client.start_ipython_scheduler
   Client.submit
   Client.unpublish_dataset
   Client.upload_file
   Client.wait_for_workers
   Client.who_has

.. currentmodule:: distributed

.. autosummary::
   worker_client
   get_worker
   get_client
   secede
   rejoin
   Reschedule

.. currentmodule:: distributed.recreate_exceptions

.. autosummary::
   ReplayExceptionClient.get_futures_error
   ReplayExceptionClient.recreate_error_locally

.. currentmodule:: distributed


**Future**

.. autosummary::
   Future
   Future.add_done_callback
   Future.cancel
   Future.cancelled
   Future.done
   Future.exception
   Future.result
   Future.retry
   Future.traceback

**Client Coordination**

.. currentmodule:: distributed

.. autosummary::
   Event
   Lock
   Queue
   Variable


**Other**

.. autosummary::
   as_completed
   distributed.diagnostics.progress
   wait
   fire_and_forget
   futures_of
   get_task_stream


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

.. autoclass:: distributed.recreate_exceptions.ReplayExceptionClient
   :members:


Future
------

.. autoclass:: Future
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

.. autofunction:: distributed.diagnostics.progress
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

.. autoclass:: Event
   :members:
.. autoclass:: Lock
   :members:
.. autoclass:: Semaphore
   :members:
.. autoclass:: Queue
   :members:
.. autoclass:: Variable
   :members:


Adaptive
--------

.. currentmodule:: distributed.deploy
.. autoclass:: Adaptive
   :members:

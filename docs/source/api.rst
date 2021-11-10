.. _api:

API
===

.. currentmodule:: distributed


Top-level functions
-------------------

.. autosummary::
   :toctree: generated/

   worker_client
   get_worker
   get_client
   secede
   rejoin
   Reschedule
   as_completed
   wait
   fire_and_forget
   futures_of


Recreate Tasks
~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   recreate_tasks.ReplayTaskClient.recreate_task_locally
   recreate_tasks.ReplayTaskClient.recreate_error_locally

Diagnostics
~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   diagnostics.progressbar.progress
   get_task_stream
   get_task_metadata
   performance_report

Utilities
~~~~~~~~~

.. autosummary::
   :toctree: generated/

   utils.Log
   utils.Logs

Client
------

.. autosummary::
   :toctree: generated/

   Client

.. autoautosummary:: distributed.Client
   :toctree: generated/
   :methods:


Client Coordination
~~~~~~~~~~~~~~~~~~~

.. autosummary::
   :toctree: generated/

   Event
   Lock
   MultiLock
   Queue
   Variable
   Semaphore

Future
------

.. autosummary::
   :toctree: generated/

   Future

.. autoautosummary:: distributed.Future
   :toctree: generated/
   :methods:


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
   :toctree: generated/

   LocalCluster

.. autoautosummary:: distributed.LocalCluster
   :toctree: generated/
   :methods:

.. autosummary::
   :toctree: generated/

   SpecCluster

.. autoautosummary:: distributed.SpecCluster
   :toctree: generated/
   :methods:

Adaptive
--------

.. currentmodule:: distributed.deploy
.. autosummary::
   :toctree: generated/
   
   Adaptive

.. autoautosummary:: distributed.Adaptive
   :toctree: generated/
   :methods:
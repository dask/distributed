Local Cluster
=============

For convenience you can start a local cluster from your Python session.

.. code-block:: python

   >>> from distributed import Client, LocalCluster
   >>> cluster = LocalCluster()
   LocalCluster("127.0.0.1:8786", workers=8, ncores=8)
   >>> client = Client(cluster)
   <Client: scheduler=127.0.0.1:8786 processes=8 cores=8>

You can dynamically scale this cluster up and down:

.. code-block:: python

   >>> worker = cluster.add_worker()
   >>> cluster.remove_worker(worker)

Alternatively, a ``LocalCluster`` is made for you automatically if you create
an ``Client`` with no arguments:

.. code-block:: python

   >>> from distributed import Client
   >>> client = Client()
   >>> client
   <Client: scheduler=127.0.0.1:8786 processes=8 cores=8>
   
Embedded interpreter
--------------------

You have to set the `correct executable for multiprocessing <https://docs.python.org/3/library/multiprocessing.html#multiprocessing.set_executable>`_ to launch a local cluster from within an embedded Python interpreter. Otherwise attempting to spawn workers will spawn new instances of the embedding application instead.

.. code-block:: python
    
    multiprocessing.set_executable(
        os.path.join(sys.exec_prefix, 'pythonw.exe'))  # Windows only

API
---

.. currentmodule:: distributed.deploy.local

.. autoclass:: LocalCluster
   :members:


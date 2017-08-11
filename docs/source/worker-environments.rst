Worker Environments
===================

A "worker environment" can be used to identify and prepare a set of workers for
certain tasks. For example, you may have some setup logic to connect to a
database that must be run prior to doing work, but don't want to open a
connection on every single worker. As another example, you may have certain
tasks that require a high amount of memory, so you wish to identify workers with
at least 32GB of RAM.

Components
----------

An environment consists of

1. A unique name like ``"high-memory"`` or ``"database-access"``
2. A ``condition``, a function of no arguments to be called on the worker.
   Determines whether that worker qualifies for the environment.
3. A ``setup`` a function of no arguments to be run on the worker when they
   join an environment
4. A ``teardown`` function of no arguments to be run on the worker when they
   leave the environment
5. A ``state`` variable (TODO: what exactly is this? Tentatively, each worker
   has a dictionary ``environment_state :: name -> Dict``, where you can get and
   put whatever you want in that nested ``Dict``)

TODO: Can / should the setup or teardown methods take the ``dask_worker`` itself?

Only the name is required.

Registering Environments
------------------------

Environments can be registered using :meth:`Client.register_worker_environment`.
You can use either a functional style:

.. code-block:: python

   def has_high_memory(dask_worker):
       import psutil
       return psutil.virtual_memory.total > 30e9

   client.register_worker_environment(name='high-memory',
                                      condition=has_high_memory)


Or class-based style by passing an instance of :class:`WorkerEnvironment`:

.. code-block:: python

   client.register_worker_environment(name='high-memory',
                                      environment=HighMemory)


If using the functional style, an instance of ``WorkerEnvironment`` is created
with the ``condition``, ``setup``, and ``teardown`` functions passed to
``client.register_worker_environment`` attached as methods.

The ``condition`` function will be scheduled to be run on each worker. Workers
that pass the condition are added to the environment, and any setup functions
are run.

The ``teardown`` method is run when workers are closed.

Specifying Environments
-----------------------

Environments can be created in code by subclassing ``distributed.Environment``
and overriding the ``condition``, ``setup``, and ``teardown`` methods.

.. code-block:: python

   class HighMemory(WorkerEnvironment):
       "An environment for workers with at least 30GB of RAM"
       name = 'high-memory'

       def condition(self):
           import psutil
           return psutil.virtual_memory.total > 30e9


Using Environmnets
------------------


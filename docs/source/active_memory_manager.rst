Active Memory Manager
=====================
The Active Memory Manager, or *AMM*, is an experimental daemon that optimizes memory
usage of workers across the Dask cluster. It is disabled by default.


Memory imbalance and duplication
--------------------------------
Whenever a Dask task returns data, it is stored on the worker that executed the task for
as long as it's a dependency of other tasks, is referenced by a
:class:`~distributed.Client` through a :class:`~distributed.Future`, or is part of a
:doc:`published dataset <publish>`.

Dask assigns tasks to workers following criteria of CPU occupancy, :doc:`resources`, and
locality. In the trivial use case of tasks that are not connected to each other, take
the same time to compute, return data of the same size, and have no resource
constraints, one will observe a perfect balance in memory occupation across workers too.
In all other use cases, however, as the computation goes it could cause an imbalance in
memory usage.

When a task runs on a worker and requires in input the output of a task from a different
worker, Dask will transparently transfer the data between workers, ending up with
multiple copies of the same data on different workers. This is generally desirable, as
it avoids re-transferring the data if it's required again later on. However, it also
causes increased overall memory usage across the cluster.


Enabling the Active Memory Manager
----------------------------------
The AMM can be enabled through the :doc:`Dask configuration file <configuration>`:

.. code-block:: yaml

   distributed:
     scheduler:
       active-memory-manager:
         start: true
         interval: 2s

The above is the recommended setup and will run all enabled *AMM policies* (see below)
every two seconds. Alternatively, you can manually start/stop the AMM from the
:class:`~distributed.Client` or trigger a one-off iteration:

.. code-block:: python

   >>> client.amm.start()  # Start running every 2 seconds
   >>> client.amm.stop()  # Stop running periodically
   >>> client.amm.running()
   False
   >>> client.amm.run_once()


Policies
--------
The AMM by itself doesn't do anything. The user must enable *policies* which suggest
actions regarding Dask data. The AMM runs the policies and enacts their suggestions, as
long as they don't harm data integrity. These suggestions can be of two types:

- Replicate the data of an in-memory Dask task from one worker to another.
  This should not be confused with replication caused by task dependencies.
- Delete one or more replicas of an in-memory task. The AMM will never delete the last
  replica of a task, even if a policy asks to.

There are no "move" operations. A move is performed in two passes: first a policy
creates a copy; in the next AMM iteration, the same or another policy deletes the
original (if the copy succeeded).

Unless a policy puts constraints on which workers should be impacted, the AMM will
automatically create replicas on workers with the lowest memory usage first and delete
them from workers with the highest memory usage first.

Individual policies are enabled, disabled, and configured through the Dask config:


.. code-block:: yaml

   distributed:
     scheduler:
       active-memory-manager:
         start: true
         interval: 2s
         policies:
         - class: distributed.active_memory_manager.ReduceReplicas
         - class: my_package.MyPolicy
           arg1: foo
           arg2: bar

See below for custom policies like the one in the example above.

The default Dask config file contains a sane selection of builtin policies that should
be generally desirable. You should try first with just ``start: true`` in your Dask
config and see if it is fit for purpose for you before you tweak individual policies.


Built-in policies
-----------------
ReduceReplicas
++++++++++++++
class
    :class:`distributed.active_memory_manager.ReduceReplicas`
parameters
    None

This policy is enabled in the default Dask config. Whenever a Dask task is replicated
on more than one worker and the additional replicas don't appear to serve an ongoing
computation, this policy drops all excess replicas.

.. note::
   This policy is incompatible with :meth:`~distributed.Client.replicate` and with the
   ``broadcast=True`` parameter of :meth:`~distributed.Client.scatter`. If you invoke
   :meth:`~distributed.Client.replicate` to create additional replicas and then later
   run this policy, it will delete all replicas but one (but not necessarily the new
   ones).


Custom policies
---------------
Power users can write their own policies by subclassing
:class:`~distributed.active_memory_manager.ActiveMemoryManagerPolicy`. The class should
define two methods:

``__init__``
    A custom policy may load parameters from the Dask config through ``__init__``
    parameters. If you don't need configuration, you don't need to implement this
    method.
``run``
    This method accepts no parameters and is invoked by the AMM every 2 seconds (or
    whatever the AMM interval is).
    It must yield zero or more of the following :class:`~distributed.active_memory_manager.Suggestion` namedtuples:

    ``yield Suggestion("replicate", <TaskState>)``
        Create one replica of the target task on the worker with the lowest memory usage
        that doesn't hold a replica yet. To create more than one replica, you need to
        yield the same command more than once.
    ``yield Suggestion("replicate", <TaskState>, {<WorkerState>, <WorkerState>, ...})``
        Create one replica of the target task on the worker with the lowest memory among
        the listed candidates.
    ``yield Suggestion("drop", <TaskState>)``
        Delete one replica of the target task on the worker with the highest memory
        usage across the whole cluster.
    ``yield Suggestion("drop", <TaskState>, {<WorkerState>, <WorkerState>, ...})``
        Delete one replica of the target task on the worker with the highest memory
        among the listed candidates.

    The AMM will silently reject unacceptable suggestions, such as:

    - Delete the last replica of a task
    - Delete a replica from a subset of workers that don't hold any
    - Delete a replica from a worker that currently needs it for computation
    - Replicate a task that is not yet in memory
    - Create more replicas of a task than there are workers
    - Create replicas of a task on workers that already hold them
    - Create replicas on paused or retiring workers

    It is generally a good idea to design policies to be as simple as possible and let
    the AMM take care of the edge cases above by ignoring some of the suggestions.

    Optionally, the ``run`` method may retrieve which worker the AMM just selected, as
    follows:

    .. code-block:: python

        ws = (yield Suggestion("drop", ts))

The ``run`` method can access the following attributes:

``self.manager``
    The :class:`~distributed.active_memory_manager.ActiveMemoryManagerExtension` that
    the policy is attached to
``self.manager.scheduler``
    :class:`~distributed.scheduler.Scheduler` to which the suggestions will be applied.
    From there you can access various attributes such as ``tasks`` and ``workers``.
``self.manager.workers_memory``
    Read-only mapping of ``{WorkerState: bytes}``. bytes is the expected RAM usage of
    the worker after all suggestions accepted so far in the current AMM iteration, from
    all policies, will be enacted. Note that you don't need to access this if you are
    happy to always create/delete replicas on the workers with the lowest and highest
    memory usage respectively - the AMM will handle it for you.
``self.manager.pending``
    Read-only mapping of ``{TaskState: ({<WorkerState>, ...}, {<WorkerState>, ...})``.
    The first set contains the workers that will receive a new replica of the task
    according to the suggestions accepted so far; the second set contains the workers
    which will lose a replica.
``self.manager.policies``
    Set of policies registered in the AMM. A policy can deregister itself as follows:

    .. code-block:: python

       def run(self):
           self.manager.policies.drop(self)

Example
+++++++
The following custom policy ensures that keys "foo" and "bar" are replicated on all
workers at all times. New workers will receive a replica soon after connecting to the
scheduler. The policy will do nothing if the target keys are not in memory somewhere or
if all workers already hold a replica. Note that this example is incompatible with the
:class:`~distributed.active_memory_manager.ReduceReplicas` built-in policy.

In mymodule.py (it must be accessible by the scheduler):

.. code-block:: python

    from distributed.active_memory_manager import ActiveMemoryManagerPolicy, Suggestion


    class EnsureBroadcast(ActiveMemoryManagerPolicy):
        def __init__(self, key):
            self.key = key

        def run(self):
            ts = self.manager.scheduler.tasks.get(self.key)
            if not ts:
                return
            for _ in range(len(self.manager.scheduler.workers) - len(ts.who_has)):
                yield Suggestion("replicate", ts)

Note that the policy doesn't bother testing for edge cases such as paused workers or
other policies also requesting replicas; the AMM takes care of it. In theory you could
rewrite the last two lines as follows (at the cost of some wasted CPU cycles):

.. code-block:: python

    for _ in range(1000):
        yield Suggestion("replicate", ts)

In distributed.yaml:

.. code-block:: yaml

   distributed:
     scheduler:
       active-memory-manager:
         start: true
         interval: 2s
         policies:
         - class: mymodule.EnsureBroadcast
           key: foo
         - class: mymodule.EnsureBroadcast
           key: bar

We could have alternatively used a single policy instance with a list of keys - the
above design merely illustrates that you may have multiple instances of the same policy
running side by side.


API reference
-------------
.. autoclass:: distributed.active_memory_manager.ActiveMemoryManagerExtension
   :members:

.. autoclass:: distributed.active_memory_manager.ActiveMemoryManagerPolicy
   :members:

.. autoclass:: distributed.active_memory_manager.Suggestion
   :members:

.. autoclass:: distributed.active_memory_manager.AMMClientProxy
   :members:
   :undoc-members:

.. autoclass:: distributed.active_memory_manager.ReduceReplicas

.. autoclass:: distributed.active_memory_manager.RetireWorker
   :members:

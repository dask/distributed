Scheduling State
================

.. currentmodule:: distributed.scheduler

Overview
--------

The life of a computation with Dask can be described in the following stages:

1.  The user authors a graph using some library, perhaps Dask.delayed or
    dask.dataframe or the ``submit/map`` functions on the client.  They submit
    these tasks to the scheduler.
2.  The schedulers assimilates these tasks into its graph of all tasks to track
    and as their dependencies become available it asks workers to run each of
    these tasks.
3.  The worker receives information about how to run the task, communicates
    with its peer workers to collect dependencies, and then runs the relevant
    function on the appropriate data.  It reports back to the scheduler that it
    has finished.
4.  The scheduler reports back to the user that the task has completed.  If the
    user desires, it then fetches the data from the worker through the
    scheduler.

Most relevant logic is in tracking tasks as they evolve from newly submitted,
to waiting for dependencies, to actively running on some worker, to finished in
memory, to garbage collected.  Tracking this process, and tracking all effects
that this task has on other tasks that might depend on it, is the majority of
the complexity of the dynamic task scheduler.  This section describes the
system used to perform this tracking.

For more abstract information about the policies used by the scheduler, see
:doc:`Scheduling Policies<scheduling-policies>`.

The scheduler keeps internal state about several kinds of entities:

* Individual tasks known to the scheduler
* Workers connected to it
* Clients connected to it


.. note::
   Everything listed in the page is an internal detail of how Dask operates.
   It may change between versions and you should probably avoid relying
   on it in user code (including on any APIs explained here).


Task State
----------

Internally, the scheduler moves tasks between a fixed set of states,
notably ``released``, ``waiting``, ``no-worker``, ``processing``,
``memory``, ``error``.

Tasks flow along the following states with the following allowed transitions:

.. image:: images/task-state.svg
    :alt: Dask scheduler task states

*  *Released*: Known but not actively computing or in memory
*  *Waiting*: On track to be computed, waiting on dependencies to arrive in
   memory
*  *No-worker*: Ready to be computed, but no appropriate worker exists
   (for example because of resource restrictions, or because no worker is
   connected at all).
*  *Processing*: Actively being computed by one or more workers
*  *Memory*: In memory on one or more workers
*  *Erred*: Task computation, or one of its dependencies, has encountered an error
*  *Forgotten* (not actually a state): Task is no longer needed by any client
   or dependent task

In addition to the literal state, though, other information needs to be
kept and updated about each task.  Individual task state is stored in an
object named :class:`TaskState` and consists of the following information:

.. class:: TaskState

   .. attribute:: key: str

      The key is the unique identifier of a task, generally formed
      from the name of the function, followed by a hash of the function
      and arguments, like ``'inc-ab31c010444977004d656610d2d421ec'``.

   .. attribute:: prefix: str

      The key prefix, used in certain calculations to get an estimate
      of the task's duration based on the duration of other tasks in the
      same "family" (for example ``'inc'``).

   .. attribute:: run_spec: object

      A specification of how to run the task.  The type and meaning of this
      value is opaque to the scheduler, as it is only interpreted by the
      worker to which the task is sent for executing.

      As a special case, this attribute may also be ``None``, in which case
      the task is "pure data" (such as, for example, a piece of data loaded
      in the scheduler using :meth:`Client.scatter`).  A "pure data" task
      cannot be computed again if its value is lost.

   .. attribute:: priority: tuple

      The priority provides each task with a relative ranking which is used
      to break ties when many tasks are being considered for execution.

      This ranking is generally a 2-item tuple.  The first (and dominant)
      item corresponds to when it was submitted.  Generally, earlier tasks
      take precedence.  The second item is determined by the client, and is
      a way to prioritize tasks within a large graph that may be important,
      such as if they are on the critical path, or good to run in order to
      release many dependencies.  This is explained further in
      :doc:`Scheduling Policy <scheduling-policies>`.

   .. attribute:: state: str

      This task's current state.  Valid states include ``released``,
      ``waiting``, ``no-worker``, ``processing``, ``memory``, ``erred``
      and ``forgotten``.  If it is ``forgotten``, the task isn't stored
      in the ``task_states`` dictionary anymore and will probably disappear
      soon from memory.

   .. attribute:: dependencies: {TaskState}

      The set of tasks this task depends on for proper execution.  Only
      tasks still alive are listed in this set.  If, for whatever reason,
      this task also depends on a forgotten task, the
      :attr:`has_lost_dependencies` flag is set.

      A task can only be executed once all its dependencies have already
      been successfully executed and have their result stored on at least
      one worker.  This is tracked by progressively draining the
      :attr:`waiting_on` set.

   .. attribute:: dependents: {TaskState}

      The set of tasks which depend on this task.  Only tasks still alive
      are listed in this set.

      This is the reverse mapping of :attr:`dependencies`.

   .. attribute:: has_lost_dependencies: bool

      Whether any of the dependencies of this task has been forgotten.
      For memory consumption reasons, forgotten tasks are not kept in
      memory even though they may have dependent tasks.  When a task is
      forgotten, therefore, each of its dependents has their
      :attr:`has_lost_dependencies` attribute set to ``True``.

      If :attr:`has_lost_dependencies` is true, this task cannot go
      into the "processing" state anymore.

   .. attribute:: waiting_on: {TaskState}

      The set of tasks this task is waiting on *before* it can be executed.
      This is always a subset of :attr:`dependencies`.  Each time one of the
      dependencies has finished processing, it is removed from the
      :attr:`waiting_on` set.

      Once :attr:`waiting_on` becomes empty, this task can move from the
      "waiting" state to the "processing" state (unless one of the
      dependencies errored out, in which case this task is instead
      marked "erred").

   .. attribute:: waiters: {TaskState}

      The set of tasks which need this task to remain alive.  This is always
      a subset of :attr:`dependents`.  Each time one of the dependents
      has finished processing, it is removed from the :attr:`waiters`
      set.

      Once both :attr:`waiters` and :attr:`who_wants` become empty, this
      task can be released (if it has a non-empty :attr:`run_spec`) or
      forgotten (otherwise) by the scheduler, and by any workers
      in :attr:`who_has`.

      .. note:: Counter-intuitively, :attr:`waiting_on` and
         :attr:`waiters` are not reverse mappings of each other.

   .. attribute:: who_wants: {ClientState}

      The set of clients who want this task's result to remain alive.
      This is the reverse mapping of :attr:`ClientState.wants_what`.

      When a client submits a graph to the scheduler it also specifies
      which output tasks it desires, such that their results are not released
      from memory.

      Once a task has finished executing (i.e. moves into the "memory"
      or "erred" state), the clients in :attr:`who_wants` are notified.

      Once both :attr:`waiters` and :attr:`who_wants` become empty, this
      task can be released (if it has a non-empty :attr:`run_spec`) or
      forgotten (otherwise) by the scheduler, and by any workers
      in :attr:`who_has`.

   .. attribute:: who_has: {WorkerState}

      The set of workers who have this task's result in memory.
      It is non-empty iff the task is in the "memory" state.  There can be
      more than one worker in this set if, for example, :meth:`Client.scatter`
      or :meth:`Client.replicate` was used.

      This is the reverse mapping of :attr:`WorkerState.has_what`.

   .. attribute:: processing_on: WorkerState (or None)

      If this task is in the "processing" state, which worker is currently
      processing it.  Otherwise this is ``None``.

      This attribute is kept in sync with :attr:`WorkerState.processing`.

   .. attribute:: retries: int

      The number of times this task can automatically be retried in case
      of failure.  If a task fails executing (the worker returns with
      an error), its :attr:`retries` attribute is checked.  If it is
      equal to 0, the task is marked "erred".  If it is greater than 0,
      the :attr:`retries` attribute is decremented and execution is
      attempted again.

   .. attribute:: nbytes: int (or None)

      The number of bytes, as determined by ``sizeof``, of the result
      of a finished task.  This number is used for diagnostics and to
      help prioritize work.

   .. attribute:: exception: object

      If this task failed executing, the exception object is stored here.
      Otherwise this is ``None``.

   .. attribute:: traceback: object

      If this task failed executing, the traceback object is stored here.
      Otherwise this is ``None``.

   .. attribute:: exception_blame: TaskState (or None)

      If this task or one of its dependencies failed executing, the
      failed task is stored here (possibly itself).  Otherwise this
      is ``None``.

   .. attribute:: suspicious: int

      The number of times this task has been involved in a worker death.

      Some tasks may cause workers to die (such as calling ``os._exit(0)``).
      When a worker dies, all of the tasks on that worker are reassigned
      to others.  This combination of behaviors can cause a bad task to
      catastrophically destroy all workers on the cluster, one after
      another.  Whenever a worker dies, we mark each task currently
      processing on that worker (as recorded by
      :attr:`WorkerState.processing`) as suspicious.

      If a task is involved in three deaths (or some other fixed constant)
      then we mark the task as ``erred``.

   .. attribute:: host_restrictions: {hostnames}

      A set of hostnames where this task can be run (or ``None`` if empty).
      Usually this is empty unless the task has been specifically restricted
      to only run on certain hosts.  A hostname may correspond to one or
      several connected workers.

   .. attribute:: worker_restrictions: {worker addresses}

      A set of complete worker addresses where this can be run (or ``None``
      if empty).  Usually this is empty unless the task has been specifically
      restricted to only run on certain workers.

      Note this is tracking worker addresses, not worker states, since
      the specific workers may not be connected at this time.

   .. attribute:: resource_restrictions: {resource: quantity}

      Resources required by this task, such as ``{'gpu': 1}`` or
      ``{'memory': 1e9}`` (or ``None`` if empty).  These are user-defined
      names and are matched against the contents of each
      :attr:`WorkerState.resources` dictionary.

   .. attribute:: loose_restrictions: bool

      If ``False``, each of :attr:`host_restrictions`,
      :attr:`worker_restrictions` and :attr:`resource_restrictions` is
      a hard constraint: if no worker is available satisfying those
      restrictions, the task cannot go into the "processing" state and
      will instead go into the "no-worker" state.

      If ``True``, the above restrictions are mere preferences: if no worker
      is available satisfying those restrictions, the task can still go
      into the "processing" state and be sent for execution to another
      connected worker.


The scheduler keeps track of all the :class:`TaskState` objects (those
not in the "forgotten" state) using several containers:

.. attribute:: task_states: {str: TaskState}

   A dictionary mapping task keys (usually strings) to :class:`TaskState`
   objects.  Task keys are how information about tasks is communicated
   between the scheduler and clients, or the scheduler and workers; this
   dictionary is then used to find the corresponding :class:`TaskState`
   object.

.. attribute:: unrunnable: {TaskState}

   A set of :class:`TaskState` objects in the "no-worker" state.  These
   tasks already have all their :attr:`~TaskState.dependencies` satisfied
   (their :attr:`~TaskState.waiting_on` set is empty), and are waiting
   for an appropriate worker to join the network before computing.


Worker State
------------

Each worker's current state is stored in a :class:`WorkerState` object.
This information is involved in deciding
:ref:`which worker to run a task on <decide-worker>`.

.. class:: WorkerState

   .. attribute:: worker_key

      This worker's unique key.  This can be its connected address
      (such as ``'tcp://127.0.0.1:8891``) or an alias (such as ``'alice'``).

   .. attribute:: processing: {TaskState: cost}

      A dictionary of tasks that have been submitted to this worker.
      Each task state is asssociated with the expected cost in seconds
      of running that task, summing both the task's expected computation
      time and the expected communication time of its result.

      Multiple tasks may be submitted to a worker in advance and the worker
      will run them eventually, depending on its execution resources
      (but see :doc:`work-stealing`).

      All the tasks here are in the "processing" state.

      This attribute is kept in sync with :attr:`TaskState.processing_on`.

   .. attribute:: has_what: {TaskState}

      The set of tasks which currently reside on this worker.
      All the tasks here are in the "memory" state.

      This is the reverse mapping of :class:`TaskState.who_has`.

   .. attribute:: nbytes: int

      The total memory size, in bytes, used by the tasks this worker
      holds in memory (i.e. the tasks in this worker's :attr:`has_what`).

   .. attribute:: ncores: int

      The number of CPU cores made available on this worker.

   .. attribute:: resources: {str: Number}

      The available resources on this worker like ``{'gpu': 2}``.
      These are abstract quantities that constrain certain tasks from
      running at the same time on this worker.

   .. attribute:: used_resources: {str: Number}

      The sum of each resource used by all tasks allocated to this worker.
      The numbers in this dictionary can only be less or equal than
      those in this worker's :attr:`resources`.

   .. attribute:: occupancy: Number

      The total expected runtime, in seconds, of all tasks currently
      processing on this worker.  This is the sum of all the costs in
      this worker's :attr:`processing` dictionary.


In addition to individual worker state, the scheduler maintains two
containers to help with scheduling tasks:

.. attribute:: Scheduler.saturated: {WorkerState}

   A set of workers whose computing power (as
   measured by :attr:`WorkerState.ncores`) is fully exploited by processing
   tasks, and whose current :attr:`~WorkerState.occupancy` is a lot greater
   than the average.

.. attribute:: Scheduler.idle: {WorkerState}

   A set of workers whose computing power is not fully exploited.  These
   workers are assumed to be able to start computing new tasks immediately.


These two sets are disjoint.  Also, some workers may be *neither* "idle"
nor "saturated".  "Idle" workers will be preferred when
:ref:`deciding a suitable worker <decide-worker>` to run a new task on.
Conversely, "saturated" workers may see their workload lightened through
:doc:`work-stealing`.


Client State
------------

Information about each individual client is kept in a :class:`ClientState`
object:

.. class:: ClientState

   .. attribute:: client_key: str

      A unique identifier for this client.  This is generally an opaque
      string generated by the client itself.

   .. attribute:: wants_what: {TaskState}

      A set of tasks this client wants kept in memory, so that it can
      download its result when desired.  This is the reverse mapping of
      :class:`TaskState.who_wants`.

      Tasks are typically removed from this set when the corresponding
      object in the client's space (for example a ``Future`` or a Dask
      collection) gets garbage-collected.


.. XXX list invariants somewhere?


Understanding a Task's Flow
---------------------------

As seen above, there are numerous pieces of information pertaining to
task and worker state, and some of them can be computed, updated or
removed during a task's transitions.

The table below shows which state variable a task is in, depending on the
task's state.  Cells with a check mark (`✓`) indicate the task key *must*
be present in the given state variable; cells with an question mark (`?`)
indicate the task key *may* be present in the given state variable.


================================================================ ======== ======= ========= ========== ====== =====
State variable                                                   Released Waiting No-worker Processing Memory Erred
================================================================ ======== ======= ========= ========== ====== =====
:attr:`TaskState.dependencies`                                   ✓        ✓       ✓         ✓          ✓      ✓
:attr:`TaskState.dependents`                                     ✓        ✓       ✓         ✓          ✓      ✓
---------------------------------------------------------------- -------- ------- --------- ---------- ------ -----
:attr:`TaskState.host_restrictions`                              ?        ?       ?         ?          ?      ?
:attr:`TaskState.worker_restrictions`                            ?        ?       ?         ?          ?      ?
:attr:`TaskState.resource_restrictions`                          ?        ?       ?         ?          ?      ?
:attr:`TaskState.loose_restrictions`                             ?        ?       ?         ?          ?      ?
---------------------------------------------------------------- -------- ------- --------- ---------- ------ -----
:attr:`TaskState.waiting_on`                                              ✓                 ✓
:attr:`TaskState.waiters`                                                 ✓                 ✓
:attr:`TaskState.processing_on`                                                             ✓
:attr:`WorkerState.processing`                                                              ✓
:attr:`TaskState.who_has`                                                                              ✓
:attr:`WorkerState.has_what`                                                                           ✓
:attr:`TaskState.nbytes` *(1)*                                   ?        ?       ?         ?          ✓      ?
:attr:`TaskState.exception` *(2)*                                                                             ?
:attr:`TaskState.traceback` *(2)*                                                                             ?
:attr:`TaskState.exception_blame`                                                                             ✓
:attr:`TaskState.retries`                                        ?        ?       ?         ?          ?      ?
:attr:`TaskState.suspicious_tasks`                               ?        ?       ?         ?          ?      ?
================================================================ ======== ======= ========= ========== ====== =====

Notes:

1. :attr:`TaskState.nbytes`: this attribute can be known as long as a
   task has already been computed, even if it has been later released.

2. :attr:`TaskState.exception` and :attr:`TaskState.traceback` should
   be looked up on the :attr:`TaskState.exception_blame` task.


The table below shows which worker state variables are updated on each
task state transition.

==================================== ==========================================================
Transition                           Affected worker state
==================================== ==========================================================
released → waiting                   occupancy, idle, saturated
waiting → processing                 occupancy, idle, saturated, used_resources
waiting → memory                     idle, saturated, nbytes
processing → memory                  occupancy, idle, saturated, used_resources, nbytes
processing → erred                   occupancy, idle, saturated, used_resources
processing → released                occupancy, idle, saturated, used_resources
memory → released                    nbytes
memory → forgotten                   nbytes
==================================== ==========================================================

.. note::
   Another way of understanding this table is to observe that entering or
   exiting a specific task state updates a well-defined set of worker state
   variables.  For example, entering and exiting the "memory" state updates
   :attr:`WorkerState.nbytes`.


Implementation
--------------

Every transition between states is a separate method in the scheduler.  These
task transition functions are prefixed with ``transition`` and then have the
name of the start and finish task state like the following.

.. code-block:: python

   def transition_released_waiting(self, key):

   def transition_processing_memory(self, key):

   def transition_processing_erred(self, key):

These functions each have three effects.

1.  They perform the necessary transformations on the scheduler state (the 20
    dicts/lists/sets) to move one key between states.
2.  They return a dictionary of recommended ``{key: state}`` transitions to
    enact directly afterwards on other keys.  For example after we transition a
    key into memory we may find that many waiting keys are now ready to
    transition from waiting to a ready state.
3.  Optionally they include a set of validation checks that can be turned on
    for testing.

Rather than call these functions directly we call the central function
``transition``:

.. code-block:: python

   def transition(self, key, final_state):
       """ Transition key to the suggested state """

This transition function finds the appropriate path from the current to the
final state.  It also serves as a central point for logging and diagnostics.

Often we want to enact several transitions at once or want to continually
respond to new transitions recommended by initial transitions until we reach a
steady state.  For that we use the ``transitions`` function (note the plural ``s``).

.. code-block:: python

   def transitions(self, recommendations):
       recommendations = recommendations.copy()
       while recommendations:
           key, finish = recommendations.popitem()
           new = self.transition(key, finish)
           recommendations.update(new)

This function runs ``transition``, takes the recommendations and runs them as
well, repeating until no further task-transitions are recommended.


Stimuli
-------

Transitions occur from stimuli, which are state-changing messages to the
scheduler from workers or clients.  The scheduler responds to the following
stimuli:

* **Workers**
    * Task finished: A task has completed on a worker and is now in memory
    * Task erred: A task ran and erred on a worker
    * Task missing data: A task tried to run but was unable to find necessary
      data on other workers
    * Worker added: A new worker was added to the network
    * Worker removed: An existing worker left the network

* **Clients**
    * Update graph: The client sends more tasks to the scheduler
    * Release keys: The client no longer desires the result of certain keys

Stimuli functions are prepended with the text ``stimulus``, and take a variety
of keyword arguments from the message as in the following examples:

.. code-block:: python

   def stimulus_task_finished(self, key=None, worker=None, nbytes=None,
                              type=None, compute_start=None, compute_stop=None,
                              transfer_start=None, transfer_stop=None):

   def stimulus_task_erred(self, key=None, worker=None,
                           exception=None, traceback=None)

These functions change some non-essential administrative state and then call
transition functions.

Note that there are several other non-state-changing messages that we receive
from the workers and clients, such as messages requesting information about the
current state of the scheduler.  These are not considered stimuli.


API
---

.. currentmodule:: distributed.scheduler

.. autoclass:: Scheduler
   :members:

.. autofunction:: decide_worker

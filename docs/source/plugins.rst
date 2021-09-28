Scheduler Plugins
=================

.. autoclass:: distributed.diagnostics.plugin.SchedulerPlugin
   :members:


RabbitMQ Example
----------------

RabbitMQ is a distributed messaging queue that we can use to post updates about
task transitions. By posting transitions to RabbitMQ, we allow other machines
to do the processing of transitions and keep scheduler processing to a minimum.
See the
`RabbitMQ tutorial <https://www.rabbitmq.com/tutorials/tutorial-two-python.html>`_
for more information on RabbitMQ and how to consume the messages.

.. code-block:: python

   import json
   from distributed.diagnostics.plugin import SchedulerPlugin
   import pika

   class RabbitMQPlugin(SchedulerPlugin):
       def __init__(self):
           # Update host to be your RabbitMQ host
           self.connection = pika.BlockingConnection(
               pika.ConnectionParameters(host='localhost'))
           self.channel = self.connection.channel()
           self.channel.queue_declare(queue='dask_task_status', durable=True)

       def transition(self, key, start, finish, *args, **kwargs):
           message = dict(
               key=key,
               start=start,
               finish=finish,
           )
           self.channel.basic_publish(
               exchange='',
               routing_key='dask_task_status',
               body=json.dumps(message),
               properties=pika.BasicProperties(
                   delivery_mode=2,  # make message persistent
               ))

   @click.command()
   def dask_setup(scheduler):
       plugin = RabbitMQPlugin()
       scheduler.add_plugin(plugin)

Run with: ``dask-scheduler --preload <filename.py>``

Accessing Full Task State
-------------------------

If you would like to access the full :class:`distributed.scheduler.TaskState`
stored in the scheduler you can do this by passing and storing a reference to
the scheduler as so:

.. code-block:: python

   from distributed.diagnostics.plugin import SchedulerPlugin

   class MyPlugin(SchedulerPlugin):
       def __init__(self, scheduler):
            self.scheduler = scheduler

       def transition(self, key, start, finish, *args, **kwargs):
            # Get full TaskState
            ts = self.scheduler.tasks[key]

   @click.command()
   def dask_setup(scheduler):
       plugin = MyPlugin(scheduler)
       scheduler.add_plugin(plugin)

Worker Plugins
==============

:class:`distributed.diagnostics.plugin.WorkerPlugin` provides a base class
for creating your own worker plugins. In addition, Dask provides some
:ref:`built-in plugins <plugins.builtin>`.

.. autoclass:: distributed.diagnostics.plugin.WorkerPlugin
   :members:

.. _plugins.builtin:

Built-In Worker Plugins
-----------------------

.. autoclass:: distributed.diagnostics.plugin.PipInstall
.. autoclass:: distributed.diagnostics.plugin.UploadFile


Nanny Plugins
=============

.. autoclass:: distributed.diagnostics.plugin.NannyPlugin
   :members:


Built-In Nanny Plugins
----------------------

.. autoclass:: distributed.diagnostics.plugin.Environ
.. autoclass:: distributed.diagnostics.plugin.UploadDirectory
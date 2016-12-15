Shared Futures With Channels
============================

A channel is a changing stream of futures shared between any number of clients
connected to the same scheduler.


Examples
--------

Basic Usage
~~~~~~~~~~~
Create channels from your Client:

.. code-block:: python

    >>> client = Client('scheduler-address:8786')
    >>> chan = client.channel('my-channel')

Append futures onto a channel

.. code-block:: python

    >>> future = client.submit(add, 1, 2)
    >>> chan.append(future)

A channel maintains a collection of current futures added by both your
client, and others.

.. code-block:: python

    >>> chan.futures
    deque([<Future: status: pending, key: add-12345>,
           <Future: status: pending, key: sub-56789>])

If you wish to persist the current status of tasks outside of the distributed
cluster (e.g. to take a snapshot in case of shutdown) you can copy a channel full
of futures as it is a python deque_.

.. _deque: https://docs.python.org/3.5/library/collections.html#collections.deque`

.. code-block:: python

    channelcopy = list(chan.futures)

You can iterate over a channel to get back futures.

.. code-block:: python

    >>> anotherclient = Client('scheduler-address:8786')
    >>> chan = anotherclient.channel('my-channel')
    >>> for future in chan:
    ...     pass

When done writing, call flush to wait until your appends have been
fully registered with the scheduler.

.. code-block:: python

    >>> client = Client('scheduler-address:8786')
    >>> chan = client.channel('my-channel')
    >>> future2 = client.submit(time.sleep,2)
    >>> chan.append(future2)
    >>> chan.flush()


Example with local_client
~~~~~~~~~~~~~~~~~~~~~~~~~

Using channels with `local client`_ allows for a more decoupled version
of what is possible with :doc:`Data Streams with Queues<queues>`
in that independent worker clients can build up a set of results
which can be read later by a different client.
This opens up Dask/Distributed to being integrated in a wider application
environment similar to other python task queues such as Celery_.

.. _local client: http://distributed.readthedocs.io/en/latest/task-launch.html#submit-tasks-from-worker
.. _Celery: http://www.celeryproject.org/

.. code-block:: python

    import random, time, operator
    from distributed import Client, local_client
    from time import sleep

    def emit(name):
        with local_client() as c:
           chan = c.channel(name)
           while True:
               future = c.submit(random.random, pure=False)
               chan.append(future)
               sleep(1)

    def combine():
        with local_client() as c:
            a_chan = c.channel('a')
            b_chan = c.channel('b')
            out_chan = c.channel('adds')
            for a, b in zip(a_chan, b_chan):
                future = c.submit(operator.add, a, b)
                out_chan.append(future)

    client = Client()

    emitters = (client.submit(emit, 'a'), client.submit(emit, 'b'))
    combiner = client.submit(combine)
    chan = client.channel('adds')


    for future in chan:
        print(future.result())
       ...:
    1.782009416831722
    ...

All iterations on a channel by different clients can be stopped using the ``stop`` method

.. code-block:: python

    chan.stop()


Very short-lived clients
~~~~~~~~~~~~~~~~~~~~~~~~

If you wish to submit work to your cluster from a short lived client such as a
web application view, a AWS Lambda function or some other fire and forget script,
channels give a way to do this.

Motivating Example - A workflow designer
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Imagine the following deployment scenario:

.. image:: images/channels_example.png
    :alt: Workflow designer application

1) A `Custom Dask Graph`_ is designed in a web app, perhaps inserting code snippets.

.. code-block:: javascript

    {
        "tracking_key_1" :["allowed_module.func", arg1, arg2],
        "tracking_key_2" :["allowed_module.func", "tracking_key_1"]
    }

2) Import strings or code snippets are validated and imported to function objects.
The graph might be persisted and appropriate keys added for tracking. The graph is
added to a channel that is followed by one of the clients. As the client will
only exist for the lifetime of the request then the graph is transmitted using scatter.

.. code-block:: python

    client = Client('scheduler-address:8786')
    chan = client.channel('input')
    dask_graph = {
                    "tracking_key_1" :(func, arg1, arg2),
                    "tracking_key_2" :(func2, "tracking_key_1")
                 }
    future_list = client.scatter([( "<worflow_run_id>", dask_graph])
    chan.append(future_list[0])
    chan.flush()

3) A long running client process is listening for new new futures
on the channel `input`. A long running local client function
`hold_completed_futures` listens for output tasks to hold them ready
for a different client. Dask graphs are submitted and run in the
`run_on_remote` function. The constituent futures are added to the output
graph so that the python web services can view status


.. code-block:: python

    from distributed import Client
    from distributed.client import Future, sync
    from distributed import local_client
    import time

    def run_on_remote(dsk):
        with local_client() as c:
            output_channel = c.channel("output")

            print(dsk)
            keys = list(dsk.keys())
            keys.sort()
            print(keys)
            #nonblocking get initially so consituent futures can be added to a channel
            future = c._get(dsk, keys)
            for key in keys:
                future = Future(key, c)
                output_channel.append(future)
            #blocking get in order to ensure task is completed
            data = c.get(dsk, keys)
        return data

    def hold_completed_futures():
        with local_client() as c:
            output_channel = c.channel("output")
            for future in output_channel:
                print("holding a future")
                time.sleep(3600)
                if future.status != "finished":
                    print(future.status)


    if __name__=='__main__':
        client = Client('localhost:8786')
        input_channel = client.channel("input")
        output_channel = client.channel("output")
        main_future = client.submit(hold_completed_futures)
        #output_channel = client.channel("output")
        for future in input_channel:
            #we know this future is just shared data so we call result
            workflow_run_id, dask_graph = future.result()
            print(future.key)
            print("recieved", dask_graph)

            fut=client.submit(run_on_remote, dask_graph, pure=True, key=workflow_run_id)
            output_channel.append(fut)

4) We have now created an output channel with all of the futures from the dask
graph. The next task is to create functions with further short-lived clients
that can manage these futures from the web application.

.. code-block:: python

    client = Client('scheduler-address:8786')
    chan = client.channel('output')
    futures_snapshot = list(chan.futures)
    for future in futures_snapshot:
        if future.key in futures_of_interest:
            #send a response or update database etc. if the futures are completed
            pass

5) The workflow designer UI app can then retrieve a task status for the original
JSON dask graph that was submitted.



.. _`Custom Dask Graph`: http://dask.pydata.org/en/latest/custom-graphs.html

Further Details
---------------

Often it is desirable to respond to events from outside the distributed cluster
or to instantiate a new client in order to check on the progress of a set of tasks.
The channels feature makes these and many other workflows possible.

This functionality is similar to queues but
additionally means that multiple clients can send data to a long running function
rather than one client holding a queue instance.

Several clients connected to the same scheduler can communicate a sequence
of futures between each other through shared channels. All clients can
append to the channel at any time. All clients will be updated when a
channel updates. The central scheduler maintains consistency and ordering
of events. It also allows the Dask Scheduler to be extended in a clean way
using the normal Distributed task submission.


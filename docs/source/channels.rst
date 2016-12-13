Channels: Futures Shared Between Clients
===================================

Often it is desirable to respond to events from outside the distributed cluster 
or to instantiate a new client in order to check on the progress of a set of tasks. 
The channels feature makes these and many other workflows possible. 
It also allows the Dask Scheduler to be extended in a clean way using the normal
Distributed task submission,

Channel:
--------
A changing stream of futures shared between clients

Several clients connected to the same scheduler can communicate a sequence
of futures between each other through shared channels. All clients can
append to the channel at any time. All clients will be updated when a
channel updates. The central scheduler maintains consistency and ordering
of events.

Examples

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

You can iterate over a channel to get back futures.

.. code-block:: python
>>> for future in chan:
...     pass

Example with local_client

.. code-block:: python
import random, time, operator
from distributed import Client, local_client
from tornado import gen

def emit(name):
    with local_client() as c:
       chan = c.channel(name)
       while True:
           future = c.submit(random.random, pure=False)
            chan.append(future)
            gen.sleep(1)

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

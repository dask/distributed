.. _ssl:

SSL
===

Currently dask distributed has experimental support for SSL communication.

In order to use this all workers / schedulers and clients need to be started with
these additional arguments.

.. code-block:: python

   >>> from distributed import Client
   >>> connection_kwargs = {'ssl_options': {'certfile': '/path/to/cert.pem', 'keyfile': '/path/to/key.pem'}}
   >>> client = Client(connection_kwargs=connection_kwargs)

When starting up the dask workers / scheduler on the command line you have to specify these as command line arguments

.. code-block:: bash

    $ dask-worker --certfile /path/to/cert.pem --keyfile /path/to/key.pem

Dask does not provide any mechanism to distribute these keys for users.

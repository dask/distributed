.. _tls:

TLS/SSL
=======

Currently dask distributed has experimental support for TLS/SSL communication.

In order to use this all workers / schedulers and clients need to be started with
these additional arguments.

.. code-block:: python

   >>> from distributed import config
   >>> config['tls-certfile'] = '/path/to/cert.pem'
   >>> config['tls-keyfile'] = '/path/to/key.pem'
   >>> config['default-scheme'] = 'tls'
   >>> client = Client()

When starting up the dask workers / scheduler on the command line you have to specify these as command line arguments

.. code-block:: bash

    $ dask-worker --certfile /path/to/cert.pem --keyfile /path/to/key.pem

Dask does not provide any mechanism to distribute these keys for users.

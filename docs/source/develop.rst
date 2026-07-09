Development Guidelines
======================

This repository is part of the Dask_ projects.  General development guidelines
including where to ask for help, a layout of repositories, testing practices,
and documentation and style standards are available at the `Dask developer
guidelines`_ in the main documentation.

.. _Dask: http://dask.org
.. _`Dask developer guidelines`: http://docs.dask.org/en/latest/develop.html

Install
-------
Clone this repository with git::

     git clone git@github.com:dask/distributed.git
     cd distributed

From the top level of your cloned Dask repository you can deploy and test a
local version of thisd project, along with all necessary dependencies, using
pixi_.

Pixi uses lockfiles to freeze the installed version of all dependencies.
To update the lockfile::

   pixi update

To keep a fork in sync with the upstream source::

   cd distributed
   git remote add upstream git@github.com:dask/distributed.git
   git remote -v
   git fetch -a upstream
   git checkout main
   git pull upstream main
   git push origin main

.. _pixi: https://pixi.prefix.dev/


Test
----

Test using ``pytest``::

   pixi run test

You can pass arbitrary pytest parameters to the command; e.g.::

   pixi run test distributed/tests/test_scheduler.py -k scatter

Test with coverage, including slow tests::

   pixi run test-ci

Generate a local coverage report after running ``test-ci``::

   pixi run coverage html

There are several variant environments for testing, against obsolete but
still supported versions of dependencies, as well as against variant and
experimental configurations::

   pixi run -e mindeps test-ci
   pixi run -e mindeps-array test-ci
   pixi run -e mindeps-dataframe test-ci
   pixi run -e py310 test-ci
   pixi run -e py311 test-ci
   pixi run -e py312 test-ci
   pixi run -e py313 test-ci
   pixi run -e py314 test-ci
   pixi run -e py314t test-ci
   pixi run -e nightly test-ci

There are also specialty test tasks::

   pixi run -e <any environment> test-noqueue


Tornado
-------

Dask.distributed is a Tornado TCP application.  Tornado provides us with both a
communication layer on top of sockets, as well as a syntax for writing
asynchronous coroutines, similar to asyncio.  You can make modest changes to
the policies within this library without understanding much about Tornado,
however moderate changes will probably require you to understand Tornado
IOLoops, coroutines, and a little about non-blocking communication..  The
Tornado API documentation is quite good and we recommend that you read the
following resources:

*  http://www.tornadoweb.org/en/stable/gen.html
*  http://www.tornadoweb.org/en/stable/ioloop.html

Additionally, if you want to interact at a low level with the communication
between workers and scheduler then you should understand the Tornado
``TCPServer`` and ``IOStream`` available here:

*  http://www.tornadoweb.org/en/stable/networking.html

Dask.distributed wraps a bit of logic around Tornado.  See
:doc:`Foundations<foundations>` for more information.

Writing Tests
-------------

Testing distributed systems is normally quite difficult because it is difficult
to inspect the state of all components when something goes wrong.  Fortunately,
the non-blocking asynchronous model within Tornado allows us to run a
scheduler, multiple workers, and multiple clients all within a single thread.
This gives us predictable performance, clean shutdowns, and the ability to drop
into any point of the code during execution.
At the same time, sometimes we want everything to run in different processes in
order to simulate a more realistic setting.

The test suite contains three kinds of tests

1.  ``@gen_cluster``: Fully asynchronous tests where all components live in the
    same event loop in the main thread.  These are good for testing complex
    logic and inspecting the state of the system directly.  They are also
    easier to debug and cause the fewest problems with shutdowns.
2.  ``def test_foo(client)``: Tests with multiple processes forked from the main
    process.  These are good for testing the synchronous (normal user) API and
    when triggering hard failures for resilience tests.
3.  ``popen``: Tests that call out to the command line to start the system.
    These are rare and mostly for testing the command line interface.

If you are comfortable with the Tornado interface then you will be happiest
using the ``@gen_cluster`` style of test, e.g.

.. code-block:: python

    # tests/test_submit.py

    from distributed.utils_test import gen_cluster, inc
    from distributed import Client, Future, Scheduler, Worker

    @gen_cluster(client=True)
    async def test_submit(c, s, a, b):
        assert isinstance(c, Client)
        assert isinstance(s, Scheduler)
        assert isinstance(a, Worker)
        assert isinstance(b, Worker)

        future = c.submit(inc, 1)
        assert isinstance(future, Future)
        assert future.key in c.futures

        # result = future.result()  # This synchronous API call would block
        result = await future
        assert result == 2

        assert future.key in s.tasks
        assert future.key in a.data or future.key in b.data


The ``@gen_cluster`` decorator sets up a scheduler, client, and workers for
you and cleans them up after the test.  It also allows you to directly inspect
the state of every element of the cluster directly.  However, you can not use
the normal synchronous API (doing so will cause the test to wait forever) and
instead you need to use the coroutine API, where all blocking functions are
prepended with an underscore (``_``) and awaited with ``await``.
Beware, it is a common mistake to use the blocking interface within these tests.

If you want to test the normal synchronous API you can use the ``client``
pytest fixture style test, which sets up a scheduler and workers for you in
different forked processes:

.. code-block:: python

   from distributed.utils_test import client

   def test_submit(client):
       future = client.submit(inc, 10)
       assert future.result() == 11

Additionally, if you want access to the scheduler and worker processes you can
also add the ``s, a, b`` fixtures as well.


.. code-block:: python

   from distributed.utils_test import client

   def test_submit(client, s, a, b):
       future = client.submit(inc, 10)
       assert future.result() == 11  # use the synchronous/blocking API here

       a['proc'].terminate()  # kill one of the workers

       result = future.result()  # test that future remains valid
       assert result == 2

In this style of test you do not have access to the scheduler or workers.  The
variables ``s, a, b`` are now dictionaries holding a
``multiprocessing.Process`` object and a port integer.  However, you can now
use the normal synchronous API (never use ``await`` in this style of test) and you
can close processes easily by terminating them.

Typically for most user-facing functions you will find both kinds of tests.
The ``@gen_cluster`` tests test particular logic while the ``client`` pytest
fixture tests test basic interface and resilience.

You should avoid ``popen`` style tests unless absolutely necessary, such as if
you need to test the command line interface.

Code Formatting
---------------

Dask.distributed uses several code linters (ruff, black, mypy),
which are enforced by CI. Developers should run them locally before they submit a PR,
through the single command ``pixi run lint``. This makes sure that linter
versions and options are aligned for all developers.

Optionally, you may wish to setup the `pre-commit hooks <https://pre-commit.com/>`_ to
run automatically when you make a git commit. This can be done by running::

   pixi run -e lint pre-commit install

from the root of the distributed repository. Now the code linters will be run each time
you commit changes. You can skip these checks with ``git commit --no-verify`` or with
the short version ``git commit -n``.

Making Pull Requests
--------------------

Pull Request Etiquette
~~~~~~~~~~~~~~~~~~~~~~

When opening a Pull Request you are beginning a dialog with maintainers. This is a bidirectional
relationship where you are asking for the reviewer's time to look at your contribution, and 
the reviewer will likely ask for your input and engage you in discussion around the changes.

Please do not propose code that you are not willing to stand behind and discuss.
Be prepared to respond to review feedback, apply critical thinking and iterate on your contributions.

We ask that you fill out all sections of PR templates and provide reasoning behind your changes,
ideally with a linked issue that has been discussed by the community.

Automated Contributions and AI Policy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We encourage the use of AI and automated tools to assist in code development,
documentation, and testing. However, we ask that contributors disclose these tools and
use them in a way that aligns with Dask's community guidelines. In particular:

- Do not use tools to think or speak for you in discussions, code reviews, or any other 
  interactions within the Dask community.
- The AI agent can exclusively open _draft_ PRs that are clearly tagged as being written
  by unsupervised AI. There is a `open-pr` agent skill that explains how.
- You (the human) **must fully review, understand, and approve** everything that the AI
  agent wrote. You must replace all AI-generated prose with your own human thoughts.
- Only after your personal review and cleanup happened, you can remove the disclaimer
  and mark the PR as ready to be reviewed.

TL;DR version
^^^^^^^^^^^^^

**Maintainers will close, without reading it, any issue or PR that looks written by
AI.** First draft written by AI is OK. Final PR is not.

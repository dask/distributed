Frequently Asked Questions
==========================

More questions can be found on StackOverflow at http://stackoverflow.com/search?tab=votes&q=dask%20distributed

How do I use external modules?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Most functions you will run on your cluster will require imports. This
is even true for passing any object which is not a python builtin -
the pickle serialisation method will save references to imported modules
rather than trying to send all of your source code.

You therefore must ensure that workers have access to all of the modules
you will need, and ideally with exactly the same versions.

Maintain consistent environments
````````````````````````````````

If you manage your environments yourself, then setting up module consistency
can be as simple as creating environments from the same pip or conda specification
on each machine. You should consult the documentation for ``pip``, ``pipenv``
and ``conda``, whichever you normally use. You will normally want to be as specific
about package versions as possible, and distribute the same environment file to
workers before installation.

However, other common ways to distribute an environment directly, rather than build it
in-place, include:

- docker images, where the environment has been built into the image; this is the
  normal route when you are running on infrastructure enabled by docker, such as
  kubernetes
- `conda-pack`_ is a tool for bundling existing conda environments, so they can be
  relocated to other machines. This tool was specifically created for dask on YARN/hadoop
  clusters, but could be used elsewhere
- shared filesystem, e.g., NFS, that can be seen by all machines. Note that importing
  python modules is fairly IO intensive, so your server needs to be able to handle
  many requests
- cluster install method (parcels...): depending on your infrastructure, there may be
  ways to install specific binaries to all workers in a cluster.

.. _conda-pack: https://conda.github.io/conda-pack/

Temporary installations
```````````````````````
The worker plugin ``distributed.diagnostics.plugin.PipInstall`` allows you to
run pip installation commands on your workers, and optionally have them restart
upon success. Please read the plugin documentation to see how to use this.


Send Source
```````````

Particularly during development, you may want to send files directly to workers
that are already running.

You should use ``client.upload_file`` in these cases.
For more detail, see the `API docs`_ and a
StackOverflow question
`"Can I use functions imported from .py files in Dask/Distributed?"`__
This function supports both standalone file and setuptools's ``.egg`` files
for larger modules.

__ http://stackoverflow.com/questions/39295200/can-i-use-functions-imported-from-py-files-in-dask-distributed
.. _API docs: https://distributed.readthedocs.io/en/latest/api.html#distributed.executor.Executor.upload_file

Too many open file descriptors?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Your operating system imposes a limit to how many open files or open network
connections any user can have at once.  Depending on the scale of your
cluster the ``dask-scheduler`` may run into this limit.

By default most Linux distributions set this limit at 1024 open
files/connections and OS-X at 128 or 256.  Each worker adds a few open
connections to a running scheduler (somewhere between one and ten, depending on
how contentious things get.)

If you are on a managed cluster you can usually ask whoever manages your
cluster to increase this limit.  If you have root access and know what you are
doing you can change the limits on Linux by editing
``/etc/security/limits.conf``.  Instructions are here under the heading "User
Level FD Limits":
http://www.cyberciti.biz/faq/linux-increase-the-maximum-number-of-open-files/

Error when running dask-worker about ``OMP_NUM_THREADS``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For more problems with OMP_NUM_THREADS, see
http://stackoverflow.com/questions/39422092/error-with-omp-num-threads-when-using-dask-distributed


Does Dask handle Data Locality?
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Yes, both data locality in memory and data locality on disk.

Often it's *much* cheaper to move computations to where data lives.  If one of
your tasks creates a large array and a future task computes the sum of that
array, you want to be sure that the sum runs on the same worker that has the
array in the first place, otherwise you'll wait for a long while as the data
moves between workers.  Needless communication can easily dominate costs if
we're sloppy.

The Dask Scheduler tracks the location and size of every intermediate value
produced by every worker and uses this information when assigning future tasks
to workers.  Dask tries to make computations more efficient by minimizing data
movement.

Sometimes your data is on a hard drive or other remote storage that isn't
controlled by Dask.  In this case the scheduler is unaware of exactly where your
data lives, so you have to do a bit more work.  You can tell Dask to
preferentially run a task on a particular worker or set of workers.

For example Dask developers use this ability to build in data locality when we
communicate to data-local storage systems like the Hadoop File System.  When
users use high-level functions like
``dask.dataframe.read_csv('hdfs:///path/to/files.*.csv')`` Dask talks to the
HDFS name node, finds the locations of all of the blocks of data, and sends
that information to the scheduler so that it can make smarter decisions and
improve load times for users.


PermissionError [Errno 13] Permission Denied: \`/root/.dask\`
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

This error can be seen when starting distributed through the standard process
control tool ``supervisor`` and running as a non-root user. This is caused
by ``supervisor`` not passing the shell environment variables through to the
subprocess, head to `this section`_ of the supervisor documentation to see
how to pass the ``$HOME`` and ``$USER`` variables through.

.. _this section: http://supervisord.org/subprocess.html#subprocess-environment


KilledWorker, CommsClosed, etc.
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

In the case that workers disappear unexpectedly from your cluster, you may see
a range of error messages. After checking the logs of the workers affected, you
should read the section :doc:`killed`.

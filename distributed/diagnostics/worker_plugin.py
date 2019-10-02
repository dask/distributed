class WorkerPlugin(object):
    """ Interface to extend the Worker

    Throughout the lifecycle of a task (see :doc:`Worker <worker>`), Workers are
    instructed by the scheduler to compute certain tasks, resulting in transitions
    in the state of each task. The Worker owning the task is then notified of this
    state transition.

    A plugin enables custom code to run at each of these state transitions. Whenever a
    task changes its state, the ``transition`` method on this class will be called. The
    user code runs within the Worker's main thread.
.
    To implement a plugin implement some of the methods of this class and register
    the plugin to your client in order to have it attached to every existing and
    future workers with ``Client.register_worker_plugin``.

    Examples
    --------
    >>> class ErrorLogger(WorkerPlugin):
    ...     def __init__(self, logger):
    ...         self.logger = logger
    ...
    ...     def setup(self, worker):
    ...         self.worker = worker
    ...
    ...     def transition(self, key, start, finish, *args, **kwargs):
    ...         if finish == 'error':
    ...             exc = self.worker.exceptions[key]
    ...             self.logger.error("Task '%s' has failed with exception: %s" % (key, str(exc)))

    >>> l = ErrorLogger()
    >>> client.register_worker_plugin(l)  # doctest: +SKIP
    """

    def setup(self, worker):
        """ Run when the plugin is attached to a worker """

    def teardown(self):
        """ Run when the worker to which the plugin is attached is closed """

    def transition(self, key, start, finish, **kwargs):
        """
        Run when the worker to which the plugin is attached is notified
        of a task state transition

        Parameters
        ----------
        key: string
        start: string
            Start state of the transition.
            One of waiting, ready, executing, long-running, memory, error.
        finish: string
            Final state of the transition.
        kwargs: More options passed when transitioning
        """

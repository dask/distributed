from tornado import gen


class WorkerEnvironment(object):
    """A worker environment.

    Parameters
    ----------
    condition : callable
    setup : callable
    teardown : callable
    """

    _name = None  # Used only for a nicer repr, and nothing else

    def __init__(self, condition=None, setup=None, teardown=None):
        self._condition = condition
        self._setup = setup
        self._teardown = teardown

    def __repr__(self):
        return "<WorkerEnvironment({})>".format(self._name or '')

    @gen.coroutine
    def condition(self):
        """A function be run on a worker determining whether
        it qualifies for this environment.

        Returns
        -------
        qualifies : bool

        Notes
        -----
        By default, the just returns True, meaning every worker falls
        in the environment.
        """
        if self._condition:
            yield gen.maybe_future(self._condition())
        return True

    @gen.coroutine
    def setup(self):
        """Run on the worker when it's added to the environment"""
        if self._setup:
            return self._setup()

    @gen.coroutine
    def teardown(self):
        """Run on the worker when it's removed from the environment"""
        if self._teardown:
            return self._teardown()

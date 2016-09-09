from __future__ import absolute_import, division, print_function


class Environment(object):
    def __init__(self, condition=None, setup=None):
        self._condition = condition
        self._setup = setup

    def condition(self):
        if self._condition:
            return self._condition()
        return True

    def setup(self):
        if self._setup:
            self._setup()

    def __getstate__(self):
        return (self._condition, self._setup)

    def __setstate__(self, state):
        self._condition, self._setup = state

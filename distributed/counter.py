from crick import TDigest
from tornado.ioloop import PeriodicCallback

class Counter(object):
    def __init__(self, loop=None, intervals=(5, 60, 3600)):
        self.intervals = intervals
        self.digests = [TDigest() for i in self.intervals]
        self.tick = 0

        self.loop = loop
        self._pc = PeriodicCallback(self.shift, self.intervals[0] * 1000,
                                    io_loop=self.loop)
        self._pc.start()

    def add(self, item):
        self.digests[0].add(item)

    def update(self, seq):
        self.digests[0].update(seq)

    def shift(self):
        self.tick += 1
        for i, inter in enumerate(self.intervals[:-1]):
            if self.tick % inter == 0:
                self.digests[i + 1].merge(self.digests[i])
                self.digests[i] = TDigest()

    def count(self):
        return sum(d.count() for d in self.digests)

from __future__ import print_function, division, absolute_import

from collections import defaultdict, deque
import logging
from math import log
import random
from time import time

from toolz import first
from tornado.iostream import StreamClosedError
from tornado.ioloop import PeriodicCallback

from .utils import key_split, log_errors
from .diagnostics.plugin import SchedulerPlugin

BANDWIDTH = 100e6
LATENCY = 10e-3
log_2 = log(2)

logger = logging.getLogger(__name__)



class WorkStealing(SchedulerPlugin):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.stealable = dict()
        self.key_stealable = dict()
        self.stealable_unknown_durations = defaultdict(set)

        self.cost_multipliers = [1 + 2 ** (i - 6) for i in range(15)]
        self.cost_multipliers[0] = 1

        for worker in scheduler.workers:
            self.add_worker(worker=worker)

        self._pc = PeriodicCallback(callback=self.balance,
                                    callback_time=100,
                                    io_loop=self.scheduler.loop)
        self.scheduler.loop.add_callback(self._pc.start)
        self.scheduler.plugins.append(self)
        self.scheduler.extensions['stealing'] = self
        self.log = deque(maxlen=100000)

    def add_worker(self, scheduler=None, worker=None):
        self.stealable[worker] = [set() for i in range(15)]

    def remove_worker(self, scheduler=None, worker=None):
        del self.stealable[worker]

    def teardown(self):
        self._pc.stop()

    def transition(self, key, start, finish, compute_start=None,
            compute_stop=None, *args, **kwargs):
        if finish == 'processing':
            self.put_key_in_stealable(key)

        if start == 'processing':
            self.remove_key_from_stealable(key)
            if finish == 'memory':
                ks = key_split(key)
                if ks in self.stealable_unknown_durations:
                    for k in self.stealable_unknown_durations.pop(ks):
                        if self.scheduler.task_state[k] == 'processing':
                            self.put_key_in_stealable(k, split=ks)


    def put_key_in_stealable(self, key, split=None):
        try:
            worker = first(self.scheduler.rprocessing[key])
        except Exception as e:
            logger.exception(e)
            import pdb; pdb.set_trace()
        cost_multiplier, level = self.steal_time_ratio(key, split=split)
        if cost_multiplier is not None:
            self.stealable[worker][level].add(key)
            self.key_stealable[key] = (worker, level)

    def remove_key_from_stealable(self, key):
        result = self.key_stealable.pop(key, None)
        if result is not None:
            worker, level = result
            try:
                self.stealable[worker][level].remove(key)
            except:
                pass

    def steal_time_ratio(self, key, split=None):
        """ The compute to communication time ratio of a key

        Returns
        -------

        cost_multiplier: The increased cost from moving this task as a factor.
        For example a result of zero implies a task without dependencies.
        level: The location within a stealable list to place this value
        """
        if (key not in self.scheduler.loose_restrictions
                and (key in self.scheduler.host_restrictions or
                     key in self.scheduler.worker_restrictions) or
            key in self.scheduler.resource_restrictions):
            return None, None  # don't steal

        if not self.scheduler.dependencies[key]:  # no dependencies fast path
            return 0, 0

        nbytes = sum(self.scheduler.nbytes.get(k, 1000)
                     for k in self.scheduler.dependencies[key])

        transfer_time = nbytes / BANDWIDTH + LATENCY
        split = split or key_split(key)
        if split in fast_tasks:
            return None, None
        try:
            compute_time = self.scheduler.task_duration[split]
        except KeyError:
            self.stealable_unknown_durations[split].add(key)
            return None, None
        else:
            cost_multiplier = transfer_time / compute_time
            if cost_multiplier > 100:
                return None, None

            level = int(round(log(cost_multiplier) / log_2 + 6, 0))
            level = max(1, level)
            return cost_multiplier, level

    def move_task(self, key, victim, thief):
        with log_errors():
            if self.scheduler.validate:
                if victim not in self.scheduler.rprocessing[key]:
                    import pdb; pdb.set_trace()

            # self.remove_key_from_stealable(key)
            logger.info("Moved %s, %s: %2f -> %s: %2f", key,
                    victim, self.scheduler.occupancy[victim],
                    thief, self.scheduler.occupancy[thief])

            duration = self.scheduler.processing[victim].pop(key)
            self.scheduler.rprocessing[key].remove(victim)
            self.scheduler.occupancy[victim] -= duration
            self.scheduler.total_occupancy -= duration

            duration = self.scheduler.task_duration.get(key_split(key), 0.5)
            self.scheduler.processing[thief][key] = duration
            self.scheduler.rprocessing[key].add(thief)
            self.scheduler.occupancy[thief] += duration
            self.scheduler.total_occupancy += duration

            self.scheduler.worker_streams[victim].send({'op': 'release-task',
                                                        'key': key})

            try:
                self.scheduler.send_task_to_worker(thief, key)
            except StreamClosedError:
                self.scheduler.remove_worker(thief)

    def balance(self):
        with log_errors():
            i = 0
            s = self.scheduler
            occupancy = s.occupancy
            idle = s.idle
            saturated = s.saturated
            if not idle or not saturated:
                return

            start = time()

            broken = False
            seen = False
            acted = False

            if len(s.saturated) < 20:
                saturated = sorted(saturated, key=occupancy.get, reverse=True)

            if len(idle) < 20:
                idle = sorted(idle, key=occupancy.get)


            for level, cost_multiplier in enumerate(self.cost_multipliers):
                if not idle or not saturated:
                    break
                for sat in list(saturated):
                    stealable = self.stealable[sat][level]
                    if not stealable or not idle:
                        continue
                    else:
                        seen = True

                    for key in list(stealable):
                        idl = idle[i % len(idle)]
                        i += 1
                        duration = s.task_duration[key_split(key)]

                        if (occupancy[idl] + cost_multiplier * duration
                          <= occupancy[sat] - duration / 2):
                            self.move_task(key, sat, idl)
                            self.log.append((level, sat, idl, key))
                            self.scheduler.check_idle_saturated(sat)
                            self.scheduler.check_idle_saturated(idl)
                            stealable.remove(key)
                            acted = True
                if seen and not acted:
                    break

            stop = time()
            if self.scheduler.digests:
                self.scheduler.digests['steal-duration'].add(stop - start)

    def restart(self, scheduler):
        for stealable in self.stealable.values():
            for s in stealable:
                s.clear()

        self.key_stealable.clear()
        self.stealable_unknown_durations.clear()


fast_tasks = {'shuffle-split'}

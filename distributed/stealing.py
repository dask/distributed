from __future__ import print_function, division, absolute_import

from collections import defaultdict, deque
import logging
from math import log
import os
from time import time

from .config import config
from .core import CommClosedError
from .diagnostics.plugin import SchedulerPlugin
from .utils import key_split, log_errors, PeriodicCallback

try:
    from cytoolz import topk
except ImportError:
    from toolz import topk

BANDWIDTH = 100e6
LATENCY = 10e-3
log_2 = log(2)

logger = logging.getLogger(__name__)


LOG_PDB = config.get('pdb-on-err') or os.environ.get('DASK_ERROR_PDB', False)


class WorkStealing(SchedulerPlugin):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        self.stealable_all = [set() for i in range(15)]
        self.stealable = dict()
        self.key_stealable = dict()
        self.stealable_unknown_durations = defaultdict(set)

        self.cost_multipliers = [1 + 2 ** (i - 6) for i in range(15)]
        self.cost_multipliers[0] = 1

        for worker in scheduler.workers:
            self.add_worker(worker=worker)

        pc = PeriodicCallback(callback=self.balance,
                              callback_time=100)
        self._pc = pc
        self.scheduler.periodic_callbacks['stealing'] = pc
        self.scheduler.plugins.append(self)
        self.scheduler.extensions['stealing'] = self
        self.scheduler.events['stealing'] = deque(maxlen=100000)
        self.count = 0
        self.in_flight = dict()
        self.in_flight_occupancy = defaultdict(lambda: 0)

        self.scheduler.worker_handlers['steal-response'] = self.move_task_confirm

    @property
    def log(self):
        return self.scheduler.events['stealing']

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
                        if k in self.in_flight:
                            continue
                        tts = self.scheduler.task_states[k]
                        if tts.state == 'processing':
                            self.put_key_in_stealable(k, split=ks)
            else:
                if key in self.in_flight:
                    del self.in_flight[key]

    def put_key_in_stealable(self, key, split=None):
        ws = self.scheduler.task_states[key].processing_on
        worker = ws.worker_key
        cost_multiplier, level = self.steal_time_ratio(key, split=split)
        self.log.append(('add-stealable', key, worker, level))
        if cost_multiplier is not None:
            self.stealable_all[level].add(key)
            self.stealable[worker][level].add(key)
            self.key_stealable[key] = (worker, level)

    def remove_key_from_stealable(self, key):
        result = self.key_stealable.pop(key, None)
        if result is None:
            return

        worker, level = result
        self.log.append(('remove-stealable', key, worker, level))
        try:
            self.stealable[worker][level].remove(key)
        except KeyError:
            pass
        try:
            self.stealable_all[level].remove(key)
        except KeyError:
            pass

    def steal_time_ratio(self, key, split=None):
        """ The compute to communication time ratio of a key

        Returns
        -------

        cost_multiplier: The increased cost from moving this task as a factor.
        For example a result of zero implies a task without dependencies.
        level: The location within a stealable list to place this value
        """
        ts = self.scheduler.task_states[key]
        if (not ts.loose_restrictions
            and (ts.host_restrictions or ts.worker_restrictions
                 or ts.resource_restrictions)):
            return None, None  # don't steal

        if not ts.dependencies:  # no dependencies fast path
            return 0, 0

        nbytes = sum(dep.get_nbytes() for dep in ts.dependencies)

        transfer_time = nbytes / BANDWIDTH + LATENCY
        split = split or key_split(key)
        if split in fast_tasks:
            return None, None
        ws = ts.processing_on
        if ws is None:
            self.stealable_unknown_durations[split].add(key)
            return None, None
        else:
            compute_time = ws.processing[ts]
            if compute_time < 0.005:  # 5ms, just give up
                return None, None
            cost_multiplier = transfer_time / compute_time
            if cost_multiplier > 100:
                return None, None

            level = int(round(log(cost_multiplier) / log_2 + 6, 0))
            level = max(1, level)
            return cost_multiplier, level

    def move_task_request(self, key, victim, thief):
        try:
            ts = self.scheduler.task_states[key]

            if self.scheduler.validate:
                if victim is not ts.processing_on:
                    import pdb
                    pdb.set_trace()

            self.remove_key_from_stealable(key)
            logger.debug("Request move %s, %s: %2f -> %s: %2f", key,
                         victim, victim.occupancy,
                         thief, thief.occupancy)

            victim_duration = victim.processing[ts]

            thief_duration = (
                self.scheduler.get_task_duration(ts) +
                self.scheduler.get_comm_cost(ts, thief)
            )

            self.scheduler.worker_comms[victim.worker_key].send(
                {'op': 'steal-request', 'key': key})

            self.in_flight[key] = {'victim': victim,
                                   'thief': thief,
                                   'victim_duration': victim_duration,
                                   'thief_duration': thief_duration}

            self.in_flight_occupancy[victim] -= victim_duration
            self.in_flight_occupancy[thief] += thief_duration
        except CommClosedError:
            logger.info("Worker comm closed while stealing: %s", victim)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def move_task_confirm(self, key=None, worker=None, state=None):
        try:
            try:
                d = self.in_flight.pop(key)
            except KeyError:
                return
            thief = d['thief']
            victim = d['victim']
            logger.debug("Confirm move %s, %s -> %s.  State: %s", key, victim,
                         thief, state)

            self.in_flight_occupancy[thief] -= d['thief_duration']
            self.in_flight_occupancy[victim] += d['victim_duration']

            ts = self.scheduler.task_states[key]

            if not self.in_flight:
                self.in_flight_occupancy = defaultdict(lambda: 0)

            if ts.state != 'processing' or ts.processing_on is not victim:
                old_thief = thief.occupancy
                new_thief = sum(thief.processing.values())
                old_victim = victim.occupancy
                new_victim = sum(victim.processing.values())
                thief.occupancy = new_thief
                victim.occupancy = new_victim
                self.scheduler.total_occupancy += new_thief - old_thief + new_victim - old_victim
                return

            # One of the pair has left, punt and reschedule
            if (thief.worker_key not in self.scheduler.workers or
                victim.worker_key not in self.scheduler.workers):
                self.scheduler.reschedule(key)
                return

            # Victim had already started execution, reverse stealing
            if state in ('memory', 'executing', 'long-running', None):
                self.log.append(('already-computing',
                                 key, victim.worker_key, thief.worker_key))
                self.scheduler.check_idle_saturated(thief)
                self.scheduler.check_idle_saturated(victim)

            # Victim was waiting, has given up task, enact steal
            elif state in ('waiting', 'ready'):
                self.remove_key_from_stealable(key)
                ts.processing_on = thief
                duration = victim.processing.pop(ts)
                victim.occupancy -= duration
                thief.processing[ts] = d['thief_duration']
                thief.occupancy += d['thief_duration']
                self.scheduler.total_occupancy += d['thief_duration'] - duration
                self.put_key_in_stealable(key)

                try:
                    self.scheduler.send_task_to_worker(thief.worker_key, key)
                except CommClosedError:
                    self.scheduler.remove_worker(thief.worker_key)
                self.log.append(('confirm',
                                 key, victim.worker_key, thief.worker_key))
            else:
                raise ValueError("Unexpected task state: %s" % state)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb
                pdb.set_trace()
            raise

    def balance(self):
        s = self.scheduler

        def combined_occupancy(ws):
            return ws.occupancy + self.in_flight_occupancy[ws]

        def perhaps_move_task(level, key, sat, idl, duration, cost_multiplier):
            occ_idl = combined_occupancy(idl)
            occ_sat = combined_occupancy(sat)

            if (occ_idl + cost_multiplier * duration
                    <= occ_sat - duration / 2):
                self.move_task_request(key, sat, idl)
                log.append((start, level, key, duration,
                            sat.worker_key, occ_sat,
                            idl.worker_key, occ_idl))
                s.check_idle_saturated(sat, occ=occ_sat)
                s.check_idle_saturated(idl, occ=occ_idl)

        with log_errors():
            i = 0
            idle = s.idle
            saturated = s.saturated
            if not idle or len(idle) == len(s.workers):
                return

            log = []
            start = time()

            if not s.saturated:
                saturated = topk(10, s.workers.values(), key=combined_occupancy)
                saturated = [ws for ws in saturated
                             if combined_occupancy(ws) > 0.2
                             and len(ws.processing) > ws.ncores]
            elif len(s.saturated) < 20:
                saturated = sorted(saturated, key=combined_occupancy, reverse=True)
            if len(idle) < 20:
                idle = sorted(idle, key=combined_occupancy)

            for level, cost_multiplier in enumerate(self.cost_multipliers):
                if not idle:
                    break
                for sat in list(saturated):
                    stealable = self.stealable[sat.worker_key][level]
                    if not stealable or not idle:
                        continue

                    for key in list(stealable):
                        if key not in self.key_stealable:
                            stealable.remove(key)
                            continue
                        ts = s.task_states[key]
                        i += 1
                        if not idle:
                            break
                        idl = idle[i % len(idle)]

                        try:
                            duration = sat.processing[ts]
                        except KeyError:
                            stealable.remove(key)
                            continue

                        perhaps_move_task(level, key, sat, idl,
                                          duration, cost_multiplier)

                if self.cost_multipliers[level] < 20:  # don't steal from public at cost
                    stealable = self.stealable_all[level]
                    for key in list(stealable):
                        if not idle:
                            break
                        if key not in self.key_stealable:
                            stealable.remove(key)
                            continue
                        ts = s.task_states[key]

                        sat = ts.processing_on
                        if sat is None:
                            stealable.remove(key)
                            continue
                        if combined_occupancy(sat) < 0.2:
                            continue
                        if len(sat.processing) <= sat.ncores:
                            continue

                        i += 1
                        idl = idle[i % len(idle)]
                        duration = sat.processing[ts]

                        perhaps_move_task(level, key, sat, idl,
                                          duration, cost_multiplier)

            if log:
                self.log.append(log)
                self.count += 1
            stop = time()
            if s.digests:
                s.digests['steal-duration'].add(stop - start)

    def restart(self, scheduler):
        for stealable in self.stealable.values():
            for s in stealable:
                s.clear()

        for s in self.stealable_all:
            s.clear()
        self.key_stealable.clear()
        self.stealable_unknown_durations.clear()

    def story(self, *keys):
        keys = set(keys)
        out = []
        for L in self.log:
            if not isinstance(L, list):
                L = [L]
            for t in L:
                if any(x in keys for x in t):
                    out.append(t)
        return out


fast_tasks = {'shuffle-split'}

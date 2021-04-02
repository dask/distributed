import logging
from collections import defaultdict, deque
from math import log2
from time import time

from tlz import topk
from tornado.ioloop import PeriodicCallback

import dask

from .comm.addressing import get_address_host
from .core import CommClosedError
from .diagnostics.plugin import SchedulerPlugin
from .utils import log_errors, parse_timedelta

LATENCY = 10e-3

logger = logging.getLogger(__name__)


LOG_PDB = dask.config.get("distributed.admin.pdb-on-err")


class WorkStealing(SchedulerPlugin):
    def __init__(self, scheduler):
        self.scheduler = scheduler
        # { level: { task states } }
        self.stealable_all = [set() for i in range(15)]
        # { worker: { level: { task states } } }
        self.stealable = dict()
        # { task state: (worker, level) }
        self.key_stealable = dict()

        self.cost_multipliers = [1 + 2 ** (i - 6) for i in range(15)]
        self.cost_multipliers[0] = 1

        for worker in scheduler.workers:
            self.add_worker(worker=worker)

        callback_time = parse_timedelta(
            dask.config.get("distributed.scheduler.work-stealing-interval"),
            default="ms",
        )
        # `callback_time` is in milliseconds
        pc = PeriodicCallback(callback=self.balance, callback_time=callback_time * 1000)
        self._pc = pc
        self.scheduler.periodic_callbacks["stealing"] = pc
        self.scheduler.plugins.append(self)
        self.scheduler.extensions["stealing"] = self
        self.scheduler.events["stealing"] = deque(maxlen=100000)
        self.count = 0
        # { task state: <stealing info dict> }
        self.in_flight = dict()
        # { worker state: occupancy }
        self.in_flight_occupancy = defaultdict(lambda: 0)

        self.scheduler.stream_handlers["steal-response"] = self.move_task_confirm

    def log(self, msg):
        return self.scheduler.log_event("stealing", msg)

    def add_worker(self, scheduler=None, worker=None):
        self.stealable[worker] = [set() for i in range(15)]

    def remove_worker(self, scheduler=None, worker=None):
        del self.stealable[worker]

    def teardown(self):
        self._pc.stop()

    def transition(
        self, key, start, finish, compute_start=None, compute_stop=None, *args, **kwargs
    ):
        if finish == "processing":
            ts = self.scheduler.tasks[key]
            self.put_key_in_stealable(ts)
        elif start == "processing":
            ts = self.scheduler.tasks[key]
            self.remove_key_from_stealable(ts)
            if finish != "memory":
                self.in_flight.pop(ts, None)

    def put_key_in_stealable(self, ts):
        cost_multiplier, level = self.steal_time_ratio(ts)
        if cost_multiplier is not None:
            ws = ts.processing_on
            worker = ws.address
            self.stealable_all[level].add(ts)
            self.stealable[worker][level].add(ts)
            self.key_stealable[ts] = (worker, level)

    def remove_key_from_stealable(self, ts):
        result = self.key_stealable.pop(ts, None)
        if result is None:
            return

        worker, level = result
        try:
            self.stealable[worker][level].remove(ts)
        except KeyError:
            pass
        try:
            self.stealable_all[level].remove(ts)
        except KeyError:
            pass

    def steal_time_ratio(self, ts):
        """The compute to communication time ratio of a key

        Returns
        -------
        cost_multiplier: The increased cost from moving this task as a factor.
        For example a result of zero implies a task without dependencies.
        level: The location within a stealable list to place this value
        """
        if not ts.dependencies:  # no dependencies fast path
            return 0, 0

        split = ts.prefix.name
        if split in fast_tasks:
            return None, None

        ws = ts.processing_on
        compute_time = ws.processing[ts]
        if compute_time < 0.005:  # 5ms, just give up
            return None, None

        nbytes = ts.get_nbytes_deps()
        transfer_time = nbytes / self.scheduler.bandwidth + LATENCY
        cost_multiplier = transfer_time / compute_time
        if cost_multiplier > 100:
            return None, None

        level = int(round(log2(cost_multiplier) + 6))
        if level < 1:
            level = 1

        return cost_multiplier, level

    def move_task_request(self, ts, victim, thief):
        try:
            if self.scheduler.validate:
                if victim is not ts.processing_on and LOG_PDB:
                    import pdb

                    pdb.set_trace()

            key = ts.key
            self.remove_key_from_stealable(ts)
            logger.debug(
                "Request move %s, %s: %2f -> %s: %2f",
                key,
                victim,
                victim.occupancy,
                thief,
                thief.occupancy,
            )

            victim_duration = victim.processing[ts]

            thief_duration = self.scheduler.get_task_duration(
                ts
            ) + self.scheduler.get_comm_cost(ts, thief)

            self.scheduler.stream_comms[victim.address].send(
                {"op": "steal-request", "key": key}
            )

            self.in_flight[ts] = {
                "victim": victim,
                "thief": thief,
                "victim_duration": victim_duration,
                "thief_duration": thief_duration,
            }

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

    async def move_task_confirm(self, key=None, worker=None, state=None):
        try:
            try:
                ts = self.scheduler.tasks[key]
            except KeyError:
                logger.debug("Key released between request and confirm: %s", key)
                return
            try:
                d = self.in_flight.pop(ts)
            except KeyError:
                return
            thief = d["thief"]
            victim = d["victim"]
            logger.debug(
                "Confirm move %s, %s -> %s.  State: %s", key, victim, thief, state
            )

            self.in_flight_occupancy[thief] -= d["thief_duration"]
            self.in_flight_occupancy[victim] += d["victim_duration"]

            if not self.in_flight:
                self.in_flight_occupancy = defaultdict(lambda: 0)

            if ts.state != "processing" or ts.processing_on is not victim:
                old_thief = thief.occupancy
                new_thief = sum(thief.processing.values())
                old_victim = victim.occupancy
                new_victim = sum(victim.processing.values())
                thief.occupancy = new_thief
                victim.occupancy = new_victim
                self.scheduler.total_occupancy += (
                    new_thief - old_thief + new_victim - old_victim
                )
                return

            # One of the pair has left, punt and reschedule
            if (
                thief.address not in self.scheduler.workers
                or victim.address not in self.scheduler.workers
            ):
                self.scheduler.reschedule(key)
                return

            # Victim had already started execution, reverse stealing
            if state in ("memory", "executing", "long-running", None):
                self.log(("already-computing", key, victim.address, thief.address))
                self.scheduler.check_idle_saturated(thief)
                self.scheduler.check_idle_saturated(victim)

            # Victim was waiting, has given up task, enact steal
            elif state in ("waiting", "ready", "constrained"):
                self.remove_key_from_stealable(ts)
                ts.processing_on = thief
                duration = victim.processing.pop(ts)
                victim.occupancy -= duration
                self.scheduler.total_occupancy -= duration
                if not victim.processing:
                    self.scheduler.total_occupancy -= victim.occupancy
                    victim.occupancy = 0
                thief.processing[ts] = d["thief_duration"]
                thief.occupancy += d["thief_duration"]
                self.scheduler.total_occupancy += d["thief_duration"]
                self.put_key_in_stealable(ts)

                try:
                    self.scheduler.send_task_to_worker(thief.address, ts)
                except CommClosedError:
                    await self.scheduler.remove_worker(thief.address)
                self.log(("confirm", key, victim.address, thief.address))
            else:
                raise ValueError("Unexpected task state: %s" % state)
        except Exception as e:
            logger.exception(e)
            if LOG_PDB:
                import pdb

                pdb.set_trace()
            raise
        finally:
            try:
                self.scheduler.check_idle_saturated(thief)
            except Exception:
                pass
            try:
                self.scheduler.check_idle_saturated(victim)
            except Exception:
                pass

    def balance(self):
        s = self.scheduler

        def combined_occupancy(ws):
            return ws.occupancy + self.in_flight_occupancy[ws]

        def maybe_move_task(level, ts, sat, idl, duration, cost_multiplier):
            occ_idl = combined_occupancy(idl)
            occ_sat = combined_occupancy(sat)

            if occ_idl + cost_multiplier * duration <= occ_sat - duration / 2:
                self.move_task_request(ts, sat, idl)
                log.append(
                    (
                        start,
                        level,
                        ts.key,
                        duration,
                        sat.address,
                        occ_sat,
                        idl.address,
                        occ_idl,
                    )
                )
                s.check_idle_saturated(sat, occ=occ_sat)
                s.check_idle_saturated(idl, occ=occ_idl)

        with log_errors():
            i = 0
            idle = s.idle.values()
            saturated = s.saturated
            if not idle or len(idle) == len(s.workers):
                return

            log = []
            start = time()

            if not s.saturated:
                saturated = topk(10, s.workers.values(), key=combined_occupancy)
                saturated = [
                    ws
                    for ws in saturated
                    if combined_occupancy(ws) > 0.2 and len(ws.processing) > ws.nthreads
                ]
            elif len(s.saturated) < 20:
                saturated = sorted(saturated, key=combined_occupancy, reverse=True)
            if len(idle) < 20:
                idle = sorted(idle, key=combined_occupancy)

            for level, cost_multiplier in enumerate(self.cost_multipliers):
                if not idle:
                    break
                for sat in list(saturated):
                    stealable = self.stealable[sat.address][level]
                    if not stealable or not idle:
                        continue

                    for ts in list(stealable):
                        if ts not in self.key_stealable or ts.processing_on is not sat:
                            stealable.discard(ts)
                            continue
                        i += 1
                        if not idle:
                            break

                        if _has_restrictions(ts):
                            thieves = [ws for ws in idle if _can_steal(ws, ts, sat)]
                        else:
                            thieves = idle
                        if not thieves:
                            break
                        thief = thieves[i % len(thieves)]

                        duration = sat.processing.get(ts)
                        if duration is None:
                            stealable.discard(ts)
                            continue

                        maybe_move_task(
                            level, ts, sat, thief, duration, cost_multiplier
                        )

                if self.cost_multipliers[level] < 20:  # don't steal from public at cost
                    stealable = self.stealable_all[level]
                    for ts in list(stealable):
                        if not idle:
                            break
                        if ts not in self.key_stealable:
                            stealable.discard(ts)
                            continue

                        sat = ts.processing_on
                        if sat is None:
                            stealable.discard(ts)
                            continue
                        if combined_occupancy(sat) < 0.2:
                            continue
                        if len(sat.processing) <= sat.nthreads:
                            continue

                        i += 1
                        if _has_restrictions(ts):
                            thieves = [ws for ws in idle if _can_steal(ws, ts, sat)]
                        else:
                            thieves = idle
                        if not thieves:
                            continue
                        thief = thieves[i % len(thieves)]
                        duration = sat.processing[ts]

                        maybe_move_task(
                            level, ts, sat, thief, duration, cost_multiplier
                        )

            if log:
                self.log(log)
                self.count += 1
            stop = time()
            if s.digests:
                s.digests["steal-duration"].add(stop - start)

    def restart(self, scheduler):
        for stealable in self.stealable.values():
            for s in stealable:
                s.clear()

        for s in self.stealable_all:
            s.clear()
        self.key_stealable.clear()

    def story(self, *keys):
        keys = set(keys)
        out = []
        for _, L in self.scheduler.get_event("stealing"):
            if not isinstance(L, list):
                L = [L]
            for t in L:
                if any(x in keys for x in t):
                    out.append(t)
        return out


def _has_restrictions(ts):
    """Determine whether the given task has restrictions and whether these
    restrictions are strict.
    """
    return not ts.loose_restrictions and (
        ts.host_restrictions or ts.worker_restrictions or ts.resource_restrictions
    )


def _can_steal(thief, ts, victim):
    """Determine whether worker ``thief`` can steal task ``ts`` from worker
    ``victim``.

    Assumes that `ts` has some restrictions.
    """
    if (
        ts.host_restrictions
        and get_address_host(thief.address) not in ts.host_restrictions
    ):
        return False
    elif ts.worker_restrictions and thief.address not in ts.worker_restrictions:
        return False

    if victim.resources is None:
        return True

    for resource, value in victim.resources.items():
        try:
            supplied = thief.resources[resource]
        except KeyError:
            return False
        else:
            if supplied < value:
                return False
    return True


fast_tasks = {"shuffle-split"}

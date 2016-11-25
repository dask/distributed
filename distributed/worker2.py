from collections import defaultdict, deque
import heapq
import random

from tornado import gen
from tornado.iostream import StreamClosedError

from .batched import BatchedSend
from .core import read, write, connect, close, send_recv, error_message
from .utils import log_errors, validate_key

from .worker import (Worker, funcname, convert_args_to_str,
        convert_kwargs_to_str, logger, pack_data, apply_function)


import psutil
process = psutil.Process()


class Worker2(Worker):
    def __init__(self, *args, **kwargs):
        with log_errors():
            self.tasks = dict()
            self.dependencies = dict()
            self.dependents = dict()
            self.waiting_for_data = dict()
            self.who_has = dict()

            self.data_needed = deque()

            self.has_what = defaultdict(deque)
            self.in_flight = dict()
            self.total_connections = 10
            self.connections = {}

            self.nbytes = dict()
            self.priorities = dict()
            self.durations = dict()

            self.heap = list()
            self.executing = set()

            self.batched_stream = None
            self.target_message_size = 10e6  # 10 MB

            self.log = deque(maxlen=100000)
            self.validate = True

            Worker.__init__(self, *args, **kwargs)

    ################
    # Update Graph #
    ################

    @gen.coroutine
    def compute_stream(self, stream):
        with log_errors():
            assert not self.batched_stream
            self.batched_stream = BatchedSend(interval=2, loop=self.loop)
            self.batched_stream.start(stream)

            closed = False

            while not closed:
                try:
                    msgs = yield read(stream)
                except StreamClosedError:
                    if self.reconnect:
                        break
                    else:
                        yield self._close(report=False)
                    break

                for msg in msgs:
                    op = msg.pop('op', None)
                    if 'key' in msg:
                        validate_key(msg['key'])
                    if op == 'close':
                        closed = True
                        break
                    elif op == 'compute-task':
                        self.add_task(**msg)
                    else:
                        logger.warning("Unknown operation %s, %s", op, msg)

                self.ensure_communicating()
                self.ensure_computing()

            yield self.batched_stream.close()
            logger.info('Close compute stream')

    def add_task(self, key, function=None, args=None, kwargs=None, task=None,
            who_has=None, nbytes=None, priority=None, duration=None):
        with log_errors():
            self.log.append(('add-task', key))
            try:
                self.tasks[key] = self._deserialize(function, args, kwargs, task)
            except Exception as e:
                logger.warn("Could not deserialize task", exc_info=True)
                emsg = error_message(e)
                emsg['key'] = key
                self.batched_stream.send(emsg)
                return

            self.priorities[key] = priority
            self.durations[key] = duration

            if nbytes:
                self.nbytes.update(nbytes)

            if who_has:
                self.dependencies[key] = set(who_has)
                for dep in who_has:
                    if dep in self.dependents:
                        self.dependents[dep].add(key)
                    else:
                        self.dependents[dep] = {key}
                who_has = {dep: v for dep, v in who_has.items() if dep not in self.data}
                self.waiting_for_data[key] = set(who_has)
            else:
                self.dependencies[key] = set()
                self.waiting_for_data[key] = set()

            if who_has:
                for dep, workers in who_has.items():
                    if dep in self.who_has:
                        self.who_has[dep].update(workers)
                    else:
                        self.who_has[dep] = set(workers)

                    for worker in workers:
                        self.has_what[worker].append(dep)

                self.data_needed.append(key)
            else:
                self.task_ready(key)

    ##########################
    # Gather Data from Peers #
    ##########################

    def ensure_communicating(self):
        with log_errors():
            while self.data_needed and len(self.connections) < self.total_connections:
                key = self.data_needed[0]
                deps = self.dependencies[key]
                deps = {d for d in deps
                          if d not in self.data
                          and d not in self.in_flight}

                n = self.total_connections - len(self.connections)

                self.log.append(('gather-key', key, deps))

                for dep in list(deps)[:n]:
                    self.gather_dep(dep)

                if n >= len(deps):
                    self.data_needed.popleft()

    @gen.coroutine
    def gather_dep(self, dep):
        with log_errors():
            if not self.who_has[dep]:
                # TODO: ask scheduler nicely for new who_has before canceling
                for key in list(self.dependents[dep]):
                    if dep in self.waiting_for_data.get(key, ()):
                        self.cancel_key(key)
                return

            worker = random.choice(list(self.who_has[dep]))
            deps = {dep}
            total_bytes = self.nbytes[dep]
            L = self.has_what[worker]

            while L:
                d = L.popleft()
                if d in self.data or d in self.in_flight:
                    continue
                if total_bytes + self.nbytes[d] > self.target_message_size:
                    break
                deps.add(d)
                total_bytes += self.nbytes[d]

            ip, port = worker.split(':')
            future = connect(ip, int(port))
            self.connections[future] = True
            stream = yield future
            self.connections[stream] = deps
            del self.connections[future]
            for d in deps:
                if d in self.in_flight:
                    self.in_flight[d].add(stream)
                else:
                    self.in_flight[d] = {stream}
            self.log.append(('request-dep', dep, worker, deps))
            response = yield send_recv(stream, op='get_data', keys=list(deps),
                                       close=True)
            self.log.append(('receive-dep', dep, worker, list(response)))
            stream.close()
            del self.connections[stream]

            assert len(self.connections) < self.total_connections

            for d in deps:
                self.in_flight[d].remove(stream)
                if not self.in_flight[d]:
                    del self.in_flight[d]

            for d, v in response.items():
                if d not in self.data:
                    self.data[d] = v
                for key in self.dependents[d]:
                    if key in self.waiting_for_data:
                        if d in self.waiting_for_data[key]:
                            self.waiting_for_data[key].remove(d)
                        if not self.waiting_for_data[key]:
                            del self.waiting_for_data[key]
                            self.task_ready(key)

            self.scheduler.add_keys(address=self.address, keys=list(response))

            for d in deps:
                if d not in response and d in self.dependents:
                    self.log.append(('missing-dep', d))
                    if dep == d:  # high priority dependence, go immediately
                        try:
                            self.who_has[dep].remove(worker)
                        except KeyError:  # TODO: why does this sometimes fail
                            pass
                        self.gather_dep(dep)
                    else:
                        for key in self.dependents[d]:
                            self.data_needed.append(key)

            self.ensure_communicating()

    @gen.coroutine
    def query_who_has(self, *deps):
        with log_errors():
            response = yield self.scheduler.who_has(keys=deps)
            self.update_who_has(response)
            raise gen.Return(response)

    def update_who_has(self, who_has):
        with log_errors():
            for dep, workers in who_has.items():
                if dep in self.who_has:
                    self.who_has[dep].update(workers)
                else:
                    self.who_has[dep] = set(workers)

                for worker in workers:
                    self.has_what[worker].append(dep)

    def cancel_key(self, key):
        with log_errors():
            if key in self.waiting_for_data:
                missing = [dep for dep in self.dependencies[key]
                           if dep not in self.data
                           and not self.who_has.get(dep)]
                self.log.append(('report-missing-data', key, missing))
                self.batched_stream.send({'status': 'missing-data',
                                          'key': key,
                                          'keys': missing})
            del self.tasks[key]
            del self.waiting_for_data[key]
            for dep in self.dependencies.pop(key):
                self.dependents[dep].remove(key)
                if not self.dependents[dep]:
                    del self.dependents[dep]

    ################
    # Execute Task #
    ################

    def task_ready(self, key):
        with log_errors():
            assert not self.waiting_for_data.pop(key, None)
            if not all(dep in self.data for dep in self.dependencies[key]):
                import pdb; pdb.set_trace()

            heapq.heappush(self.heap, (self.priorities[key], key))

            self.ensure_computing()

    def ensure_computing(self):
        with log_errors():
            while self.heap and len(self.executing) < self.ncores:
                _, key = heapq.heappop(self.heap)
                self.executing.add(key)
                self.loop.add_callback(self.execute, key)

    @gen.coroutine
    def execute(self, key, report=False):
        with log_errors():
            self.executing.add(key)
            function, args, kwargs = self.tasks[key]

            diagnostics = {}  # TODO

            args2 = pack_data(args, self.data)
            kwargs2 = pack_data(kwargs, self.data)

            result = yield self.executor_submit(key, apply_function, function,
                                                args2, kwargs2,
                                                self.execution_state, key)

            result['key'] = key
            result.update(diagnostics)

            if result['status'] == 'OK':
                self.data[key] = result.pop('result')
                if report:  # TODO: remove?
                    response = yield self.scheduler.add_keys(keys=[key],
                                            address=(self.ip, self.port))
                    if not response == 'OK':
                        logger.warn('Could not report results to scheduler: %s',
                                    str(response))
            else:
                logger.warn(" Compute Failed\n"
                    "Function: %s\n"
                    "args:     %s\n"
                    "kwargs:   %s\n",
                    str(funcname(function))[:1000],
                    convert_args_to_str(args, max_len=1000),
                    convert_kwargs_to_str(kwargs, max_len=1000), exc_info=True)

            logger.debug("Send compute response to scheduler: %s, %s", key,
                         result)

            self.batched_stream.send(result)

            self.executing.remove(key)
            self.ensure_computing()

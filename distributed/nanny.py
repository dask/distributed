from __future__ import print_function, division, absolute_import

import atexit
from datetime import datetime, timedelta
from functools import partial
import logging
from multiprocessing.queues import Empty
import os
import shutil
import threading
from time import sleep
import sys
import weakref

from tornado.ioloop import IOLoop, TimeoutError
from tornado import gen

from .comm import get_address_host, get_local_address_for
from .core import rpc, RPCClosed, CommClosedError, coerce_to_address
from .metrics import disk_io_counters, net_io_counters, time
from .node import ServerNode
from .process import AsyncProcess
from .security import Security
from .utils import get_ip, ignoring, mp_context, log_errors, silence_logging
from .worker import _ncores, run


logger = logging.getLogger(__name__)


class Nanny(ServerNode):
    """ A process to manage worker processes

    The nanny spins up Worker processes, watches then, and kills or restarts
    them as necessary.
    """
    process = None
    status = None

    def __init__(self, scheduler_ip, scheduler_port=None, worker_port=0,
                 ncores=None, loop=None, local_dir=None, services=None,
                 name=None, memory_limit='auto', reconnect=True,
                 validate=False, quiet=False, resources=None, silence_logs=None,
                 death_timeout=None, preload=(), security=None, **kwargs):
        if scheduler_port is None:
            self.scheduler_addr = coerce_to_address(scheduler_ip)
        else:
            self.scheduler_addr = coerce_to_address((scheduler_ip, scheduler_port))
        self._given_worker_port = worker_port
        self.ncores = ncores or _ncores
        self.reconnect = reconnect
        self.validate = validate
        self.resources = resources
        self.death_timeout = death_timeout
        self.preload = preload

        self.security = security or Security()
        assert isinstance(self.security, Security)
        self.connection_args = self.security.get_connection_args('worker')
        self.listen_args = self.security.get_listen_args('worker')

        self.local_dir = local_dir

        self.loop = loop or IOLoop.current()
        self.scheduler = rpc(self.scheduler_addr, connection_args=self.connection_args)
        self.services = services
        self.name = name
        self.memory_limit = memory_limit
        self.quiet = quiet
        self.should_watch = True

        if silence_logs:
            silence_logging(level=silence_logs)
        self.silence_logs = silence_logs

        handlers = {'instantiate': self.instantiate,
                    'kill': self._kill,
                    'restart': self.restart,
                    'terminate': self._close,
                    #'monitor_resources': self.monitor_resources,
                    'run': self.run}

        #self._reset()

        super(Nanny, self).__init__(handlers, io_loop=self.loop,
                                    connection_args=self.connection_args,
                                    **kwargs)

        self.status = 'init'

    def __str__(self):
        return "<Nanny: %s, threads: %d>" % (self.worker_address, self.ncores)

    __repr__ = __str__

    @gen.coroutine
    def _unregister(self, timeout=10):
        if self.process is None:
            return
        worker_address = self.process.worker_address
        if worker_address is None:
            return

        allowed_errors = (gen.TimeoutError, CommClosedError, EnvironmentError, RPCClosed)
        try:
            yield gen.with_timeout(timedelta(seconds=timeout),
                                   self.scheduler.unregister(address=self.worker_address),
                                   quiet_exceptions=allowed_errors)
        except allowed_errors:
            pass

    @property
    def worker_address(self):
        return None if self.process is None else self.process.worker_address

    @property
    def worker_dir(self):
        return None if self.process is None else self.process.worker_dir

    @gen.coroutine
    def _start(self, addr_or_port=0):
        """ Start nanny, start local process, start watching """

        # XXX Factor this out
        if not addr_or_port:
            # Default address is the required one to reach the scheduler
            self.listen(get_local_address_for(self.scheduler.address),
                        listen_args=self.listen_args)
            self.ip = get_address_host(self.address)
        elif isinstance(addr_or_port, int):
            # addr_or_port is an integer => assume TCP
            self.ip = get_ip(
                get_address_host(self.scheduler.address)
            )
            self.listen((self.ip, addr_or_port),
                        listen_args=self.listen_args)
        else:
            self.listen(addr_or_port, listen_args=self.listen_args)
            self.ip = get_address_host(self.address)

        logger.info('        Start Nanny at: %r', self.address)
        response = yield self.instantiate()
        if response == 'OK':
            #self.loop.add_callback(self._watch)
            assert self.worker_address
            self.status = 'running'

    def start(self, addr_or_port=0):
        self.loop.add_callback(self._start, addr_or_port)

    @gen.coroutine
    def _kill(self, comm=None, timeout=10):
        """ Kill the local worker process

        Blocks until both the process is down and the scheduler is properly
        informed
        """
        if self.process is None:
            raise gen.Return('OK')

        deadline = self.loop.time() + timeout
        yield self.process.kill(grace_delay=timeout)
        yield self._unregister(deadline - self.loop.time())

    @gen.coroutine
    def instantiate(self, comm=None):
        """ Start a local worker process

        Blocks until the process is up and the scheduler is properly informed
        """
        if self.process is None:
            self.process = WorkerProcess(
                self.scheduler_addr,
                ncores=self.ncores, nanny_port=self.port,
                worker_port=self._given_worker_port,
                local_dir=self.local_dir, silence_logs=self.silence_logs)
                #-                kwargs={'services': self.services,
                #-                        'name': self.name,
                #-                        'memory_limit': self.memory_limit,
                #-                        'reconnect': self.reconnect,
                #-                        'resources': self.resources,
                #-                        'validate': self.validate,
                #-                        'silence_logs': self.silence_logs,
                #-                        'death_timeout': self.death_timeout,
                #-                        'preload': self.preload,
                #-                        'security': self.security})

        yield self.process.start()
        yield self.process.wait_until_running()
        raise gen.Return('OK')

    @gen.coroutine
    def restart(self, comm=None):
        #self.should_watch = False
        if self.process is not None:
            yield self._kill()
        yield self.instantiate()
        #self.should_watch = True
        raise gen.Return('OK')

    def is_alive(self):
        return self.process is not None and self.process.is_alive()

    def run(self, *args, **kwargs):
        return run(self, *args, **kwargs)

    #@gen.coroutine
    #def _watch(self, wait_seconds=0.20):
        #""" Watch the local process, if it dies then spin up a new one """
        #while True:
            #if closing[0] or self.status == 'closed':
                #yield self._close()
                #break
            #elif self.should_watch and self.process and not isalive(self.process):
                #logger.warning("Discovered failed worker")
                #self.cleanup()
                #try:
                    #yield self.scheduler.unregister(address=self.worker_address)
                #except (EnvironmentError, CommClosedError):
                    #if self.reconnect:
                        #yield gen.sleep(wait_seconds)
                    #else:
                        #yield self._close()
                        #break
                #if self.status != 'closed':
                    #logger.warning('Restarting worker...')
                    #yield self.instantiate()
            #else:
                #yield gen.sleep(wait_seconds)

    @gen.coroutine
    def _close(self, comm=None, timeout=5, report=None):
        """
        Close the nanny process, stop listening.
        """
        if self.status == 'closed':
            raise gen.Return('OK')
        logger.info("Closing Nanny at %r", self.address)
        self.status = 'closed'
        try:
            if self.process is not None:
                yield self._kill(timeout=timeout)
        finally:
        #with ignoring(gen.TimeoutError):
            #yield self._kill(timeout=timeout)
            self.rpc.close()
            self.scheduler.close_rpc()
            self.stop()
        raise gen.Return('OK')

    #def resource_collect(self):
        #try:
            #import psutil
        #except ImportError:
            #return {}
        #p = psutil.Process(self.process.pid)
        #return {'timestamp': datetime.now().isoformat(),
                #'cpu_percent': psutil.cpu_percent(),
                #'status': p.status(),
                #'memory_percent': p.memory_percent(),
                #'memory_info': p.memory_info()._asdict(),
                #'disk_io_counters': disk_io_counters()._asdict(),
                #'net_io_counters': net_io_counters()._asdict()}

    #@gen.coroutine
    #def monitor_resources(self, comm, interval=1):
        #while not comm.closed():
            #if self.process:
                #yield comm.write(self.resource_collect())
            #yield gen.sleep(interval)


class WorkerProcess(object):

    def __init__(self, scheduler_addr, ncores, nanny_port,
                 worker_port, local_dir, silence_logs,
                 **worker_kwargs):
        self.status = 'init'
        self.scheduler_addr = scheduler_addr
        self.silence_logs = silence_logs
        self.worker_port = worker_port
        worker_kwargs.update(ncores=ncores,
                             service_ports={'nanny': nanny_port},
                             local_dir=local_dir,
                             silence_logs=silence_logs)
        self.worker_kwargs = worker_kwargs

        # Initialized when worker is ready
        self.worker_dir = None
        self.worker_address = None

    # XXX allow setting worker death callback

    @gen.coroutine
    def start(self):
        if self.status in ('starting', 'running'):
            raise ValueError("Worker already started. Please kill first")

        self.init_result_q = mp_context.Queue()
        self.child_stop_q = mp_context.Queue()
        self.process = AsyncProcess(
            target=self.run,
            kwargs=dict(worker_args=(self.scheduler_addr,),
                        worker_kwargs=self.worker_kwargs,
                        worker_start_args=(self.worker_port,),
                        silence_logs=self.silence_logs,
                        init_result_q=self.init_result_q,
                        child_stop_q=self.child_stop_q,)
            )
        self.process.daemon = True   # do we want this?
        self.status = 'starting'
        yield self.process.start()

    def _death_message(self, pid, exitcode):
        assert exitcode is not None
        if exitcode == 255:
            return "Worker process %d was killed by unsigned signal" % (pid,)
        elif exitcode >= 0:
            return "Worker process %d exited with status %d" % (pid, exitcode,)
        else:
            return "Worker process %d was killed by signal %d" % (pid, -exitcode,)

    def assert_alive(self):
        r = self.process.exitcode
        if r is not None:
            raise ValueError(self._death_message(self.process.pid, r))

    def is_alive(self):
        return self.process.is_alive()

    def mark_stopped(self):
        assert self.process.exitcode is not None
        self.status = 'stopped'
        # Best effort to clean up worker directory
        if self.worker_dir and os.path.exists(self.worker_dir):
            shutil.rmtree(self.worker_dir, ignore_errors=True)
        self.worker_dir = None

    @gen.coroutine
    def wait_until_running(self):
        if self.status == 'running':
            return
        elif self.status != 'starting':
            raise ValueError("Worker not started")

        delay = 0.05
        while True:
            self.assert_alive()
            try:
                msg = self.init_result_q.get_nowait()
            except Empty:
                yield gen.sleep(delay)
                continue
            self.status = 'running'

            if isinstance(msg, Exception):
                yield self.process.join()
                self.mark_stopped()
                raise msg
            else:
                self.worker_address = msg['address']
                self.worker_dir = msg['dir']
                assert self.worker_address
                raise gen.Return(msg)

    @gen.coroutine
    def kill(self, grace_delay=10):
        """
        """
        loop = IOLoop.current()
        deadline = loop.time() + grace_delay

        if self.status == 'starting':
            try:
                yield self.wait_until_running()
            except Exception:
                logger.warning("Worker failed starting", exc_info=True)
        if self.status == 'stopped':
            return
        assert self.status == 'running'

        print("kill A")
        self.child_stop_q.put({'op': 'stop',
                               'timeout': max(0, deadline - loop.time()) * 0.8,
                               })

        print("kill B")
        while self.is_alive() and loop.time() < deadline:
            yield gen.sleep(0.05)
        print("kill C")

        if self.is_alive():
            logger.warning("Worker process still alive after %d seconds, killing",
                           grace_delay)
            try:
                print("kill D")
                yield self.process.terminate()
            except Exception as e:
                logger.error("Failed to kill worker process: %s", e)
            print("kill E")

        self.mark_stopped()

    @classmethod
    def run(cls, worker_args, worker_kwargs, worker_start_args,
            silence_logs, init_result_q, child_stop_q):  # pragma: no cover
        from distributed import Worker

        try:
            from dask.multiprocessing import initialize_worker_process
        except ImportError:   # old Dask version
            pass
        else:
            initialize_worker_process()

        if silence_logs:
            logger.setLevel(silence_logs)

        IOLoop.clear_instance()
        loop = IOLoop()
        loop.make_current()
        worker = Worker(*worker_args, **worker_kwargs)

        @gen.coroutine
        def do_stop(timeout):
            try:
                yield worker._close(report=False, nanny=False)
            finally:
                loop.stop()

        def watch_stop_q():
            while True:
                try:
                    msg = child_stop_q.get(timeout=1000)
                except Empty:
                    pass
                else:
                    assert msg['op'] == 'stop'
                    do_stop(msg['timeout'])
                    break

        t = threading.Thread(target=watch_stop_q)
        t.daemon = True
        t.start()

        @gen.coroutine
        def run():
            """
            Try to start worker and inform parent of outcome.
            """
            try:
                yield worker._start(*worker_start_args)
            except Exception as e:
                logger.exception(e)
                init_result_q.put(e)
            else:
                assert worker.port
                init_result_q.put({'address': worker.address,
                                   'dir': worker.local_dir})
                yield worker.wait_until_closed()
                logger.info("Worker closed")

        try:
            loop.run_sync(run)
        except KeyboardInterrupt:
            sys.exit(1)
        except TimeoutError:
            # Loop was stopped before wait_until_closed() returned, ignore
            pass
        finally:
            loop.stop()
            loop.close()


#def run_worker_fork(q, scheduler_addr, ncores, nanny_port,
                    #worker_ip, worker_port, local_dir, silence_logs,
                    #**kwargs):
    #"""
    #Create a worker in a forked child.
    #"""
    #from distributed import Worker  # pragma: no cover
    #from tornado.ioloop import IOLoop  # pragma: no cover

    #try:
        #from dask.multiprocessing import initialize_worker_process
    #except ImportError:   # old Dask version
        #pass
    #else:
        #initialize_worker_process()

    #if silence_logs:
        #logger.setLevel(silence_logs)

    #IOLoop.clear_instance()  # pragma: no cover
    #loop = IOLoop()  # pragma: no cover
    #loop.make_current()  # pragma: no cover
    #worker = Worker(scheduler_addr, ncores=ncores,
                    #service_ports={'nanny': nanny_port},
                    #local_dir=local_dir, silence_logs=silence_logs,
                    #**kwargs)  # pragma: no cover

    #@gen.coroutine  # pragma: no cover
    #def run():
        #try:  # pragma: no cover
            #yield worker._start(worker_port)  # pragma: no cover
        #except Exception as e:  # pragma: no cover
            #logger.exception(e)  # pragma: no cover
            #q.put(e)  # pragma: no cover
        #else:
            #assert worker.port  # pragma: no cover
            #q.put({'address': worker.address, 'dir': worker.local_dir})  # pragma: no cover

        #yield worker.wait_until_closed()

        #logger.info("Worker closed")
    #try:
        #loop.run_sync(run)
    #except TimeoutError:
        #logger.info("Worker timed out")
    #except KeyboardInterrupt:
        #pass
    #finally:
        #loop.stop()
        #loop.close()


#def isalive(proc):
    #return proc is not None and proc.is_alive()


#def join(proc, timeout):
    #if proc is not None:
        #proc.join(timeout)


#processes_to_close = weakref.WeakSet()


#@atexit.register
#def _closing():
    #for proc in processes_to_close:
        #try:
            #proc.terminate()
        #except OSError:
            #pass

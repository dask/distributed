from __future__ import print_function, division, absolute_import

from datetime import timedelta
import logging
from sys import argv, exit
import socket

import click
from distributed import Nanny, Worker, sync, rpc
from distributed.utils import get_ip, All
from distributed.worker import _ncores
from distributed.http import HTTPWorker
from distributed.cli.utils import check_python_3
from tornado.ioloop import IOLoop
from tornado import gen

logger = logging.getLogger('distributed.dask_worker')

import signal


def handle_signal(sig, frame):
    loop = IOLoop.instance()
    if loop._running:
        loop.add_callback(loop.stop)
    else:
        exit(1)


@click.command()
@click.argument('scheduler', type=str)
@click.option('--worker-port', type=int, default=0,
              help="Serving worker port, defaults to randomly assigned")
@click.option('--http-port', type=int, default=0,
              help="Serving http port, defaults to randomly assigned")
@click.option('--nanny-port', type=int, default=0,
              help="Serving nanny port, defaults to randomly assigned")
@click.option('--port', type=int, default=0,
              help="Deprecated, see --nanny-port")
@click.option('--host', type=str, default=None,
              help="Serving host. Defaults to an ip address that can hopefully"
                   " be visible from the scheduler network.")
@click.option('--nthreads', type=int, default=0,
              help="Number of threads per process. Defaults to number of cores")
@click.option('--nprocs', type=int, default=1,
              help="Number of worker processes.  Defaults to one.")
@click.option('--name', type=str, default='', help="Alias")
@click.option('--no-nanny', is_flag=True)
def main(scheduler, host, worker_port, http_port, nanny_port, nthreads, nprocs,
        no_nanny, name, port):
    if port:
        logger.info("--port is deprecated, use --nanny-port instead")
        assert not nanny_port
        nanny_port = port
    try:
        scheduler_host, scheduler_port = scheduler.split(':')
        scheduler_ip = socket.gethostbyname(scheduler_host)
        scheduler_port = int(scheduler_port)
    except IndexError:
        logger.info("Usage:  dask-worker scheduler_host:scheduler_port")

    if nprocs > 1 and worker_port != 0:
        logger.error("Failed to launch worker.  You cannot use the --port argument when nprocs > 1.")
        exit(1)

    if nprocs > 1 and name:
        logger.error("Failed to launch worker.  You cannot use the --name argument when nprocs > 1.")
        exit(1)

    if not nthreads:
        nthreads = _ncores // nprocs

    services = {('http', http_port): HTTPWorker}

    loop = IOLoop.current()

    if no_nanny:
        kwargs = {}
        t = Worker
    else:
        kwargs = {'worker_port': worker_port}
        t = Nanny

    if host is not None:
        ip = socket.gethostbyname(host)
    else:
        # lookup the ip address of a local interface on a network that
        # reach the scheduler
        ip = get_ip(scheduler_ip, scheduler_port)
    nannies = [t(scheduler_ip, scheduler_port, ncores=nthreads, ip=ip,
                 services=services, name=name, loop=loop, **kwargs)
               for i in range(nprocs)]

    for nanny in nannies:
        nanny.start(nanny_port)

    loop.start()
    logger.info("End worker")
    loop.close()

    loop2 = IOLoop()

    @gen.coroutine
    def f():
        scheduler = rpc(ip=nannies[0].center.ip, port=nannies[0].center.port)
        yield gen.with_timeout(timedelta(seconds=2),
                All([scheduler.unregister(address=n.worker_address, close=True)
                    for n in nannies if n.process]), io_loop=loop2)

    loop2.run_sync(f)

    for n in nannies:
        n.process.terminate()

    for n in nannies:
        n.process.join(timeout=1)

    for nanny in nannies:
        nanny.stop()


def go():
    # NOTE: We can't use the generic install_signal_handlers() function from
    # distributed.cli.utils because we're handling the signal differently.
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    check_python_3()
    main()

if __name__ == '__main__':
    go()

from __future__ import print_function, division, absolute_import

import atexit
import logging
import os
import socket
import sys

import bokeh
import distributed.bokeh
from toolz import get_in, concat

from ..utils import ignoring
from ..compatibility import logging_names
from ..config import config


dirname = os.path.dirname(distributed.__file__)
paths = [os.path.join(dirname, 'bokeh', name)
         for name in ['background', 'status', 'tasks', 'workers',
             'memory-usage.py', 'task-stream.py', 'task-progress.py',
             'resource-profiles.py', 'worker-table.py', 'processing-stacks.py']]

logger = logging.getLogger(__name__)

dask_dir = os.path.join(os.path.expanduser('~'), '.dask')
if not os.path.exists(dask_dir):
    os.mkdir(dask_dir)

logging_level = get_in(['logging', 'bokeh'], config, 'critical')
logging_level = logging_names[logging_level.upper()]


class BokehWebInterface(object):

    process = None

    def __init__(self, host='127.0.0.1', http_port=9786, bokeh_port=8787,
                 scheduler_address='tcp://127.0.0.1:8786',
                 bokeh_whitelist=[], log_level=logging_level,
                 show=False, prefix=None, use_xheaders=False, quiet=True):
        self.port = bokeh_port
        ip = socket.gethostbyname(host)

        hosts = ['localhost',
                 '127.0.0.1',
                 ip,
                 host]

        with ignoring(Exception):
            hosts.append(socket.gethostbyname(ip))
        with ignoring(Exception):
            hosts.append(socket.gethostbyname(socket.gethostname()))

        hosts = ['%s:%d' % (h, bokeh_port) for h in hosts]

        hosts.append("*")

        hosts.extend(map(str, bokeh_whitelist))

        args = ([sys.executable, '-m', 'bokeh', 'serve'] + paths +
                ['--check-unused-sessions=50',
                 '--unused-session-lifetime=1',
                 '--allow-websocket-origin=*',
                 '--port', str(bokeh_port)])
        if bokeh.__version__ <= '0.12.4':
            args += sum([['--host', h] for h in hosts], [])

        if prefix:
            args.extend(['--prefix', prefix])

        if show:
            args.append('--show')

        if use_xheaders:
            args.append('--use-xheaders')

        if log_level in ('debug', 'info', 'warning', 'error', 'critical'):
            args.extend(['--log-level', log_level])

        bokeh_options = {'host': host,
                         'http-port': http_port,
                         'scheduler-address': scheduler_address,
                         'bokeh-port': bokeh_port}

        args.extend(['--args'] + list(map(str, concat(bokeh_options.items()))))

        import subprocess
        process = subprocess.Popen(args)
        self.process = process

        @atexit.register
        def cleanup_process():
            try:
                process.terminate()
            except OSError:
                pass

        if not quiet:
            logger.info("Web UI: http://%s:%d/status/"
                         % (ip, bokeh_port))

    def close(self, join=True, timeout=None):
        if self.process is not None and self.process.poll() is None:
            self.process.terminate()

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def bokeh_main(args):
    from bokeh.command.bootstrap import main
    main(args)

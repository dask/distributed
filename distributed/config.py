from __future__ import print_function, division, absolute_import

import logging
import logging.config
import os
import sys

import dask
import yaml

from .compatibility import logging_names


fn = os.path.join(os.path.dirname(__file__), 'distributed.yaml')
dask.config.ensure_config_file(source=fn)

with open(fn) as f:
    defaults = yaml.load(f)

dask.config.update(dask.config.config, defaults, priority='old')

aliases = {
    'allowed-failures': 'scheduler.allowed-failures',
    'bandwidth': 'scheduler.bandwidth',
    'default-data-size': 'scheduler.default-data-size',
    'transition-log-length': 'scheduler.transition-log-length',
    'work-stealing': 'scheduler.work-stealing',
    'worker-ttl': 'scheduler.worker-ttl',

    'multiprocessing-method': 'worker.multiprocessing-method',
    'use-file-locking': 'worker.use-file-locking',
    'profile-interval': 'worker.profile.interval',
    'profile-cycle-interval': 'worker.profile.cycle',
    'worker-memory-target': 'worker.memory.target',
    'worker-memory-spill': 'worker.memory.spill',
    'worker-memory-pause': 'worker.memory.pause',
    'worker-memory-terminate': 'worker.memory.terminate',

    'heartbeat-interval': 'client.heartbeat',

    'compression': 'comm.compression',
    'connect-timeout': 'comm.timeouts.connect',
    'tcp-timeout': 'comm.timeouts.tcp',
    'default-scheme': 'comm.default-scheme',
    'socket-backlog': 'comm.socket-backlog',
    'recent-messages-log-length': 'comm.recent-messages-log-length',

    'diagnostics-link': 'dashboard.link',
    'bokeh-export-tool': 'dashboard.export-tool',

    'tick-time': 'admin.tick.interval',
    'tick-maximum-delay': 'admin.tick.limit',
    'log-length': 'admin.log-length',
    'log-format': 'admin.log-format',
    'pdb-on-err': 'admin.pdb-on-err',
}

dask.config.rename(aliases)


#########################
# Logging specific code #
#########################
#
# Here we enact the policies in the logging part of the configuration

logger = logging.getLogger(__name__)


def _initialize_logging_old_style(config):
    """
    Initialize logging using the "old-style" configuration scheme, e.g.:
        {
        'logging': {
            'distributed': 'info',
            'tornado': 'critical',
            'tornado.application': 'error',
            }
        }
    """
    loggers = {  # default values
        'distributed': 'info',
        'distributed.client': 'warning',
        'bokeh': 'critical',
        'tornado': 'critical',
        'tornado.application': 'error',
    }
    loggers.update(config.get('logging', {}))

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter(dask.config.get('admin.log-format',
                                                           config=config)))
    for name, level in loggers.items():
        if isinstance(level, str):
            level = logging_names[level.upper()]
        logger = logging.getLogger(name)
        logger.setLevel(level)
        logger.handlers[:] = []
        logger.addHandler(handler)
        logger.propagate = False


def _initialize_logging_new_style(config):
    """
    Initialize logging using logging's "Configuration dictionary schema".
    (ref.: https://docs.python.org/2/library/logging.config.html#logging-config-dictschema)
    """
    logging.config.dictConfig(config.get('logging'))


def _initialize_logging_file_config(config):
    """
    Initialize logging using logging's "Configuration file format".
    (ref.: https://docs.python.org/2/library/logging.config.html#configuration-file-format)
    """
    logging.config.fileConfig(config.get('logging-file-config'), disable_existing_loggers=False)


def initialize_logging(config):
    if 'logging-file-config' in config:
        if 'logging' in config:
            raise RuntimeError("Config options 'logging-file-config' and 'logging' are mutually exclusive.")
        _initialize_logging_file_config(config)
    else:
        log_config = config.get('logging', {})
        if 'version' in log_config:
            # logging module mandates version to be an int
            log_config['version'] = int(log_config['version'])
            _initialize_logging_new_style(config)
        else:
            _initialize_logging_old_style(config)


initialize_logging(dask.config.config)

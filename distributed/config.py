from __future__ import print_function, division, absolute_import

import logging
import logging.config
import os
import sys

from dask.config import config, ensure_config_file, update
import yaml

from .compatibility import logging_names


fn = os.path.join(os.path.dirname(__file__), 'config.yaml')
ensure_config_file(source=fn)


with open(fn) as f:
    defaults = yaml.load(f)


update(config, defaults, priority='old')


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
    handler.setFormatter(logging.Formatter(config['log-format']))
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
    logging.config.dictConfig(config['logging'])


def _initialize_logging_file_config(config):
    """
    Initialize logging using logging's "Configuration file format".
    (ref.: https://docs.python.org/2/library/logging.config.html#configuration-file-format)
    """
    logging.config.fileConfig(config['logging-file-config'], disable_existing_loggers=False)


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


initialize_logging(config)

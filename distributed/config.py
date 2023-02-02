from __future__ import annotations

import asyncio
import heapq
import logging
import logging.config
import os
import sys
from collections.abc import Callable, Hashable
from typing import Any

import yaml

import dask
from dask.utils import import_required, parse_timedelta

from distributed.compatibility import WINDOWS, logging_names
from distributed.metrics import monotonic

config = dask.config.config


fn = os.path.join(os.path.dirname(__file__), "distributed.yaml")

with open(fn) as f:
    defaults = yaml.safe_load(f)

dask.config.update_defaults(defaults)

aliases = {
    "allowed-failures": "distributed.scheduler.allowed-failures",
    "bandwidth": "distributed.scheduler.bandwidth",
    "default-data-size": "distributed.scheduler.default-data-size",
    "transition-log-length": "distributed.scheduler.transition-log-length",
    "work-stealing": "distributed.scheduler.work-stealing",
    "worker-ttl": "distributed.scheduler.worker-ttl",
    "multiprocessing-method": "distributed.worker.multiprocessing-method",
    "use-file-locking": "distributed.worker.use-file-locking",
    "profile-interval": "distributed.worker.profile.interval",
    "profile-cycle-interval": "distributed.worker.profile.cycle",
    "worker-memory-target": "distributed.worker.memory.target",
    "worker-memory-spill": "distributed.worker.memory.spill",
    "worker-memory-pause": "distributed.worker.memory.pause",
    "worker-memory-terminate": "distributed.worker.memory.terminate",
    "heartbeat-interval": "distributed.client.heartbeat",
    "compression": "distributed.comm.compression",
    "connect-timeout": "distributed.comm.timeouts.connect",
    "tcp-timeout": "distributed.comm.timeouts.tcp",
    "default-scheme": "distributed.comm.default-scheme",
    "socket-backlog": "distributed.comm.socket-backlog",
    "recent-messages-log-length": "distributed.comm.recent-messages-log-length",
    "diagnostics-link": "distributed.dashboard.link",
    "bokeh-export-tool": "distributed.dashboard.export-tool",
    "tick-time": "distributed.admin.tick.interval",
    "tick-maximum-delay": "distributed.admin.tick.limit",
    "log-length": "distributed.admin.log-length",
    "log-format": "distributed.admin.log-format",
    "pdb-on-err": "distributed.admin.pdb-on-err",
    "ucx": "distributed.comm.ucx",
    "rmm": "distributed.rmm",
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
        "distributed": "info",
        "distributed.client": "warning",
        "bokeh": "error",
        "tornado": "critical",
        "tornado.application": "error",
    }
    base_config = _find_logging_config(config)
    loggers.update(base_config.get("logging", {}))

    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(
        logging.Formatter(
            dask.config.get("distributed.admin.log-format", config=config)
        )
    )
    for name, level in loggers.items():
        if isinstance(level, str):
            level = logging_names[level.upper()]
        logger = logging.getLogger(name)
        logger.setLevel(level)

        # Ensure that we're not registering the logger twice in this hierarchy.
        anc = None
        already_registered = False
        for ancestor in name.split("."):
            if anc is None:
                anc = logging.getLogger(ancestor)
            else:
                anc.getChild(ancestor)

            if handler in anc.handlers:
                already_registered = True
                break

        if not already_registered:
            logger.addHandler(handler)


def _initialize_logging_new_style(config):
    """
    Initialize logging using logging's "Configuration dictionary schema".
    (ref.: https://docs.python.org/3/library/logging.config.html#configuration-dictionary-schema)
    """
    base_config = _find_logging_config(config)
    logging.config.dictConfig(base_config.get("logging"))


def _initialize_logging_file_config(config):
    """
    Initialize logging using logging's "Configuration file format".
    (ref.: https://docs.python.org/3/howto/logging.html#configuring-logging)
    """
    base_config = _find_logging_config(config)
    logging.config.fileConfig(
        base_config.get("logging-file-config"), disable_existing_loggers=False
    )


def _find_logging_config(config):
    """
    Look for the dictionary containing logging-specific configurations,
    starting in the 'distributed' dictionary and then trying the top-level
    """
    logging_keys = {"logging-file-config", "logging"}
    if logging_keys & config.get("distributed", {}).keys():
        return config["distributed"]
    else:
        return config


def initialize_logging(config):
    base_config = _find_logging_config(config)
    if "logging-file-config" in base_config:
        if "logging" in base_config:
            raise RuntimeError(
                "Config options 'logging-file-config' and 'logging' are mutually exclusive."
            )
        _initialize_logging_file_config(config)
    else:
        log_config = base_config.get("logging", {})
        if "version" in log_config:
            # logging module mandates version to be an int
            log_config["version"] = int(log_config["version"])
            _initialize_logging_new_style(config)
        else:
            _initialize_logging_old_style(config)


class RateLimitedLogger:
    """A wrapper around a logger that limits how frequently a specific message
    will be logged, suppressing any others more frequent than that.

    Example
    -------
    >>> logger = RateLimitedLogger(logging.getLogger("foo"))
    >>> logger.info("hello-world", "Hello World!")
    Hello world!
    >>> logger.info("accounting", "You paid 100$")  # tracking is specific to each tag
    You paid 100$
    >>> logger.info("hello-world", "Hi again!")  # Too soon! This is muted
    >>> time.sleep(10)
    >>> logger.info("hello-world", "You there?")  # Enough time has passed
    You there?

    Tags don't need to be constant; obsolete tags are cleaned up automatically after
    they expire so that won't cause a memory leak.
    """

    logger: logging.Logger
    rate: float
    _prev_heap: list[tuple[float, Hashable]]
    _prev_set: set[Hashable]

    def __init__(self, logger: logging.Logger, rate: str | float = "10s"):
        self.logger = logger
        self.rate = parse_timedelta(rate)
        self._prev_heap = []
        self._prev_set = set()

    def _log(
        self, func: Callable, tag: Hashable, msg: str, /, *args: Any, **kwargs: Any
    ) -> None:
        now = monotonic()

        # Self-clean to avoid accumulating tags over time
        oldest = now - self.rate
        while self._prev_heap and self._prev_heap[0][0] < oldest:
            _, purge_tag = heapq.heappop(self._prev_heap)
            self._prev_set.remove(purge_tag)

        if tag not in self._prev_set:
            heapq.heappush(self._prev_heap, (now, tag))
            self._prev_set.add(tag)
            func(msg, *args, **kwargs)

    def clear(self) -> None:
        """Clear memory of recent log messages"""
        self._prev_heap.clear()
        self._prev_set.clear()

    def debug(self, tag: Hashable, msg: str, /, *args: Any, **kwargs: Any) -> None:
        self._log(self.logger.debug, tag, msg, *args, **kwargs)

    def info(self, tag: Hashable, msg: str, /, *args: Any, **kwargs: Any) -> None:
        self._log(self.logger.info, tag, msg, *args, **kwargs)

    def warning(self, tag: Hashable, msg: str, /, *args: Any, **kwargs: Any) -> None:
        self._log(self.logger.warning, tag, msg, *args, **kwargs)

    def error(self, tag: Hashable, msg: str, /, *args: Any, **kwargs: Any) -> None:
        self._log(self.logger.error, tag, msg, *args, **kwargs)

    def critical(self, tag: Hashable, msg: str, /, *args: Any, **kwargs: Any) -> None:
        self._log(self.logger.critical, tag, msg, *args, **kwargs)


def initialize_event_loop(config):
    event_loop = dask.config.get("distributed.admin.event-loop")
    if event_loop == "uvloop":
        uvloop = import_required(
            "uvloop",
            "The distributed.admin.event-loop configuration value "
            "is set to 'uvloop' but the uvloop module is not installed"
            "\n\n"
            "Please either change the config value or install one of the following\n"
            "    conda install uvloop\n"
            "    pip install uvloop",
        )
        uvloop.install()
    elif event_loop in {"asyncio", "tornado"}:
        if WINDOWS:
            # WindowsProactorEventLoopPolicy is not compatible with tornado 6
            # fallback to the pre-3.8 default of Selector
            # https://github.com/tornadoweb/tornado/issues/2608
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
    else:
        raise ValueError(
            "Expected distributed.admin.event-loop to be in ('asyncio', 'tornado', 'uvloop'), got %s"
            % dask.config.get("distributed.admin.event-loop")
        )


initialize_logging(dask.config.config)
initialize_event_loop(dask.config.config)

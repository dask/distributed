from __future__ import annotations

import contextlib
import io
import logging
import os
import subprocess
import sys
import tempfile

import pytest
import yaml

from distributed.config import initialize_logging
from distributed.utils import get_mp_context
from distributed.utils_test import new_config_file


def _run_in_process(fn, /, *args):
    pool = None
    try:
        with get_mp_context().Pool(1) as pool:
            return pool.apply(fn, args=args)
    finally:
        if pool is not None:
            pool.join()


class LogCaptureHandler(logging.StreamHandler):
    def __init__(self):
        super().__init__(io.StringIO())
        self.records = []

    def emit(self, record):
        self.records.append(record)
        super().emit(record)

    @property
    def record_tuples(self):
        return [(r.name, r.levelno, r.getMessage()) for r in self.records]


def _check_logging():
    d = logging.getLogger("distributed")
    assert isinstance(d.handlers[0], logging.StreamHandler)

    # we can't pass pytests' caplog to a child process so we make our own one
    caplog = LogCaptureHandler()
    logging.getLogger(None).addHandler(caplog)

    dfb = logging.getLogger("distributed.foo.bar")
    dfc = logging.getLogger("distributed.client")
    f = logging.getLogger("foo")
    fb = logging.getLogger("foo.bar")

    d.debug("1: debug")
    d.info("2: info")
    dfb.info("3: info")
    fb.info("4: info")
    fb.error("5: error")
    f.info("6: info")
    f.error("7: error")
    dfc.info("8: ignore me")
    dfc.warning("9: important")

    return caplog.record_tuples


@pytest.mark.parametrize(
    "config",
    [
        {},
        None,
    ],
)
def test_logging_default(caplog, config):
    """
    Test default logging configuration.
    """
    with contextlib.nullcontext() if config is None else new_config_file(config):
        assert _run_in_process(_check_logging) == [
            ("distributed", logging.INFO, "2: info"),
            ("distributed.foo.bar", logging.INFO, "3: info"),
            # Info logs of foreign libraries are not logged because default is
            # WARNING
            ("foo.bar", logging.ERROR, "5: error"),
            ("foo", logging.ERROR, "7: error"),
            ("distributed.client", logging.WARN, "9: important"),
        ]


def test_logging_simple_under_distributed():
    """
    Test simple ("old-style") logging configuration under the distributed key.
    """
    c = {
        "distributed": {
            "logging": {"distributed.foo": "info", "distributed.foo.bar": "error"}
        }
    }
    # Must test using a subprocess to avoid wrecking pre-existing configuration
    with new_config_file(c):
        code = """if 1:
            import logging
            import dask

            from distributed.utils_test import captured_handler

            d = logging.getLogger('distributed')
            assert len(d.handlers) == 1
            assert isinstance(d.handlers[0], logging.StreamHandler)
            df = logging.getLogger('distributed.foo')
            dfb = logging.getLogger('distributed.foo.bar')

            with captured_handler(d.handlers[0]) as distributed_log:
                df.info("1: info")
                dfb.warning("2: warning")
                dfb.error("3: error")

            distributed_log = distributed_log.getvalue().splitlines()

            assert len(distributed_log) == 2, (dask.config.config, distributed_log)
            """

        subprocess.check_call([sys.executable, "-c", code])


def test_logging_simple():
    """
    Test simple ("old-style") logging configuration.
    """
    c = {"logging": {"distributed.foo": "info", "distributed.foo.bar": "error"}}
    # Must test using a subprocess to avoid wrecking pre-existing configuration
    with new_config_file(c):
        code = """if 1:
            import logging
            import dask

            from distributed.utils_test import captured_handler

            d = logging.getLogger('distributed')
            assert len(d.handlers) == 1
            assert isinstance(d.handlers[0], logging.StreamHandler)
            df = logging.getLogger('distributed.foo')
            dfb = logging.getLogger('distributed.foo.bar')

            with captured_handler(d.handlers[0]) as distributed_log:
                df.info("1: info")
                dfb.warning("2: warning")
                dfb.error("3: error")

            distributed_log = distributed_log.getvalue().splitlines()

            assert len(distributed_log) == 2, (dask.config.config, distributed_log)
            """

        subprocess.check_call([sys.executable, "-c", code])


def test_logging_extended():
    """
    Test extended ("new-style") logging configuration.
    """
    c = {
        "logging": {
            "version": "1",
            "formatters": {
                "simple": {"format": "%(levelname)s: %(name)s: %(message)s"}
            },
            "handlers": {
                "console": {
                    "class": "logging.StreamHandler",
                    "stream": "ext://sys.stderr",
                    "formatter": "simple",
                }
            },
            "loggers": {
                "distributed.foo": {
                    "level": "INFO",
                    # 'handlers': ['console'],
                },
                "distributed.foo.bar": {
                    "level": "ERROR",
                    # 'handlers': ['console'],
                },
            },
            "root": {"level": "WARNING", "handlers": ["console"]},
        }
    }
    # Must test using a subprocess to avoid wrecking pre-existing configuration
    with new_config_file(c):
        code = """if 1:
            import logging

            from distributed.utils_test import captured_handler

            root = logging.getLogger()
            d = logging.getLogger('distributed')
            df = logging.getLogger('distributed.foo')
            dfb = logging.getLogger('distributed.foo.bar')

            with captured_handler(root.handlers[0]) as root_log:
                df.info("1: info")
                dfb.warning("2: warning")
                dfb.error("3: error")
                d.info("4: info")
                d.warning("5: warning")

            root_log = root_log.getvalue().splitlines()
            print(root_log)

            assert root_log == [
                "INFO: distributed.foo: 1: info",
                "ERROR: distributed.foo.bar: 3: error",
                "WARNING: distributed: 5: warning",
                ]
            """

        subprocess.check_call([sys.executable, "-c", code])


def test_logging_mutual_exclusive():
    """
    Ensure that 'logging-file-config' and 'logging' have to be mutual exclusive.
    """
    config = {"logging": {"dask": "warning"}, "logging-file-config": "/path/to/config"}
    with pytest.raises(RuntimeError):
        initialize_logging(config)


def test_logging_file_config():
    """
    Test `logging-file-config` logging configuration
    """
    logging_config_contents = """
[handlers]
keys=console

[formatters]
keys=simple

[loggers]
keys=root, foo, foo_bar

[handler_console]
class=StreamHandler
level=INFO
formatter=simple
args=(sys.stdout,)

[formatter_simple]
format=%(levelname)s: %(name)s: %(message)s

[logger_root]
level=WARNING
handlers=console

[logger_foo]
level=INFO
handlers=console
qualname=foo

[logger_foo_bar]
level=ERROR
handlers=console
qualname=foo.bar
"""
    with tempfile.NamedTemporaryFile(mode="w", delete=False) as logging_config:
        logging_config.write(logging_config_contents)
    dask_config = {"logging-file-config": logging_config.name}
    with new_config_file(dask_config):
        code = """if 1:
            import logging
            from distributed import config
            foo = logging.getLogger('foo')
            bar = logging.getLogger('foo.bar')
            assert logging.INFO == foo.getEffectiveLevel()
            assert logging.ERROR == bar.getEffectiveLevel()
            """
        subprocess.check_call([sys.executable, "-c", code])
    os.remove(logging_config.name)


def test_schema():
    jsonschema = pytest.importorskip("jsonschema")
    config_fn = os.path.join(os.path.dirname(__file__), "..", "distributed.yaml")
    schema_fn = os.path.join(os.path.dirname(__file__), "..", "distributed-schema.yaml")

    with open(config_fn) as f:
        config = yaml.safe_load(f)

    with open(schema_fn) as f:
        schema = yaml.safe_load(f)

    jsonschema.validate(config, schema)


def test_schema_is_complete():
    config_fn = os.path.join(os.path.dirname(__file__), "..", "distributed.yaml")
    schema_fn = os.path.join(os.path.dirname(__file__), "..", "distributed-schema.yaml")

    with open(config_fn) as f:
        config = yaml.safe_load(f)

    with open(schema_fn) as f:
        schema = yaml.safe_load(f)

    skip = {
        "distributed.scheduler.default-task-durations",
        "distributed.scheduler.dashboard.bokeh-application",
        "distributed.nanny.environ",
        "distributed.nanny.pre-spawn-environ",
        "distributed.comm.ucx.environment",
    }

    def test_matches(c, s, root):
        if set(c) != set(s["properties"]):
            raise ValueError(
                "\nThe distributed.yaml and distributed-schema.yaml files are not in sync.\n"
                "This usually happens when we add a new configuration value,\n"
                "but don't add the schema of that value to the distributed-schema.yaml file\n"
                "Please modify these files to include the missing values: \n\n"
                "    distributed.yaml:        {}\n"
                "    distributed-schema.yaml: {}\n\n"
                "Examples in these files should be a good start, \n"
                "even if you are not familiar with the jsonschema spec".format(
                    sorted(c), sorted(s["properties"])
                )
            )
        for k, v in c.items():
            key = f"{root}.{k}" if root else k
            if isinstance(v, dict) and key not in skip:
                test_matches(c[k], s["properties"][k], key)

    test_matches(config, schema, "")


def test_uvloop_event_loop():
    """Check that configuring distributed to use uvloop actually sets the event loop policy"""
    pytest.importorskip("uvloop")
    script = (
        "import distributed, asyncio, uvloop\n"
        "assert isinstance(asyncio.get_event_loop_policy(), uvloop.EventLoopPolicy)"
    )
    subprocess.check_call(
        [sys.executable, "-c", script],
        env={"DASK_DISTRIBUTED__ADMIN__EVENT_LOOP": "uvloop"},
    )

import atexit
import logging
import gc
import os
import re
import sys
import warnings

import click
import dask

from tornado.ioloop import IOLoop

from distributed import Scheduler, Security
from distributed.preloading import validate_preload_argv
from distributed.cli.utils import check_python_3, install_signal_handlers
from distributed.utils import deserialize_for_cli
from distributed.proctitle import (
    enable_proctitle_on_children,
    enable_proctitle_on_current,
)

logger = logging.getLogger("distributed.scheduler")


pem_file_option_type = click.Path(exists=True, resolve_path=True)


scheduler_options = [
    click.Option(
        ["--host"], type=str, default="", help="URI, IP or hostname of this server"
    ),
    click.Option(["--port"], type=int, default=None, help="Serving port"),
    click.Option(
        ["--interface"],
        type=str,
        default=None,
        help="Preferred network interface like 'eth0' or 'ib0'",
    ),
    click.Option(
        ["--protocol"], type=str, default=None, help="Protocol like tcp, tls, or ucx"
    ),
    click.Option(
        ["--tls-ca-file"],
        type=pem_file_option_type,
        default=None,
        help="CA cert(s) file for TLS (in PEM format)",
    ),
    click.Option(
        ["--tls-cert"],
        type=pem_file_option_type,
        default=None,
        help="certificate file for TLS (in PEM format)",
    ),
    click.Option(
        ["--tls-key"],
        type=pem_file_option_type,
        default=None,
        help="private key file for TLS (in PEM format)",
    ),
    # XXX default port (or URI) values should be centralized somewhere
    click.Option(
        ["--bokeh-port"],
        type=int,
        default=None,
        help="Deprecated.  See --dashboard-address",
    ),
    click.Option(
        ["--dashboard-address"],
        type=str,
        default=":8787",
        show_default=True,
        help="Address on which to listen for diagnostics dashboard",
    ),
    click.Option(
        ["--dashboard/--no-dashboard"],
        default=True,
        required=False,
        help="Launch the Dashboard [default: --dashboard]",
    ),
    click.Option(
        ["--bokeh/--no-bokeh"],
        "bokeh",
        default=None,
        required=False,
        help="Deprecated.  See --dashboard/--no-dashboard.",
    ),
    click.Option(
        ["--show/--no-show"], default=False, help="Show web UI [default: --show]"
    ),
    click.Option(
        ["--dashboard-prefix"],
        type=str,
        default="",
        help="Prefix for the dashboard app",
    ),
    click.Option(
        ["--use-xheaders"],
        type=bool,
        default=False,
        show_default=True,
        help="User xheaders in dashboard app for ssl termination in header",
    ),
    click.Option(
        ["--pid-file"], type=str, default="", help="File to write the process PID"
    ),
    click.Option(
        ["--scheduler-file"],
        type=str,
        default="",
        help="File to write connection information. "
        "This may be a good way to share connection information if your "
        "cluster is on a shared network file system.",
    ),
    click.Option(
        ["--preload"],
        type=str,
        multiple=True,
        is_eager=True,
        default="",
        help="Module that should be loaded by the scheduler process  "
        'like "foo.bar" or "/path/to/foo.py".',
    ),
    click.Option(
        ["--idle-timeout"],
        default=None,
        type=str,
        help="Time of inactivity after which to kill the scheduler",
    ),
    click.Argument(
        ["preload_argv"],
        nargs=-1,
        type=click.UNPROCESSED,
        callback=validate_preload_argv,
    ),
]


class SchedulerCommand(click.Command):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.params = scheduler_options
        self.context_settings = dict(ignore_unknown_options=True)


@click.version_option()
@click.command(cls=SchedulerCommand)
def main(
    host,
    port,
    bokeh_port,
    show,
    dashboard,
    bokeh,
    dashboard_prefix,
    use_xheaders,
    pid_file,
    tls_ca_file,
    tls_cert,
    tls_key,
    dashboard_address,
    **kwargs
):
    g0, g1, g2 = gc.get_threshold()  # https://github.com/dask/distributed/issues/1653
    gc.set_threshold(g0 * 3, g1 * 3, g2 * 3)

    enable_proctitle_on_current()
    enable_proctitle_on_children()

    if bokeh_port is not None:
        warnings.warn(
            "The --bokeh-port flag has been renamed to --dashboard-address. "
            "Consider adding ``--dashboard-address :%d`` " % bokeh_port
        )
        dashboard_address = bokeh_port
    if bokeh is not None:
        warnings.warn(
            "The --bokeh/--no-bokeh flag has been renamed to --dashboard/--no-dashboard. "
        )
        dashboard = bokeh

    if port is None and (not host or not re.search(r":\d", host)):
        port = 8786

    sec = Security(
        **{
            k: v
            for k, v in [
                ("tls_ca_file", tls_ca_file),
                ("tls_scheduler_cert", tls_cert),
                ("tls_scheduler_key", tls_key),
            ]
            if v is not None
        }
    )

    if "DASK_INTERNAL_INHERIT_CONFIG" in os.environ:
        config = deserialize_for_cli(os.environ["DASK_INTERNAL_INHERIT_CONFIG"])
        # Update the global config given priority to the existing global config
        dask.config.update(dask.config.global_config, config, priority="old")

    if not host and (tls_ca_file or tls_cert or tls_key):
        host = "tls://"

    if pid_file:
        with open(pid_file, "w") as f:
            f.write(str(os.getpid()))

        def del_pid_file():
            if os.path.exists(pid_file):
                os.remove(pid_file)

        atexit.register(del_pid_file)

    if sys.platform.startswith("linux"):
        import resource  # module fails importing on Windows

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        limit = max(soft, hard // 2)
        resource.setrlimit(resource.RLIMIT_NOFILE, (limit, hard))

    loop = IOLoop.current()
    logger.info("-" * 47)

    scheduler = Scheduler(
        loop=loop,
        security=sec,
        host=host,
        port=port,
        dashboard=dashboard,
        dashboard_address=dashboard_address,
        http_prefix=dashboard_prefix,
        **kwargs
    )
    logger.info("-" * 47)

    install_signal_handlers(loop)

    async def run():
        await scheduler
        await scheduler.finished()

    try:
        loop.run_sync(run)
    finally:
        scheduler.stop()

        logger.info("End scheduler at %r", scheduler.address)


def go():
    check_python_3()
    main()


if __name__ == "__main__":
    go()

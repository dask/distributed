import copy
from tornado.ioloop import IOLoop
from click.types import BoolParamType

py3_err_msg = """
Warning: Your terminal does not set locales.

If you use unicode text inputs for command line options then this may cause
undesired behavior.  This is rare.

If you don't use unicode characters in command line options then you can safely
ignore this message.  This is the common case.

You can support unicode inputs by specifying encoding environment variables,
though exact solutions may depend on your system:

    $ export LC_ALL=C.UTF-8
    $ export LANG=C.UTF-8

For more information see: http://click.pocoo.org/5/python3/
""".lstrip()


def check_python_3():
    """Ensures that the environment is good for unicode on Python 3."""
    # https://github.com/pallets/click/issues/448#issuecomment-246029304
    import click.core

    click.core._verify_python3_env = lambda: None

    try:
        from click import _unicodefun

        _unicodefun._verify_python3_env()
    except (TypeError, RuntimeError) as e:
        import click

        click.echo(py3_err_msg, err=True)


def install_signal_handlers(loop=None, cleanup=None):
    """
    Install global signal handlers to halt the Tornado IOLoop in case of
    a SIGINT or SIGTERM.  *cleanup* is an optional callback called,
    before the loop stops, with a single signal number argument.
    """
    import signal

    loop = loop or IOLoop.current()

    old_handlers = {}

    def handle_signal(sig, frame):
        async def cleanup_and_stop():
            try:
                if cleanup is not None:
                    await cleanup(sig)
            finally:
                loop.stop()

        loop.add_callback_from_signal(cleanup_and_stop)
        # Restore old signal handler to allow for a quicker exit
        # if the user sends the signal again.
        signal.signal(sig, old_handlers[sig])

    for sig in [signal.SIGINT, signal.SIGTERM]:
        old_handlers[sig] = signal.signal(sig, handle_signal)


def prepare_dask_ssh_options(entity, opt):
    opt = copy.deepcopy(opt)
    opt.name = f"{opt.name}__{entity}"
    opt.opts[0] = f'--{entity}-{opt.opts[0].replace("--", "")}'
    if opt.secondary_opts:
        opt.secondary_opts[0] = f'--{entity}-{opt.secondary_opts[0].replace("--", "")}'

    return opt


def prepare_additional_options(entity, cmd, options, **kwargs):
    options_copy = copy.deepcopy(options)
    for opt in options_copy:
        if opt.name in kwargs.keys():
            value = kwargs.get(opt.name)
            if value in [None, (), ""]:
                continue

            opt.name = opt.name.replace(f"__{entity}", "")
            opt.opts[0] = "--{}".format(opt.opts[0].replace(f"--{entity}-", ""))
            if opt.secondary_opts:
                opt.secondary_opts[0] = "--{}".format(
                    opt.secondary_opts[0].replace(f"--{entity}-", "")
                )

            opts = opt.opts[0]
            if isinstance(opt.type, BoolParamType):
                if not value:
                    try:
                        opts = opt.secondary_opts[0]
                    except IndexError:
                        continue
                value = ""
            if opt.multiple:
                for i in value:
                    if " " in i:
                        i = f'"{i}"'
                    cmd += f" {opts} {i}"
                continue
            if " " in value:
                value = f'"{value}"'

            cmd += f" {opts} {value}"

    return cmd

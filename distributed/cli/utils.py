from tornado.ioloop import IOLoop


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


def format_table(rows, headers=None):
    """Formats list of lists into a table.

    If headers is not provided the first row will be used as headers.

    >>> print(format_table([["foo", "bar"], ["fizz", "buzz"]], headers=["hello", "world"]))
    HELLO  WORLD
    foo    bar
    fizz   buzz

    """

    if headers is None:
        headers = rows.pop(0)

    if len(set([len(row) for row in rows] + [len(headers)])) != 1:
        raise RuntimeError("Headers and each row must be lists of equal length")

    col_widths = [
        max([len(str(row[i])) for row in rows] + [len(str(headers[i]))])
        for i in range(len(headers))
    ]

    try:
        rows.insert(0, [h.upper() for h in headers])
    except AttributeError:
        raise RuntimeError("Headers must be strings")

    def justify(value, length):
        if isinstance(value, int) or isinstance(value, float):
            return str(value).rjust(length)
        return str(value).ljust(length)

    return "\n".join(
        [
            "  ".join([justify(row[i], col_widths[i]) for i in range(len(row))])
            for row in rows
        ]
    )

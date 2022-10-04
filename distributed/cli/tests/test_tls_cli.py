from __future__ import annotations

from time import sleep

from distributed import Client
from distributed.metrics import time
from distributed.utils import open_port
from distributed.utils_test import (
    get_cert,
    new_config_file,
    popen,
    tls_only_config,
    tls_security,
)

ca_file = get_cert("tls-ca-cert.pem")
cert = get_cert("tls-cert.pem")
key = get_cert("tls-key.pem")
keycert = get_cert("tls-key-cert.pem")


tls_args = ["--tls-ca-file", ca_file, "--tls-cert", keycert]
tls_args_2 = ["--tls-ca-file", ca_file, "--tls-cert", cert, "--tls-key", key]


def wait_for_cores(c, nthreads=1):
    start = time()
    while len(c.nthreads()) < 1:
        sleep(0.1)
        assert time() < start + 10


def test_basic(loop, requires_default_ports):
    with popen(["dask", "scheduler", "--no-dashboard"] + tls_args) as s:
        with popen(
            ["dask", "worker", "--no-dashboard", "tls://127.0.0.1:8786"] + tls_args
        ) as w:
            with Client(
                "tls://127.0.0.1:8786", loop=loop, security=tls_security()
            ) as c:
                wait_for_cores(c)


def test_sni(loop):
    port = open_port()
    with popen(["dask-scheduler", "--no-dashboard", f"--port={port}"] + tls_args) as s:
        with popen(
            [
                "dask-worker",
                "--no-dashboard",
                "--scheduler-sni",
                "localhost",
                f"tls://127.0.0.1:{port}",
            ]
            + tls_args
        ) as w:
            with Client(
                f"tls://127.0.0.1:{port}", loop=loop, security=tls_security()
            ) as c:
                wait_for_cores(c)


def test_nanny(loop):
    port = open_port()
    with popen(
        [
            "dask",
            "scheduler",
            "--no-dashboard",
            f"--port={port}",
        ]
        + tls_args
    ) as s:
        with popen(
            ["dask", "worker", "--no-dashboard", "--nanny", f"tls://127.0.0.1:{port}"]
            + tls_args
        ) as w:
            with Client(
                f"tls://127.0.0.1:{port}", loop=loop, security=tls_security()
            ) as c:
                wait_for_cores(c)


def test_separate_key_cert(loop):
    port = open_port()
    with popen(
        [
            "dask",
            "scheduler",
            "--no-dashboard",
            f"--port={port}",
        ]
        + tls_args_2
    ) as s:
        with popen(
            ["dask", "worker", "--no-dashboard", f"tls://127.0.0.1:{port}"] + tls_args_2
        ) as w:
            with Client(
                f"tls://127.0.0.1:{port}", loop=loop, security=tls_security()
            ) as c:
                wait_for_cores(c)


def test_use_config_file(loop):
    port = open_port()
    with new_config_file(tls_only_config()):
        with popen(
            [
                "dask",
                "scheduler",
                "--no-dashboard",
                "--host",
                "tls://",
                "--port",
                str(port),
            ]
        ) as s:
            with popen(
                ["dask", "worker", "--no-dashboard", f"tls://127.0.0.1:{port}"]
            ) as w:
                with Client(
                    f"tls://127.0.0.1:{port}", loop=loop, security=tls_security()
                ) as c:
                    wait_for_cores(c)

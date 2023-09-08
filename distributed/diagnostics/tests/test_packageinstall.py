from __future__ import annotations

import asyncio
import logging
from unittest import mock

import pytest

from distributed import Nanny
from distributed.core import Status
from distributed.diagnostics.plugin import (
    CondaInstall,
    PackageInstall,
    PipInstall,
    _PackageInstaller,
)
from distributed.utils_test import captured_logger, gen_cluster


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_pip_install(c, s, a):
    with captured_logger(
        "distributed.diagnostics.plugin", level=logging.INFO
    ) as logger:
        mocked = mock.Mock()
        mocked.configure_mock(
            **{"communicate.return_value": (b"", b""), "wait.return_value": 0}
        )
        with mock.patch(
            "distributed.diagnostics.plugin.subprocess.Popen", return_value=mocked
        ) as Popen:
            await c.register_plugin(
                PipInstall(packages=["requests"], pip_options=["--upgrade"])
            )
            assert Popen.call_count == 1
            args = Popen.call_args[0][0]
            assert "python" in args[0]
            assert args[1:] == ["-m", "pip", "install", "--upgrade", "requests"]
            logs = logger.getvalue()
            assert "pip installing" in logs
            assert "failed" not in logs
            assert "restart" not in logs


@gen_cluster(client=True, nthreads=[("", 1)])
async def test_conda_install(c, s, a):
    with captured_logger(
        "distributed.diagnostics.plugin", level=logging.INFO
    ) as logger:
        run_command_mock = mock.Mock(name="run_command_mock")
        run_command_mock.configure_mock(return_value=(b"", b"", 0))
        module_mock = mock.Mock(name="conda_cli_python_api_mock")
        module_mock.run_command = run_command_mock
        module_mock.Commands.INSTALL = "INSTALL"
        with mock.patch.dict("sys.modules", {"conda.cli.python_api": module_mock}):
            await c.register_plugin(
                CondaInstall(packages=["requests"], conda_options=["--update-deps"])
            )
            assert run_command_mock.call_count == 1
            command = run_command_mock.call_args[0][0]
            assert command == "INSTALL"
            arguments = run_command_mock.call_args[0][1]
            assert arguments == ["--update-deps", "requests"]
            logs = logger.getvalue()
            assert "conda installing" in logs
            assert "failed" not in logs
            assert "restart" not in logs


@gen_cluster(client=True, nthreads=[("", 2), ("", 2)])
async def test_pip_install_fails(c, s, a, b):
    with captured_logger(
        "distributed.diagnostics.plugin", level=logging.ERROR
    ) as logger:
        mocked = mock.Mock()
        mocked.configure_mock(
            **{
                "communicate.return_value": (
                    b"",
                    b"Could not find a version that satisfies the requirement not-a-package",
                ),
                "wait.return_value": 1,
            }
        )
        with mock.patch(
            "distributed.diagnostics.plugin.subprocess.Popen", return_value=mocked
        ) as Popen:
            with pytest.raises(RuntimeError):
                await c.register_plugin(PipInstall(packages=["not-a-package"]))

            assert Popen.call_count == 1
            logs = logger.getvalue()
            assert "install failed" in logs
            assert "not-a-package" in logs


@gen_cluster(client=True, nthreads=[("", 2), ("", 2)])
async def test_conda_install_fails_when_conda_not_found(c, s, a, b):
    with captured_logger(
        "distributed.diagnostics.plugin", level=logging.ERROR
    ) as logger:
        with mock.patch.dict("sys.modules", {"conda": None}):
            with pytest.raises(RuntimeError):
                await c.register_plugin(CondaInstall(packages=["not-a-package"]))
            logs = logger.getvalue()
            assert "install failed" in logs
            assert "conda could not be found" in logs


@gen_cluster(client=True, nthreads=[("", 2), ("", 2)])
async def test_conda_install_fails_when_conda_raises(c, s, a, b):
    with captured_logger(
        "distributed.diagnostics.plugin", level=logging.ERROR
    ) as logger:
        run_command_mock = mock.Mock(name="run_command_mock")
        run_command_mock.configure_mock(side_effect=RuntimeError)
        module_mock = mock.Mock(name="conda_cli_python_api_mock")
        module_mock.run_command = run_command_mock
        module_mock.Commands.INSTALL = "INSTALL"
        with mock.patch.dict("sys.modules", {"conda.cli.python_api": module_mock}):
            with pytest.raises(RuntimeError):
                await c.register_plugin(CondaInstall(packages=["not-a-package"]))
            assert run_command_mock.call_count == 1
            logs = logger.getvalue()
            assert "install failed" in logs


@gen_cluster(client=True, nthreads=[("", 2), ("", 2)])
async def test_conda_install_fails_on_returncode(c, s, a, b):
    with captured_logger(
        "distributed.diagnostics.plugin", level=logging.ERROR
    ) as logger:
        run_command_mock = mock.Mock(name="run_command_mock")
        run_command_mock.configure_mock(return_value=(b"", b"", 1))
        module_mock = mock.Mock(name="conda_cli_python_api_mock")
        module_mock.run_command = run_command_mock
        module_mock.Commands.INSTALL = "INSTALL"
        with mock.patch.dict("sys.modules", {"conda.cli.python_api": module_mock}):
            with pytest.raises(RuntimeError):
                await c.register_plugin(CondaInstall(packages=["not-a-package"]))
            assert run_command_mock.call_count == 1
            logs = logger.getvalue()
            assert "install failed" in logs


class _StubInstaller(_PackageInstaller):
    INSTALLER = "stub"

    def __init__(self, packages: list[str]) -> None:
        super().__init__(packages)

    def install(self) -> None:
        pass


class StubInstall(PackageInstall):
    def __init__(self, packages: list[str], restart_workers: bool = False):
        installer = _StubInstaller(packages)
        super().__init__(installer=installer, restart_workers=restart_workers)


@gen_cluster(client=True, nthreads=[("", 1), ("", 1)])
async def test_package_install_installs_once_with_multiple_workers(c, s, a, b):
    with captured_logger(
        "distributed.diagnostics.plugin", level=logging.INFO
    ) as logger:
        install_mock = mock.Mock(name="install")
        with mock.patch.object(_StubInstaller, "install", install_mock):
            await c.register_plugin(
                StubInstall(
                    packages=["requests"],
                )
            )
            assert install_mock.call_count == 1
            logs = logger.getvalue()
            assert "already been installed" in logs


@pytest.mark.slow
@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_package_install_restarts_on_nanny(c, s, a):
    (addr,) = s.workers
    await c.register_plugin(
        StubInstall(
            packages=["requests"],
            restart_workers=True,
        )
    )
    # Wait until the worker is restarted
    while len(s.workers) != 1 or set(s.workers) == {addr}:
        await asyncio.sleep(0.01)


class _FailingInstaller(_PackageInstaller):
    INSTALLER = "fail"

    def __init__(self, packages: list[str]) -> None:
        super().__init__(packages)

    def install(self) -> None:
        raise RuntimeError()


class FailingInstall(PackageInstall):
    def __init__(self, packages: list[str], restart_workers: bool = False):
        installer = _FailingInstaller(packages=packages)
        super().__init__(installer=installer, restart_workers=restart_workers)


@gen_cluster(client=True, nthreads=[("", 1)], Worker=Nanny)
async def test_package_install_failing_does_not_restart_on_nanny(c, s, a):
    (addr,) = s.workers
    with pytest.raises(RuntimeError):
        await c.register_plugin(
            FailingInstall(
                packages=["requests"],
                restart_workers=True,
            )
        )
    # Nanny does not restart
    assert a.status is Status.running
    assert set(s.workers) == {addr}

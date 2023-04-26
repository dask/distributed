from __future__ import annotations

import importlib.metadata
import pathlib
import subprocess
import sys

import distributed


def test_version() -> None:
    assert distributed.__version__ == importlib.metadata.version("distributed")


def test_git_revision() -> None:
    assert isinstance(distributed.__git_revision__, str)


def test_install(tmp_path: pathlib.Path) -> None:
    """
    Test for the editable mode used by setuptools.

    by default `pip install -e` with PEP 660 (Editable installs for
    pyproject.toml based builds) setuptools will use a pth file to add a
    MetaPathFinder to sys.meta_path, however this results in namespace packages
    on the current directory taking precedence over the installed editable
    package.

    setting [tool.setuptools] package-dir = {"" = "."} however causes setuptools
    to use a pth file to add the project directory to sys.path which is how
    `python setup.py develop` behaves
    """
    cwd = tmp_path / "cwd"
    cwd.mkdir()
    decoy_namespace_package = cwd / "distributed"
    decoy_namespace_package.mkdir()
    assert (
        subprocess.run(
            [
                sys.executable,
                "-c",
                "import distributed; print(distributed.__version__)",
            ],
            cwd=cwd,
            capture_output=True,
            text=True,
            check=True,
        ).stdout
        == f"{importlib.metadata.version('distributed')}\n"
    )

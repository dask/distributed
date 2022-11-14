from __future__ import annotations

from importlib import metadata as importlib_metadata

import distributed


def test_version() -> None:
    assert distributed.__version__ == importlib_metadata.version("distributed")


def test_git_revision() -> None:
    assert isinstance(distributed.__git_revision__, str)

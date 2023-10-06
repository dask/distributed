from __future__ import annotations

import builtins
import io
import sys

import psutil
import pytest

from distributed.compatibility import MACOS
from distributed.system import memory_limit


def test_memory_limit():
    limit = memory_limit()
    assert isinstance(limit, int)
    assert limit <= psutil.virtual_memory().total
    assert limit >= 1


def test_hard_memory_limit_cgroups(monkeypatch):
    builtin_open = builtins.open

    def myopen(path, *args, **kwargs):
        if path == "/sys/fs/cgroup/memory/memory.limit_in_bytes":
            # Absurdly low, unlikely to match real value
            return io.StringIO("20")
        return builtin_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", myopen)
    monkeypatch.setattr(sys, "platform", "linux")

    limit = memory_limit()
    assert limit == 20


def test_soft_memory_limit_cgroups(monkeypatch):
    builtin_open = builtins.open

    def myopen(path, *args, **kwargs):
        if path == "/sys/fs/cgroup/memory/memory.limit_in_bytes":
            # Absurdly low, unlikely to match real value
            return io.StringIO("20")
        if path == "/sys/fs/cgroup/memory/memory.soft_limit_in_bytes":
            # Should take precedence
            return io.StringIO("10")
        return builtin_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", myopen)
    monkeypatch.setattr(sys, "platform", "linux")

    limit = memory_limit()
    assert limit == 10


def test_hard_memory_limit_cgroups2(monkeypatch):
    builtin_open = builtins.open

    def myopen(path, *args, **kwargs):
        if path == "/sys/fs/cgroup/memory.max":
            # Absurdly low, unlikely to match real value
            return io.StringIO("20")
        return builtin_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", myopen)
    monkeypatch.setattr(sys, "platform", "linux")

    limit = memory_limit()
    assert limit == 20


def test_soft_memory_limit_cgroups2(monkeypatch):
    builtin_open = builtins.open

    def myopen(path, *args, **kwargs):
        if path == "/sys/fs/cgroup/memory.max":
            # Absurdly low, unlikely to match real value
            return io.StringIO("20")
        if path == "/sys/fs/cgroup/memory.high":
            # should take precedence
            return io.StringIO("10")
        return builtin_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", myopen)
    monkeypatch.setattr(sys, "platform", "linux")

    limit = memory_limit()
    assert limit == 10


def test_softest_memory_limit_cgroups2(monkeypatch):
    builtin_open = builtins.open

    def myopen(path, *args, **kwargs):
        if path == "/sys/fs/cgroup/memory.max":
            # Absurdly low, unlikely to match real value
            return io.StringIO("20")
        if path == "/sys/fs/cgroup/memory.low":
            # should take precedence
            return io.StringIO("10")
        return builtin_open(path, *args, **kwargs)

    monkeypatch.setattr(builtins, "open", myopen)
    monkeypatch.setattr(sys, "platform", "linux")

    limit = memory_limit()
    assert limit == 10


@pytest.mark.xfail(
    MACOS,
    reason="Mac OS raises 'ValueError: current limit exceeds maximum limit' "
    "when calling setrlimit with as little as 7 GiB (getrlimit returns 2**63)",
)
def test_rlimit():
    resource = pytest.importorskip("resource")

    # decrease memory limit by one byte
    new_limit = memory_limit() - 1
    resource.setrlimit(resource.RLIMIT_RSS, (new_limit, new_limit))
    assert memory_limit() == new_limit

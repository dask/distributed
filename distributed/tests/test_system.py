from __future__ import annotations

import builtins
import io
import sys

import psutil
import pytest

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


# DNM
@pytest.mark.parametrize("f", [
    lambda l: l - 1,
    lambda l: l - 2**20,
    lambda l: l // 2,
])
def test_rlimit(f):
    resource = pytest.importorskip("resource")

    # DNM
    print("rlimit=", resource.getrlimit(resource.RLIMIT_RSS))
    print("memory_limit=", memory_limit())

    # decrease memory limit by one byte
    # DNM
    #new_limit = memory_limit() - 1
    new_limit = f(memory_limit())
    try:
        resource.setrlimit(resource.RLIMIT_RSS, (new_limit, new_limit))
        assert memory_limit() == new_limit
    except OSError:
        pytest.skip("resource could not set the RSS limit")

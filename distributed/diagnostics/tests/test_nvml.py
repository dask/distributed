import pytest
import os
from distributed.diagnostics import nvml

pytest.importorskip("pynvml")


def test_one_time():
    output = nvml.one_time()
    assert "memory-total" in output
    assert "name" in output

    assert len(output["name"]) > 0


def test_1_visible_devices():
    os.environ["CUDA_VISIBLE_DEVICES"] = "0"
    output = nvml.one_time()
    assert len(output["memory-total"]) == 1


def test_2_visible_devices():
    os.environ["CUDA_VISIBLE_DEVICES"] = "0,1"
    output = nvml.one_time()
    assert len(output["memory-total"]) == 2

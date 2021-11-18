from __future__ import annotations

import math
from typing import NewType

ShuffleId = NewType("ShuffleId", str)


def worker_for(output_partition: int, npartitions: int, workers: list[str]) -> str:
    "Get the address of the worker which should hold this output partition number"
    if output_partition < 0:
        raise IndexError(f"Negative output partition: {output_partition}")
    if output_partition >= npartitions:
        raise IndexError(
            f"Output partition {output_partition} does not exist in a shuffle producing {npartitions} partitions"
        )
    i = len(workers) * output_partition // npartitions
    return workers[i]


def partition_range(
    worker: str, npartitions: int, workers: list[str]
) -> tuple[int, int]:
    "Get the output partition numbers (inclusive) that a worker will hold"
    i = workers.index(worker)
    first = math.ceil(npartitions * i / len(workers))
    last = math.ceil(npartitions * (i + 1) / len(workers)) - 1
    return first, last


def npartitions_for(worker: str, npartitions: int, workers: list[str]) -> int:
    "Get the number of output partitions a worker will hold"
    first, last = partition_range(worker, npartitions, workers)
    return last - first + 1

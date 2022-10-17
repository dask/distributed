from __future__ import annotations

from typing import Sequence

from distributed.scheduler import TaskState


def coassignmnet_groups(
    tasks: Sequence[TaskState], start: int = 0
) -> dict[int, set[TaskState]]:
    groups = {}
    group = start
    ix = 0
    min_prio = None
    max_prio = None
    while ix < len(tasks):
        current = tasks[ix]
        if min_prio is None:
            min_prio = ix

        if not current.dependents:
            min_prio = None
            max_prio = None
            ix += 1
            continue
        # There is a way to implement this faster by just continuing to iterate
        # over ix and check if the next is a dependent or not. I chose to go
        # this route because this is what we wrote down initially
        next = min(current.dependents, key=lambda ts: ts.priority)
        next_ix = tasks.index(next)
        if next_ix != ix + 1:
            # Detect a jump
            max_prio = next_ix
            groups[group] = set(tasks[min_prio : max_prio + 1])
            group += 1
            ix = max_prio + 1
            min_prio = None
            max_prio = None
        else:
            ix = next_ix

    return groups

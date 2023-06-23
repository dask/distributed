from __future__ import annotations

from typing import Any


class Reschedule(Exception):
    """Reschedule this task

    Raising this exception will stop the current execution of the task and ask
    the scheduler to reschedule this task, possibly on a different machine.

    This does not guarantee that the task will move onto a different machine.
    The scheduler will proceed through its normal heuristics to determine the
    optimal machine to accept this task.  The machine will likely change if the
    load across the cluster has significantly changed since first scheduling
    the task.
    """


def attach_worker_info(ex: BaseException, key: Any, erred_on: Any) -> BaseException:
    """Adds worker information to the original Exception."""

    def worker_str(self: BaseException) -> str:
        return f"{str(ex)}\nin task: {key}\non worker: {erred_on}"

    extype = type(ex)
    try:
        WorkerError = type(
            extype.__name__,
            (extype,),
            dict(args=ex.args, __str__=worker_str, __module__=extype.__module__),
        )
        wrapped = WorkerError(*ex.args)
        return wrapped
    except:  # noqa
        return ex

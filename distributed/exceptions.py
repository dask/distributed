from __future__ import annotations

from asyncio import TimeoutError


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


class WorkerStartTimeoutError(TimeoutError):
    """Raised when the expected number of workers to not start within the timeout period."""

    def __init__(
        self, available_workers: int, expected_workers: int, timeout: float
    ) -> None:
        super().__init__(available_workers, expected_workers, timeout)

    @property
    def available_workers(self) -> int:
        """Number of workers that are available."""
        return self.args[0]

    @property
    def expected_workers(self) -> int:
        """Number of workers that were expected to be available."""
        return self.args[1]

    @property
    def timeout(self) -> float:
        """Timeout period in seconds."""
        return self.args[2]

    def __str__(self) -> str:
        return "Only %d/%d workers arrived after %s" % (
            self.available_workers,
            self.expected_workers,
            self.timeout,
        )

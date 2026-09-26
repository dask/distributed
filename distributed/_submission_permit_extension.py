"""Explicitly enabled prototype of scheduler-side submission protection.

This extension is not part of DEFAULT_EXTENSIONS and does not change Client APIs.
Its limits must be supplied explicitly while the public contract is evaluated.
"""

from __future__ import annotations

from collections.abc import Callable
from time import monotonic
from typing import TYPE_CHECKING

from distributed._submission_permits import (
    ClosedPermitError,
    SubmissionPermitRegistry,
)
from distributed.core import Status

if TYPE_CHECKING:
    from distributed.scheduler import Scheduler


class SubmissionPermitExtension:
    def __init__(
        self,
        scheduler: Scheduler,
        *,
        max_duration: float,
        max_pending_per_client: int,
        max_pending: int,
        max_outcomes_per_client: int,
        clock: Callable[[], float] = monotonic,
    ) -> None:
        self.scheduler = scheduler
        self.registry = SubmissionPermitRegistry(
            max_duration=max_duration,
            max_pending_per_client=max_pending_per_client,
            max_pending=max_pending,
            max_outcomes_per_client=max_outcomes_per_client,
            clock=clock,
        )
        scheduler.handlers.update(
            {
                "submission_permit_acquire": self.acquire,
                "submission_permit_status": self.status,
                "submission_permit_abort": self.abort,
            }
        )

    def register_client(self, client: str) -> str:
        return self.registry.register(client)

    def unregister_client(self, client: str, epoch: str) -> None:
        self.registry.unregister(client, epoch)

    def capabilities(self, epoch: str) -> dict[str, int | float | str]:
        return {
            "version": 1,
            "epoch": epoch,
            "max_duration": self.registry.max_duration,
        }

    def acquire(
        self, client: str, epoch: str, sequence: int, duration: float
    ) -> dict[str, int | float | str]:
        comm = self.scheduler.client_comms.get(client)
        if self.scheduler.status != Status.running or comm is None or comm.closed():
            raise ClosedPermitError("client connection or scheduler is not running")
        permit = self.registry.acquire(client, epoch, sequence, duration)
        if permit.state == "pending":
            self.scheduler.idle_since = None
        return permit.to_dict()

    def status(
        self, client: str, epoch: str, sequence: int
    ) -> dict[str, int | float | str]:
        return self.registry.status(client, epoch, sequence).to_dict()

    def abort(
        self, client: str, epoch: str, sequence: int
    ) -> dict[str, int | float | str]:
        return self.registry.abort(client, epoch, sequence).to_dict()

    def transfer(self, client: str, epoch: str, sequence: int) -> bool:
        # The caller has entered _active_graph_updates, with no await between
        # acquiring that guard and consuming this permit.
        comm = self.scheduler.client_comms.get(client)
        if self.scheduler.status != Status.running or comm is None or comm.closed():
            raise ClosedPermitError("client connection or scheduler is not running")
        return self.registry.transfer(client, epoch, sequence)

    def has_pending(self) -> bool:
        return self.registry.has_pending()

    def commit_idle_shutdown(self) -> None:
        # check_idle queues close before Scheduler.status changes. Fence grants
        # synchronously at the idle-close decision, not when the coroutine runs.
        self.registry.close()

    def teardown(self) -> None:
        self.registry.close()

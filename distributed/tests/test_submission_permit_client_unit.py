from __future__ import annotations

import asyncio

import pytest

from distributed._submission_permit_client import (
    SubmissionPermitExpiredError,
    SubmissionPermitOperation,
    SubmissionPermitRejectedError,
    SubmissionPermitUnsupportedError,
    _operation,
)


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class RPC:
    async def submission_permit_acquire(self, **kwargs):
        return {
            "sequence": kwargs["sequence"],
            "state": "pending",
            "duration": kwargs["duration"],
        }

    async def submission_permit_abort(self, **kwargs):
        return {"state": "aborted", **kwargs}


class Carrier:
    def __init__(self, client) -> None:
        self.client = client
        self.messages = []
        self.is_closed = False

    def closed(self):
        return self.is_closed

    def send(self, message):
        self.messages.append(message)

        def admit() -> None:
            key = (message["submission_epoch"], message["submission_sequence"])
            self.client._submission_permit_pending[key].set_result(
                {"epoch": key[0], "sequence": key[1], "status": "accepted"}
            )

        asyncio.get_running_loop().call_soon(admit)


class Client:
    asynchronous = True
    generation = 4
    status = "running"
    id = "unit-client"

    def __init__(self) -> None:
        self._submission_permit_capabilities = {
            "version": 1,
            "epoch": "epoch",
            "max_duration": 10,
        }
        self._submission_permit_sequence = 0
        self._submission_permit_acquire_lock = asyncio.Lock()
        self._submission_permit_changed = asyncio.Event()
        self._submission_permit_pending = {}
        self.scheduler = RPC()
        self.scheduler_comm = Carrier(self)


def test_operation_tags_only_its_captured_carrier_message():
    async def run() -> None:
        clock = Clock()
        client = Client()
        operation = SubmissionPermitOperation(
            duration=5,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=clock,
        )
        await operation.acquire(client)
        operation.begin_graph(client)
        operation.capture({"op": "update-graph", "keys": {"x"}})
        await operation.commit(client)

        assert client.scheduler_comm.messages == [
            {
                "op": "update-graph",
                "keys": {"x"},
                "submission_epoch": "epoch",
                "submission_sequence": 1,
            }
        ]
        assert not client._submission_permit_pending

    asyncio.run(run())


def test_expiry_before_commit_does_not_send_graph():
    async def run() -> None:
        clock = Clock()
        client = Client()
        operation = SubmissionPermitOperation(
            duration=2,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=clock,
        )
        await operation.acquire(client)
        operation.begin_graph(client)
        operation.capture({"op": "update-graph"})
        clock.now = 2

        with pytest.raises(SubmissionPermitExpiredError):
            await operation.commit(client)
        assert not client.scheduler_comm.messages

    asyncio.run(run())


def test_mismatched_acquire_reply_is_rejected_and_aborted():
    class BadRPC(RPC):
        async def submission_permit_acquire(self, **kwargs):
            return {"sequence": kwargs["sequence"], "state": "accepted", "duration": 1}

    async def run() -> None:
        clock = Clock()
        client = Client()
        client.scheduler = BadRPC()
        operation = SubmissionPermitOperation(
            duration=1,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=clock,
        )

        with pytest.raises(SubmissionPermitRejectedError):
            await operation.acquire(client)

    asyncio.run(run())


def test_argument_validation_and_unsupported_capability():
    with pytest.raises(ValueError, match="duration"):
        _operation(
            duration=0, timeout=1, max_clock_rate=1, clock_margin=0, clock=Clock()
        )
    with pytest.raises(ValueError, match="max_clock_rate"):
        _operation(
            duration=1, timeout=1, max_clock_rate=0.5, clock_margin=0, clock=Clock()
        )

    operation = SubmissionPermitOperation(
        duration=1,
        timeout=1,
        max_clock_rate=1,
        clock_margin=0,
        clock=Clock(),
    )
    client = Client()
    client._submission_permit_capabilities = None

    async def acquire() -> None:
        with pytest.raises(SubmissionPermitUnsupportedError):
            await operation.acquire(client)

    asyncio.run(acquire())


def test_acquire_snapshot_rejects_a_reconnected_client_before_graph_work():
    async def run() -> None:
        clock = Clock()
        client = Client()
        operation = SubmissionPermitOperation(
            duration=1,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=clock,
        )
        await operation.acquire(client)
        client.generation += 1
        client.scheduler_comm = Carrier(client)
        client._submission_permit_capabilities = {
            "version": 1,
            "epoch": "new-epoch",
            "max_duration": 10,
        }

        with pytest.raises(SubmissionPermitRejectedError, match="connection changed"):
            operation.ensure_origin(client)
        with pytest.raises(RuntimeError, match="exactly one graph"):
            operation.begin_graph(Client())

    asyncio.run(run())


def test_clock_must_not_move_backwards_or_exceed_granted_interval():
    async def run() -> None:
        clock = Clock()
        client = Client()
        operation = SubmissionPermitOperation(
            duration=2,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=clock,
        )
        await operation.acquire(client)
        clock.now = -1
        with pytest.raises(SubmissionPermitRejectedError, match="backwards"):
            operation.ensure_valid()

    asyncio.run(run())

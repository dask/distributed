from __future__ import annotations

import asyncio
from typing import Any, cast

import dask
import pytest

import distributed._submission_permit_client as permit_client
from distributed._submission_permit_client import (
    SubmissionPermitExpiredError,
    SubmissionPermitOperation,
    SubmissionPermitRejectedError,
    SubmissionPermitUnsupportedError,
    _operation,
)
from distributed.client import Client as DaskClient


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class RPC:
    async def submission_permit_acquire(self, **kwargs: Any) -> dict[str, Any]:
        return {
            "sequence": kwargs["sequence"],
            "state": "pending",
            "duration": kwargs["duration"],
        }

    async def submission_permit_abort(self, **kwargs: Any) -> dict[str, Any]:
        return {"state": "aborted", **kwargs}


class Carrier:
    def __init__(self, client: FakeClient) -> None:
        self.client = client
        self.messages: list[dict[str, Any]] = []
        self.is_closed = False

    def closed(self) -> bool:
        return self.is_closed

    def send(self, message: dict[str, Any]) -> None:
        self.messages.append(message)

        def admit() -> None:
            key = (message["submission_epoch"], message["submission_sequence"])
            self.client._submission_permit_pending[key].set_result(
                {"epoch": key[0], "sequence": key[1], "status": "accepted"}
            )

        asyncio.get_running_loop().call_soon(admit)


class FakeClient:
    asynchronous: bool = True
    generation: int = 4
    status: str = "running"
    id: str = "unit-client"

    def __init__(self) -> None:
        self._submission_permit_capabilities: dict[str, Any] | None = {
            "version": 1,
            "epoch": "epoch",
            "max_duration": 10,
        }
        self._submission_permit_sequence = 0
        self._submission_permit_acquire_lock = asyncio.Lock()
        self._submission_permit_changed = asyncio.Event()
        self._submission_permit_pending: dict[
            tuple[str, int], asyncio.Future[dict[str, Any]]
        ] = {}
        self.scheduler: RPC = RPC()
        self.scheduler_comm: Carrier = Carrier(self)


def as_dask_client(client: FakeClient) -> DaskClient:
    """Limit the real Client type boundary to these lightweight unit doubles."""
    return cast(DaskClient, client)


def test_operation_tags_only_its_captured_carrier_message():
    async def run() -> None:
        clock = Clock()
        client = FakeClient()
        dask_client = as_dask_client(client)
        operation = SubmissionPermitOperation(
            duration=5,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=clock,
        )
        await operation.acquire(dask_client)
        operation.begin_graph(dask_client)
        operation.capture({"op": "update-graph", "keys": {"x"}})
        await operation.commit(dask_client)

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
        client = FakeClient()
        dask_client = as_dask_client(client)
        operation = SubmissionPermitOperation(
            duration=2,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=clock,
        )
        await operation.acquire(dask_client)
        operation.begin_graph(dask_client)
        operation.capture({"op": "update-graph"})
        clock.now = 2

        with pytest.raises(SubmissionPermitExpiredError):
            await operation.commit(dask_client)
        assert not client.scheduler_comm.messages

    asyncio.run(run())


def test_mismatched_acquire_reply_is_rejected_and_aborted():
    class BadRPC(RPC):
        async def submission_permit_acquire(self, **kwargs: Any) -> dict[str, Any]:
            return {"sequence": kwargs["sequence"], "state": "accepted", "duration": 1}

    async def run() -> None:
        clock = Clock()
        client = FakeClient()
        dask_client = as_dask_client(client)
        client.scheduler = BadRPC()
        operation = SubmissionPermitOperation(
            duration=1,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=clock,
        )

        with pytest.raises(SubmissionPermitRejectedError):
            await operation.acquire(dask_client)

    asyncio.run(run())


def test_acquire_refuses_an_unsupported_duration_and_reusing_an_operation():
    async def run() -> None:
        client = FakeClient()
        dask_client = as_dask_client(client)
        too_long = SubmissionPermitOperation(
            duration=11,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=Clock(),
        )
        with pytest.raises(SubmissionPermitUnsupportedError, match="duration"):
            await too_long.acquire(dask_client)
        assert client._submission_permit_sequence == 0

        operation = SubmissionPermitOperation(
            duration=1,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=Clock(),
        )
        await operation.acquire(dask_client)
        with pytest.raises(RuntimeError, match="another permit"):
            await operation.acquire(dask_client)
        assert client._submission_permit_sequence == 1

    asyncio.run(run())


def test_commit_does_not_replace_an_existing_admission_waiter():
    async def run() -> None:
        client = FakeClient()
        dask_client = as_dask_client(client)
        operation = SubmissionPermitOperation(
            duration=1,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=Clock(),
        )
        await operation.acquire(dask_client)
        operation.begin_graph(dask_client)
        operation.capture({"op": "update-graph"})
        key = (operation.epoch, operation.sequence)
        assert key == ("epoch", 1)
        existing_waiter = asyncio.get_running_loop().create_future()
        client._submission_permit_pending[key] = existing_waiter

        with pytest.raises(RuntimeError, match="duplicate"):
            await operation.commit(dask_client)
        assert client._submission_permit_pending[key] is existing_waiter
        assert not client.scheduler_comm.messages
        assert not operation.dispatch_started

    asyncio.run(run())


def test_acquire_rejects_changed_capabilities_before_permit_rpc():
    class UnexpectedRPC(RPC):
        async def submission_permit_acquire(self, **kwargs: Any) -> dict[str, Any]:
            pytest.fail("a changed connection acquired a permit")

    async def run() -> None:
        client = FakeClient()
        client.scheduler = UnexpectedRPC()
        await client._submission_permit_acquire_lock.acquire()
        operation = SubmissionPermitOperation(
            duration=1,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=Clock(),
        )
        task = asyncio.create_task(operation.acquire(as_dask_client(client)))
        await asyncio.sleep(0)
        client._submission_permit_capabilities = {
            "version": 1,
            "epoch": "replacement",
            "max_duration": 10,
        }
        client._submission_permit_acquire_lock.release()

        with pytest.raises(SubmissionPermitRejectedError, match="during acquire"):
            await task
        assert client._submission_permit_sequence == 0

        bad_clock_client = FakeClient()
        bad_clock_client.scheduler = UnexpectedRPC()
        bad_clock = Clock()
        bad_clock.now = float("nan")
        invalid_clock_operation = SubmissionPermitOperation(
            duration=1,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=bad_clock,
        )
        with pytest.raises(SubmissionPermitRejectedError, match="non-finite"):
            await invalid_clock_operation.acquire(as_dask_client(bad_clock_client))
        assert bad_clock_client._submission_permit_sequence == 1

    asyncio.run(run())


def test_capture_requires_exactly_one_graph_and_commit_requires_capture():
    async def run() -> None:
        client = FakeClient()
        dask_client = as_dask_client(client)
        uncaptured = SubmissionPermitOperation(
            duration=1,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=Clock(),
        )
        await uncaptured.acquire(dask_client)
        with pytest.raises(SubmissionPermitRejectedError, match="not captured"):
            await uncaptured.commit(dask_client)
        assert not client.scheduler_comm.messages
        assert not client._submission_permit_pending

        operation = SubmissionPermitOperation(
            duration=1,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=Clock(),
        )
        await operation.acquire(dask_client)
        operation.begin_graph(dask_client)
        message = {"op": "update-graph", "keys": {"x"}}
        operation.capture(message)
        with pytest.raises(RuntimeError, match="exactly one graph"):
            operation.capture({"op": "update-graph", "keys": {"y"}})
        assert operation.message is message
        assert not client.scheduler_comm.messages

    asyncio.run(run())


def test_release_owned_continues_after_a_future_release_failure():
    class Future:
        def __init__(self, error: BaseException | None = None) -> None:
            self.error = error
            self.released = False

        def release(self) -> None:
            self.released = True
            if self.error is not None:
                raise self.error

    operation = SubmissionPermitOperation(
        duration=1,
        timeout=1,
        max_clock_rate=1,
        clock_margin=0,
        clock=Clock(),
    )
    broken = Future(RuntimeError("release failed"))
    remaining = Future()
    operation.futures = [cast(Any, broken), cast(Any, remaining)]
    operation.message = {"op": "update-graph"}

    operation.release_owned()

    assert broken.released
    assert remaining.released
    assert not operation.futures
    assert operation.message is None


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
    client = FakeClient()
    client._submission_permit_capabilities = None

    async def acquire() -> None:
        with pytest.raises(SubmissionPermitUnsupportedError):
            await operation.acquire(as_dask_client(client))

    asyncio.run(acquire())


def test_acquire_snapshot_rejects_a_reconnected_client_before_graph_work():
    async def run() -> None:
        clock = Clock()
        client = FakeClient()
        dask_client = as_dask_client(client)
        operation = SubmissionPermitOperation(
            duration=1,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=clock,
        )
        await operation.acquire(dask_client)
        client.generation += 1
        client.scheduler_comm = Carrier(client)
        client._submission_permit_capabilities = {
            "version": 1,
            "epoch": "new-epoch",
            "max_duration": 10,
        }

        with pytest.raises(SubmissionPermitRejectedError, match="connection changed"):
            operation.ensure_origin(dask_client)
        with pytest.raises(RuntimeError, match="exactly one graph"):
            operation.begin_graph(as_dask_client(FakeClient()))

    asyncio.run(run())


def test_clock_must_not_move_backwards_or_exceed_granted_interval():
    async def run() -> None:
        clock = Clock()
        client = FakeClient()
        dask_client = as_dask_client(client)
        operation = SubmissionPermitOperation(
            duration=2,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=clock,
        )
        await operation.acquire(dask_client)
        clock.now = -1
        with pytest.raises(SubmissionPermitRejectedError, match="backwards"):
            operation.ensure_valid()

    asyncio.run(run())


def test_remaining_requires_an_acquired_permit():
    operation = SubmissionPermitOperation(
        duration=1,
        timeout=1,
        max_clock_rate=1,
        clock_margin=0,
        clock=Clock(),
    )

    with pytest.raises(RuntimeError, match="not acquired"):
        operation.remaining()


def test_invalid_admission_identity_is_ignored():
    client = FakeClient()

    DaskClient._handle_submission_permit_admission(
        as_dask_client(client), epoch="epoch", sequence=True, status="accepted"
    )

    assert not client._submission_permit_pending


def test_async_failure_cleanup_preserves_the_original_error():
    class OriginalError(Exception):
        pass

    class CleanupError(Exception):
        pass

    class Operation:
        dispatch_started = False

        async def acquire(self, client: DaskClient) -> None:
            raise OriginalError

        def release_owned(self) -> None:
            raise CleanupError

        async def abort(self, client: DaskClient) -> None:
            raise CleanupError

        async def cleanup(self, client: DaskClient) -> None:
            raise CleanupError

    async def run() -> None:
        with pytest.raises(OriginalError):
            await permit_client._run(
                cast(DaskClient, object()), cast(Any, Operation()), None, False, {}
            )

    asyncio.run(run())


def test_sync_failure_cleanup_preserves_the_original_error(monkeypatch):
    class OriginalError(Exception):
        pass

    class CleanupError(Exception):
        pass

    class Operation:
        dispatch_started = False

        async def acquire(self, client: DaskClient) -> None:
            raise OriginalError

        def release_owned(self) -> None:
            raise CleanupError

        async def abort(self, client: DaskClient) -> None:
            raise CleanupError

        async def cleanup(self, client: DaskClient) -> None:
            raise CleanupError

    class Client:
        asynchronous = False

        def sync(self, func: Any, *args: Any) -> Any:
            return asyncio.run(func(*args))

    monkeypatch.setattr(permit_client, "_operation", lambda **kwargs: Operation())
    with pytest.raises(OriginalError):
        permit_client._protected(
            cast(DaskClient, Client()),
            dask.delayed(lambda: None)(),
            persist=False,
            duration=1,
            timeout=1,
            max_clock_rate=1,
            clock_margin=0,
            clock=Clock(),
            kwargs={},
        )

"""Explicit client-side submission permits for a single Dask collection.

This is deliberately a private, opt-in API.  It holds a graph while it is
prepared, then holds the operation's Futures until scheduler admission.

The caller supplies finite lease/network limits and a clock-rate bound plus a
safety margin. Timing validity assumes continuously advancing client/server
clocks whose relative rate respects that bound; it is not a guarantee for an
arbitrarily suspended or misconfigured clock. No renewal or fallback is used.

Optimization and serialization keep the ordinary Client calling thread. Async
cancellation is cooperative and cannot interrupt that synchronous preparation.
After a dispatch attempt, cancellation or an indeterminate error can leave work
running: the graph is never retransmitted and an abort RPC is never issued.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from contextvars import ContextVar
from math import isfinite
from time import monotonic
from typing import TYPE_CHECKING, Any

import dask

if TYPE_CHECKING:
    from distributed.client import Client, Future


class SubmissionPermitClientError(RuntimeError):
    """Base class for client-side submission permit failures."""


class SubmissionPermitUnsupportedError(SubmissionPermitClientError):
    """The connected scheduler did not advertise a compatible permit protocol."""


class SubmissionPermitExpiredError(SubmissionPermitClientError):
    """The conservative local permit interval ended before admission."""


class SubmissionPermitRejectedError(SubmissionPermitClientError):
    """The scheduler rejected the tagged graph."""


class SubmissionPermitIndeterminateError(SubmissionPermitClientError):
    """The graph may have been sent, but its admission outcome was not observed."""


# Short names make these useful in small caller-side error handlers too.
PermitUnsupportedError = SubmissionPermitUnsupportedError
PermitExpiredError = SubmissionPermitExpiredError
PermitRejectedError = SubmissionPermitRejectedError
PermitIndeterminateError = SubmissionPermitIndeterminateError


_current_submission: ContextVar[SubmissionPermitOperation | None] = ContextVar(
    "current_submission", default=None
)


def _finite(
    name: str, value: object, *, minimum: float, inclusive: bool = False
) -> float:
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not isfinite(value)
    ):
        raise ValueError(f"{name} must be a finite number")
    value = float(value)
    if value < minimum or (not inclusive and value == minimum):
        comparison = "at least" if inclusive else "greater than"
        raise ValueError(f"{name} must be {comparison} {minimum}")
    return value


class SubmissionPermitOperation:
    """The one graph and Futures owned by one protected submission."""

    def __init__(
        self,
        *,
        duration: float,
        timeout: float,
        max_clock_rate: float,
        clock_margin: float,
        clock: Callable[[], float],
    ) -> None:
        self.duration = duration
        self.timeout = timeout
        self.max_clock_rate = max_clock_rate
        self.clock_margin = clock_margin
        self.clock = clock
        self.started: float | None = None
        self._last_clock: float | None = None
        self.client: Client | None = None
        self.generation: int | None = None
        self.carrier: Any = None
        self.capabilities: dict[str, Any] | None = None
        self.message: dict[str, Any] | None = None
        self.futures: list[Future] = []
        self.graph_started = False
        self.sequence: int | None = None
        self.epoch: str | None = None
        self.granted_duration: float | None = None
        self.dispatch_started = False

    def begin_graph(self, client: Client) -> None:
        if self.client is not client or self.graph_started:
            raise RuntimeError("a protected submission may capture exactly one graph")
        self.graph_started = True

    def own(self, future: Future) -> None:
        self.futures.append(future)

    def capture(self, message: dict[str, Any]) -> None:
        if self.message is not None:
            raise RuntimeError("a protected submission may capture exactly one graph")
        self.message = message

    def remaining(self) -> float:
        if self.granted_duration is None or self.started is None:
            raise RuntimeError("submission permit was not acquired")
        return (
            self.granted_duration
            - self.max_clock_rate * (self._clock_now() - self.started)
            - self.clock_margin
        )

    def _clock_now(self) -> float:
        now = self.clock()
        if (
            isinstance(now, bool)
            or not isinstance(now, (int, float))
            or not isfinite(now)
        ):
            raise SubmissionPermitRejectedError("clock returned a non-finite value")
        now = float(now)
        if self._last_clock is not None and now < self._last_clock:
            raise SubmissionPermitRejectedError("clock moved backwards")
        self._last_clock = now
        return now

    def ensure_valid(self) -> None:
        if self.remaining() <= 0:
            raise SubmissionPermitExpiredError("submission permit expired locally")

    def ensure_origin(self, client: Client) -> None:
        capabilities = client._submission_permit_capabilities
        if (
            self.client is not client
            or self.generation != client.generation
            or self.carrier is not client.scheduler_comm
            or self.carrier is None
            or self.carrier.closed()
            or client.status != "running"
            or capabilities is not self.capabilities
            or capabilities is None
            or capabilities.get("epoch") != self.epoch
        ):
            raise SubmissionPermitRejectedError(
                "submission permit connection changed before graph dispatch"
            )

    async def acquire(self, client: Client) -> None:
        if self.sequence is not None:
            raise RuntimeError("a submission operation cannot acquire another permit")
        capabilities = client._submission_permit_capabilities
        if (
            not isinstance(capabilities, dict)
            or capabilities.get("version") != 1
            or not isinstance(capabilities.get("epoch"), str)
        ):
            raise SubmissionPermitUnsupportedError(
                "scheduler does not support submission permits"
            )
        epoch = capabilities["epoch"]
        maximum = capabilities.get("max_duration")
        if (
            isinstance(maximum, bool)
            or not isinstance(maximum, (int, float))
            or not isfinite(maximum)
            or maximum <= 0
            or self.duration > maximum
        ):
            raise SubmissionPermitUnsupportedError(
                "requested permit duration is not supported"
            )

        async with client._submission_permit_acquire_lock:
            # Capabilities can change while another acquisition owns the lock.
            if client._submission_permit_capabilities is not capabilities:
                raise SubmissionPermitRejectedError(
                    "submission permit connection changed during acquire"
                )
            client._submission_permit_sequence += 1
            sequence = client._submission_permit_sequence
            self.client = client
            self.generation = client.generation
            self.carrier = client.scheduler_comm
            self.capabilities = capabilities
            self.epoch = epoch
            self.sequence = sequence
            self.started = self._clock_now()
            self.ensure_origin(client)
            connection_changed = client._submission_permit_changed
            request = asyncio.create_task(
                client.scheduler.submission_permit_acquire(
                    client=client.id,
                    epoch=epoch,
                    sequence=sequence,
                    duration=self.duration,
                )
            )
            changed = asyncio.create_task(connection_changed.wait())
            try:
                done, _ = await asyncio.wait(
                    (request, changed),
                    timeout=self.timeout,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if connection_changed.is_set():
                    raise SubmissionPermitRejectedError(
                        "connection changed before the grant was received"
                    )
                if request not in done:
                    raise asyncio.TimeoutError(
                        "timed out waiting for a submission permit"
                    )
                reply = request.result()
            finally:
                request.cancel()
                changed.cancel()
                await asyncio.gather(request, changed, return_exceptions=True)

        if (
            not isinstance(reply, dict)
            or reply.get("sequence") != sequence
            or reply.get("state") != "pending"
            or isinstance(reply.get("duration"), bool)
            or not isinstance(reply.get("duration"), (int, float))
            or not isfinite(reply["duration"])
            or reply["duration"] <= 0
            or reply["duration"] > self.duration
        ):
            await self.abort(client)
            raise SubmissionPermitRejectedError(
                "scheduler did not grant a pending submission permit"
            )
        self.granted_duration = float(reply["duration"])
        try:
            self.ensure_origin(client)
            self.ensure_valid()
        except BaseException:
            await self.abort(client)
            raise

    async def abort(self, client: Client) -> None:
        if self.dispatch_started or self.epoch is None or self.sequence is None:
            return
        try:
            await asyncio.wait_for(
                client.scheduler.submission_permit_abort(
                    client=client.id, epoch=self.epoch, sequence=self.sequence
                ),
                timeout=self.timeout,
            )
        except BaseException:
            # An acquire response can have been lost; cleanup is best effort only.
            pass

    async def commit(self, client: Client) -> None:
        if self.message is None or self.epoch is None or self.sequence is None:
            raise SubmissionPermitRejectedError("protected graph was not captured")
        self.ensure_origin(client)
        self.ensure_valid()
        message = dict(
            self.message, submission_epoch=self.epoch, submission_sequence=self.sequence
        )
        key = (self.epoch, self.sequence)
        waiter = asyncio.get_running_loop().create_future()
        if key in client._submission_permit_pending:
            raise RuntimeError("duplicate submission permit admission waiter")
        client._submission_permit_pending[key] = waiter
        try:
            # send may enqueue before raising or being interrupted. From this
            # point onward, never race an abort RPC against a possibly queued graph.
            self.dispatch_started = True
            self.carrier.send(message)
            try:
                reply = await asyncio.wait_for(
                    asyncio.shield(waiter), timeout=self.timeout
                )
            except asyncio.TimeoutError as exc:
                raise SubmissionPermitIndeterminateError(
                    "timed out waiting for graph admission"
                ) from exc
        except asyncio.CancelledError:
            raise
        except SubmissionPermitClientError:
            raise
        except Exception as exc:
            raise SubmissionPermitIndeterminateError(
                "graph dispatch or its admission response was interrupted"
            ) from exc
        finally:
            if client._submission_permit_pending.get(key) is waiter:
                del client._submission_permit_pending[key]
            if not waiter.done():
                waiter.cancel()
            elif not waiter.cancelled():
                waiter.exception()
        if (
            reply.get("status") != "accepted"
            or reply.get("epoch") != self.epoch
            or reply.get("sequence") != self.sequence
        ):
            raise SubmissionPermitRejectedError(
                reply.get("reason") or "scheduler rejected protected graph"
            )
        if (
            self.client is not client
            or self.generation != client.generation
            or self.carrier is not client.scheduler_comm
            or self.carrier is None
            or self.carrier.closed()
            or client.status != "running"
            or client._submission_permit_capabilities is not self.capabilities
            or client._submission_permit_capabilities is None
            or client._submission_permit_capabilities.get("epoch") != self.epoch
        ):
            raise SubmissionPermitIndeterminateError(
                "connection changed after graph admission"
            )

    def release_owned(self) -> None:
        for future in self.futures:
            try:
                future.release()
            except BaseException:
                pass
        self.futures.clear()
        self.message = None

    def publish(self) -> None:
        self.futures.clear()
        self.message = None

    async def cleanup(self, client: Client) -> None:
        # Future.release queues refcount work onto this same event loop.
        await asyncio.sleep(0)


def _operation(
    *,
    duration: object,
    timeout: object,
    max_clock_rate: object,
    clock_margin: object,
    clock: Callable[[], float],
) -> SubmissionPermitOperation:
    if not callable(clock):
        raise TypeError("clock must be callable")
    return SubmissionPermitOperation(
        duration=_finite("duration", duration, minimum=0),
        timeout=_finite("timeout", timeout, minimum=0),
        max_clock_rate=_finite(
            "max_clock_rate", max_clock_rate, minimum=1, inclusive=True
        ),
        clock_margin=_finite("clock_margin", clock_margin, minimum=0, inclusive=True),
        clock=clock,
    )


def _prepare(
    client: Client,
    operation: SubmissionPermitOperation,
    collection: Any,
    persist: bool,
    kwargs: dict[str, Any],
) -> Any:
    token = _current_submission.set(operation)
    try:
        result = (
            client.persist(collection, **kwargs)
            if persist
            else client.compute(collection, sync=False, **kwargs)
        )
    finally:
        _current_submission.reset(token)
    if operation.message is None:
        raise RuntimeError("protected submission did not capture a graph")
    return result


async def _run(
    client: Client,
    operation: SubmissionPermitOperation,
    collection: Any,
    persist: bool,
    kwargs: dict[str, Any],
) -> Any:
    try:
        await operation.acquire(client)
        operation.ensure_origin(client)
        operation.ensure_valid()
        result = _prepare(client, operation, collection, persist, kwargs)
        # Preparation is synchronous; allow a queued cancellation to win before
        # the carrier is allowed to dispatch the graph.
        await asyncio.sleep(0)
        operation.ensure_valid()
        await operation.commit(client)
        operation.publish()
        return result
    except BaseException:
        try:
            operation.release_owned()
        except BaseException:
            pass
        if not operation.dispatch_started:
            try:
                await operation.abort(client)
            except BaseException:
                pass
        try:
            await operation.cleanup(client)
        except BaseException:
            pass
        raise


def _protected(
    client: Client,
    collection: Any,
    *,
    persist: bool,
    duration: object,
    timeout: object,
    max_clock_rate: object,
    clock_margin: object,
    clock: Callable[[], float],
    kwargs: dict[str, Any],
) -> Any:
    if not dask.is_dask_collection(collection):
        raise TypeError("protected submission requires one Dask collection")
    if _current_submission.get() is not None:
        raise RuntimeError("nested protected submissions are not supported")
    operation = _operation(
        duration=duration,
        timeout=timeout,
        max_clock_rate=max_clock_rate,
        clock_margin=clock_margin,
        clock=clock,
    )
    if client.asynchronous:
        return _run(client, operation, collection, persist, kwargs)
    # Keep graph optimization and serialization in the calling thread.  Only
    # scheduler RPCs and the direct carrier dispatch run on Client's loop.
    try:
        client.sync(operation.acquire, client)
        operation.ensure_origin(client)
        operation.ensure_valid()
        result = _prepare(client, operation, collection, persist, kwargs)
        operation.ensure_valid()
        client.sync(operation.commit, client)
        operation.publish()
        return result
    except BaseException:
        try:
            operation.release_owned()
        except BaseException:
            pass
        if not operation.dispatch_started:
            try:
                client.sync(operation.abort, client)
            except BaseException:
                pass
        try:
            client.sync(operation.cleanup, client)
        except BaseException:
            pass
        raise


def protected_compute(
    client: Client,
    collection: Any,
    *,
    duration: float,
    timeout: float,
    max_clock_rate: float,
    clock_margin: float,
    clock: Callable[[], float] = monotonic,
    **kwargs: Any,
) -> Any:
    """Return one collection's Future after acknowledged protected submission.

    For an asynchronous Client, await this function to obtain the Future, then
    await that Future for the computation result. For a synchronous Client,
    this function blocks through admission and returns the ordinary Future.
    ``sync=True`` and nested protected submissions are unsupported.

    ``timeout`` bounds each acquisition/admission/cleanup network phase; it
    does not interrupt graph preparation. ``duration`` is the requested server
    lease in seconds. The local usable interval subtracts ``clock_margin`` and
    ``max_clock_rate`` times elapsed client-clock time from the granted lease.
    All limits are explicit experimental choices, without production defaults.
    """
    return _protected(
        client,
        collection,
        persist=False,
        duration=duration,
        timeout=timeout,
        max_clock_rate=max_clock_rate,
        clock_margin=clock_margin,
        clock=clock,
        kwargs=kwargs,
    )


def protected_persist(
    client: Client,
    collection: Any,
    *,
    duration: float,
    timeout: float,
    max_clock_rate: float,
    clock_margin: float,
    clock: Callable[[], float] = monotonic,
    **kwargs: Any,
) -> Any:
    """Return one persisted collection after acknowledged protected submission.

    The mode, timing and failure semantics match :func:`protected_compute`.
    Other Client calls remain unprotected before graph dispatch.
    """
    return _protected(
        client,
        collection,
        persist=True,
        duration=duration,
        timeout=timeout,
        max_clock_rate=max_clock_rate,
        clock_margin=clock_margin,
        clock=clock,
        kwargs=kwargs,
    )

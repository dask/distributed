from __future__ import annotations

import asyncio
import threading
from functools import partial
from time import sleep

import pytest

from dask import delayed

import distributed.client as client_module
from distributed import Client
from distributed._submission_permit_client import (
    PermitExpiredError,
    PermitIndeterminateError,
    PermitRejectedError,
    PermitUnsupportedError,
    _current_submission,
    protected_compute,
    protected_persist,
)
from distributed._submission_permit_extension import SubmissionPermitExtension
from distributed.core import CommClosedError
from distributed.scheduler import DEFAULT_EXTENSIONS
from distributed.utils_test import async_poll_for, cluster, gen_cluster, inc


class Clock:
    now = 0.0

    def __call__(self):
        return self.now


class ClockedExtension(SubmissionPermitExtension):
    def __init__(self, scheduler):
        self.clock = Clock()
        super().__init__(
            scheduler,
            max_duration=10,
            max_pending_per_client=5,
            max_pending=10,
            max_outcomes_per_client=10,
            clock=self.clock,
        )


SCHEDULER_KWARGS = {
    "extensions": {**DEFAULT_EXTENSIONS, "submission-permits": ClockedExtension}
}
OPTIONS = {"duration": 5, "timeout": 1, "max_clock_rate": 1, "clock_margin": 0.1}


def watch_graph_sends(c, monkeypatch):
    messages = []
    send = c.scheduler_comm.send

    def capture(*msgs):
        messages.extend(msg.copy() for msg in msgs if msg.get("op") == "update-graph")
        send(*msgs)

    monkeypatch.setattr(c.scheduler_comm, "send", capture)
    return messages


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 1)], scheduler_kwargs=SCHEDULER_KWARGS
)
async def test_async_protected_compute_and_persist(c, s, a, monkeypatch):
    messages = watch_graph_sends(c, monkeypatch)
    value = delayed(inc)(1)
    future = await protected_compute(c, value, **OPTIONS)
    assert await future == 2
    persisted = await protected_persist(c, delayed(inc)(4), **OPTIONS)
    assert await c.compute(persisted) == 5
    assert [m.get("submission_sequence") for m in messages] == [1, 2, None]
    assert all(
        m["submission_epoch"] == c._submission_permit_capabilities["epoch"]
        for m in messages[:2]
    )
    assert not c._submission_permit_pending
    assert _current_submission.get() is None
    assert not s.extensions["submission-permits"].has_pending()
    s.validate_state()


@gen_cluster(client=True, nthreads=[])
async def test_unsupported_peer_never_prepares(c, s, monkeypatch):
    def unexpected(*args, **kwargs):
        pytest.fail("unsupported peer started graph preparation")

    monkeypatch.setattr(c, "compute", unexpected)
    with pytest.raises(PermitUnsupportedError):
        await protected_compute(c, delayed(inc)(1), **OPTIONS)
    assert not c.futures
    assert not c._submission_permit_pending


@gen_cluster(client=True, nthreads=[], scheduler_kwargs=SCHEDULER_KWARGS)
async def test_late_acquire_ack_never_prepares(c, s, monkeypatch):
    clock = Clock()
    acquire = s.handlers["submission_permit_acquire"]

    def late_acquire(client, epoch, sequence, duration):
        result = acquire(client, epoch, sequence, duration)
        clock.now = 6
        return result

    def unexpected(*args, **kwargs):
        pytest.fail("unusable grant started graph preparation")

    monkeypatch.setitem(s.handlers, "submission_permit_acquire", late_acquire)
    monkeypatch.setattr(c, "compute", unexpected)
    with pytest.raises(PermitExpiredError):
        await protected_compute(c, delayed(inc)(1), clock=clock, **OPTIONS)
    assert not c.futures
    assert not s.extensions["submission-permits"].has_pending()


@gen_cluster(client=True, nthreads=[], scheduler_kwargs=SCHEDULER_KWARGS)
async def test_serialization_error_releases_owned_futures(c, s, monkeypatch):
    class SerializationFailure(RuntimeError):
        pass

    def fail(*args, **kwargs):
        assert c.refcount  # Failure happens after private Future construction.
        raise SerializationFailure("original serialization error")

    messages = watch_graph_sends(c, monkeypatch)
    monkeypatch.setattr(client_module, "serialize", fail)
    with pytest.raises(SerializationFailure, match="original serialization error"):
        await protected_compute(c, delayed(inc)(1), **OPTIONS)
    assert not messages
    assert not c.refcount
    assert not c.futures
    assert not c._submission_permit_pending
    assert not s.extensions["submission-permits"].has_pending()
    assert _current_submission.get() is None


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 1)], scheduler_kwargs=SCHEDULER_KWARGS
)
async def test_server_rejection_preserves_existing_same_key_future(
    c, s, a, monkeypatch
):
    value = delayed(inc)(1)
    existing = c.compute(value)
    assert await existing == 2
    refs_before = dict(c.refcount)
    state = existing._state
    task_state = s.tasks[existing.key]
    compute = c.compute
    ext = s.extensions["submission-permits"]

    def expire_after_preparation(*args, **kwargs):
        result = compute(*args, **kwargs)
        assert c.refcount[existing.key] == refs_before[existing.key] + 1
        ext.clock.now = 5
        return result

    monkeypatch.setattr(c, "compute", expire_after_preparation)
    with pytest.raises(PermitRejectedError):
        await protected_compute(c, value, **OPTIONS)
    assert dict(c.refcount) == refs_before
    assert existing._state is state
    assert s.tasks[existing.key] is task_state
    assert task_state.state == "memory"
    assert await existing == 2
    assert not c._submission_permit_pending


@gen_cluster(client=True, nthreads=[], scheduler_kwargs=SCHEDULER_KWARGS)
async def test_local_expiry_during_preparation_never_sends(c, s, monkeypatch):
    clock = Clock()
    compute = c.compute
    messages = watch_graph_sends(c, monkeypatch)

    def expire(*args, **kwargs):
        result = compute(*args, **kwargs)
        clock.now = 6
        return result

    monkeypatch.setattr(c, "compute", expire)
    with pytest.raises(PermitExpiredError):
        await protected_compute(c, delayed(inc)(1), clock=clock, **OPTIONS)
    assert not messages
    assert not c.refcount
    assert not c.futures
    assert not s.tasks
    assert not s.extensions["submission-permits"].has_pending()


@pytest.mark.parametrize("cancel", [False, True])
@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 1)], scheduler_kwargs=SCHEDULER_KWARGS
)
async def test_lost_admission_or_cancel_after_send_never_retries_or_aborts(
    c, s, a, cancel, monkeypatch
):
    messages = watch_graph_sends(c, monkeypatch)
    admissions = []
    arrived = asyncio.Event()
    client_send = s.client_send
    abort = s.handlers["submission_permit_abort"]
    aborts = []

    def hold_admission(client, msg):
        if msg["op"] == "submission-permit-admission":
            admissions.append(msg.copy())
            arrived.set()
        else:
            client_send(client, msg)

    def watch_abort(*args, **kwargs):
        aborts.append((args, kwargs))
        return abort(*args, **kwargs)

    monkeypatch.setattr(s, "client_send", hold_admission)
    monkeypatch.setitem(s.handlers, "submission_permit_abort", watch_abort)
    options = dict(OPTIONS, timeout=0.1)
    task = asyncio.create_task(protected_compute(c, delayed(inc)(1), **options))
    await arrived.wait()
    if cancel:
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
    else:
        with pytest.raises(PermitIndeterminateError):
            await task
    assert len(messages) == 1
    assert len(admissions) == 1
    assert not aborts
    assert not c.refcount
    assert not c.futures
    assert not c._submission_permit_pending
    # Delayed and duplicate results cannot recreate a removed operation.
    response = {k: v for k, v in admissions[0].items() if k != "op"}
    c._handle_submission_permit_admission(**response)
    c._handle_submission_permit_admission(**response)
    assert not c._submission_permit_pending


@gen_cluster(client=True, nthreads=[], scheduler_kwargs=SCHEDULER_KWARGS)
async def test_cancel_queued_during_preparation_prevents_send(c, s, monkeypatch):
    compute = c.compute
    messages = watch_graph_sends(c, monkeypatch)

    def cancel_after_preparation(*args, **kwargs):
        result = compute(*args, **kwargs)
        asyncio.current_task().cancel()
        return result

    monkeypatch.setattr(c, "compute", cancel_after_preparation)
    task = asyncio.create_task(protected_compute(c, delayed(inc)(1), **OPTIONS))
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not messages
    assert not c.refcount
    assert not c.futures
    assert not s.extensions["submission-permits"].has_pending()


@gen_cluster(client=True, nthreads=[], scheduler_kwargs=SCHEDULER_KWARGS)
async def test_reconnect_during_preparation_cannot_buffer_old_graph(c, s, monkeypatch):
    epoch = c._submission_permit_capabilities["epoch"]
    compute = c.compute
    messages = watch_graph_sends(c, monkeypatch)

    def lose_connection(*args, **kwargs):
        result = compute(*args, **kwargs)
        c.scheduler_comm.comm.abort()
        return result

    monkeypatch.setattr(c, "compute", lose_connection)
    with pytest.raises(PermitRejectedError):
        await protected_compute(c, delayed(inc)(1), **OPTIONS)
    await async_poll_for(
        lambda: (
            c.status == "running"
            and c._submission_permit_capabilities is not None
            and c._submission_permit_capabilities["epoch"] != epoch
        )
    )
    assert not messages
    assert not c.refcount
    assert not c.futures
    assert not c._submission_permit_pending
    assert not [msg for msg in c._pending_msg_buffer if msg["op"] == "update-graph"]
    assert not s.tasks


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 1)], scheduler_kwargs=SCHEDULER_KWARGS
)
async def test_overlapping_operations_are_independent(c, s, a, monkeypatch):
    messages = watch_graph_sends(c, monkeypatch)
    first, second = await asyncio.gather(
        protected_compute(c, delayed(inc)(1), **OPTIONS),
        protected_compute(c, delayed(inc)(8), **OPTIONS),
    )
    assert await first == 2
    assert await second == 9
    assert {msg["submission_sequence"] for msg in messages} == {1, 2}
    assert not c._submission_permit_pending
    assert not s.extensions["submission-permits"].has_pending()


def test_sync_client_preserves_preparation_thread_and_survives_idle_timeout(
    monkeypatch,
):
    extensions = {
        **DEFAULT_EXTENSIONS,
        "submission-permits": partial(
            SubmissionPermitExtension,
            max_duration=10,
            max_pending_per_client=5,
            max_pending=10,
            max_outcomes_per_client=10,
        ),
    }
    with cluster(nworkers=1, scheduler_kwargs={"extensions": extensions}) as (
        s,
        workers,
    ):
        with Client(s["address"]) as c:
            original = c.compute
            caller_thread = threading.get_ident()

            def slow_preparation(*args, **kwargs):
                assert threading.get_ident() == caller_thread
                sleep(0.4)
                return original(*args, **kwargs)

            monkeypatch.setattr(c, "compute", slow_preparation)

            def set_idle_timeout(dask_scheduler):
                dask_scheduler.idle_timeout = 0.05
                # Establish idle now, so normal periodic detection would close
                # the cluster during the following 400ms preparation delay.
                dask_scheduler.check_idle()
                dask_scheduler.check_idle()
                assert dask_scheduler.idle_since is not None

            c.run_on_scheduler(set_idle_timeout)
            future = protected_compute(c, delayed(inc)(1), **OPTIONS)
            assert future.result() == 2
            assert not c._submission_permit_pending
            assert _current_submission.get() is None


@gen_cluster(client=True, nthreads=[], scheduler_kwargs=SCHEDULER_KWARGS)
async def test_cancel_while_grant_response_is_pending(c, s, monkeypatch):
    entered = asyncio.Event()
    release = asyncio.Event()
    acquire = s.handlers["submission_permit_acquire"]

    async def hold_grant(client, epoch, sequence, duration):
        grant = acquire(client, epoch, sequence, duration)
        entered.set()
        await release.wait()
        return grant

    monkeypatch.setitem(s.handlers, "submission_permit_acquire", hold_grant)
    task = asyncio.create_task(protected_compute(c, delayed(inc)(1), **OPTIONS))
    try:
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert not s.extensions["submission-permits"].has_pending()
        assert not c.refcount
        assert not c.futures
        assert not c._submission_permit_pending
        assert not s.tasks
    finally:
        release.set()


@pytest.mark.parametrize("failure", ["reconnect", "restart", "close", "timeout"])
@gen_cluster(client=True, nthreads=[], scheduler_kwargs=SCHEDULER_KWARGS)
async def test_pending_grant_is_bounded_and_wakes_on_connection_change(
    c, s, failure, monkeypatch
):
    entered = asyncio.Event()
    release = asyncio.Event()
    acquire = s.handlers["submission_permit_acquire"]
    old_epoch = c._submission_permit_capabilities["epoch"]

    async def hold_grant(client, epoch, sequence, duration):
        grant = acquire(client, epoch, sequence, duration)
        entered.set()
        await release.wait()
        return grant

    def unexpected(*args, **kwargs):
        pytest.fail("an unconfirmed grant started graph preparation")

    monkeypatch.setitem(s.handlers, "submission_permit_acquire", hold_grant)
    monkeypatch.setattr(c, "compute", unexpected)
    options = dict(OPTIONS, timeout=0.05 if failure == "timeout" else 5)
    task = asyncio.create_task(protected_compute(c, delayed(inc)(1), **options))
    try:
        await entered.wait()
        if failure == "reconnect":
            c.scheduler_comm.comm.abort()
        elif failure == "restart":
            c._handle_restart()
        elif failure == "close":
            await c.close()
        if failure == "timeout":
            with pytest.raises(asyncio.TimeoutError, match="submission permit"):
                await asyncio.wait_for(task, 1)
        else:
            with pytest.raises(PermitRejectedError):
                await asyncio.wait_for(task, 1)
        assert not c.refcount
        assert not c.futures
        assert not c._submission_permit_pending
        if failure == "reconnect":
            await async_poll_for(
                lambda: (
                    c.status == "running"
                    and c._submission_permit_capabilities is not None
                    and c._submission_permit_capabilities["epoch"] != old_epoch
                )
            )
        assert not s.extensions["submission-permits"].has_pending()
    finally:
        release.set()


@gen_cluster(client=True, nthreads=[], scheduler_kwargs=SCHEDULER_KWARGS)
async def test_restart_before_grant_reply_prevents_preparation(c, s, monkeypatch):
    acquire = s.handlers["submission_permit_acquire"]

    def restart_during_grant(client, epoch, sequence, duration):
        grant = acquire(client, epoch, sequence, duration)
        c._handle_restart()
        return grant

    def unexpected(*args, **kwargs):
        pytest.fail("a pre-restart grant started preparation")

    monkeypatch.setitem(s.handlers, "submission_permit_acquire", restart_during_grant)
    monkeypatch.setattr(c, "compute", unexpected)
    with pytest.raises(PermitRejectedError):
        await protected_compute(c, delayed(inc)(1), **OPTIONS)
    assert not c.refcount
    assert not s.tasks
    assert not s.extensions["submission-permits"].has_pending()


@pytest.mark.parametrize("failure", ["reconnect", "restart", "close"])
@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 1)], scheduler_kwargs=SCHEDULER_KWARGS
)
async def test_connection_lifecycle_wakes_admission_waiter(
    c, s, a, failure, monkeypatch
):
    arrived = asyncio.Event()
    original_send = s.client_send
    old_epoch = c._submission_permit_capabilities["epoch"]
    messages = watch_graph_sends(c, monkeypatch)
    aborts = []
    abort = s.handlers["submission_permit_abort"]

    def hold(client, msg):
        if msg["op"] == "submission-permit-admission":
            arrived.set()
        else:
            original_send(client, msg)

    def watch_abort(*args, **kwargs):
        aborts.append((args, kwargs))
        return abort(*args, **kwargs)

    monkeypatch.setattr(s, "client_send", hold)
    monkeypatch.setitem(s.handlers, "submission_permit_abort", watch_abort)
    task = asyncio.create_task(protected_compute(c, delayed(inc)(1), **OPTIONS))
    await arrived.wait()
    if failure == "reconnect":
        c.scheduler_comm.comm.abort()
    elif failure == "restart":
        c._handle_restart()
    else:
        await c.close()
    with pytest.raises(PermitIndeterminateError):
        await task
    if failure == "reconnect":
        await async_poll_for(
            lambda: (
                c.status == "running"
                and c._submission_permit_capabilities is not None
                and c._submission_permit_capabilities["epoch"] != old_epoch
            )
        )
    assert len(messages) == 1
    assert not aborts
    assert not c.refcount
    assert not c.futures
    assert not c._submission_permit_pending


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 1)], scheduler_kwargs=SCHEDULER_KWARGS
)
async def test_epoch_change_after_ack_does_not_publish_stale_futures(
    c, s, a, monkeypatch
):
    capabilities = c._submission_permit_capabilities
    handler = c._stream_handlers["submission-permit-admission"]

    def change_epoch(**msg):
        handler(**msg)
        c._submission_permit_capabilities = dict(capabilities, epoch="replacement")

    monkeypatch.setitem(c._stream_handlers, "submission-permit-admission", change_epoch)
    try:
        with pytest.raises(PermitIndeterminateError):
            await protected_compute(c, delayed(inc)(1), **OPTIONS)
        assert not c.refcount
        assert not c.futures
        assert not c._submission_permit_pending
    finally:
        c._submission_permit_capabilities = capabilities


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 1)], scheduler_kwargs=SCHEDULER_KWARGS
)
async def test_accepted_reply_may_arrive_after_original_permit_duration(
    c, s, a, monkeypatch
):
    original_send = s.client_send

    def delay_ack(client, msg):
        if msg["op"] == "submission-permit-admission":
            assert msg["status"] == "accepted"
            s.loop.call_later(0.3, original_send, client, msg)
        else:
            original_send(client, msg)

    monkeypatch.setattr(s, "client_send", delay_ack)
    future = await protected_compute(
        c, delayed(inc)(1), **dict(OPTIONS, duration=0.2, clock_margin=0.01)
    )
    assert await future == 2
    assert not c._submission_permit_pending


@pytest.mark.parametrize("captures", [0, 2])
@gen_cluster(client=True, nthreads=[], scheduler_kwargs=SCHEDULER_KWARGS)
async def test_zero_or_multiple_graphs_fail_without_send(c, s, captures, monkeypatch):
    original = c.compute
    messages = watch_graph_sends(c, monkeypatch)

    def invalid_preparation(collection, **kwargs):
        if captures == 0:
            return None
        original(collection, **kwargs)
        return original(collection, **kwargs)

    monkeypatch.setattr(c, "compute", invalid_preparation)
    with pytest.raises(RuntimeError, match="graph"):
        await protected_compute(c, delayed(inc)(1), **OPTIONS)
    assert not messages
    assert not c.refcount
    assert not c.futures
    assert not s.tasks
    assert not s.extensions["submission-permits"].has_pending()
    assert _current_submission.get() is None


@gen_cluster(client=True, nthreads=[], scheduler_kwargs=SCHEDULER_KWARGS)
async def test_enqueue_then_send_failure_is_indeterminate_and_never_aborts(
    c, s, monkeypatch
):
    send = c.scheduler_comm.send
    messages = []
    aborts = []
    abort = s.handlers["submission_permit_abort"]

    def interrupted_send(*msgs):
        send(*msgs)
        for msg in msgs:
            if msg["op"] == "update-graph":
                messages.append(msg)
                raise CommClosedError("interrupted after enqueue")

    def watch_abort(*args, **kwargs):
        aborts.append((args, kwargs))
        return abort(*args, **kwargs)

    monkeypatch.setattr(c.scheduler_comm, "send", interrupted_send)
    monkeypatch.setitem(s.handlers, "submission_permit_abort", watch_abort)
    with pytest.raises(PermitIndeterminateError):
        await protected_compute(c, delayed(inc)(1), **OPTIONS)
    assert len(messages) == 1
    assert not aborts
    assert not c.refcount
    assert not c.futures
    assert not c._submission_permit_pending


@gen_cluster(
    client=True, nthreads=[("127.0.0.1", 1)], scheduler_kwargs=SCHEDULER_KWARGS
)
async def test_rejected_persist_releases_only_its_multiple_owned_futures(
    c, s, a, monkeypatch
):
    import dask.bag as db

    collection = db.from_sequence(range(6), npartitions=3).map(inc)
    persisted = await protected_persist(c, collection, **OPTIONS)
    assert await c.compute(persisted.sum()) == 21
    # The temporary reduction Future may release asynchronously.
    await asyncio.sleep(0)
    refs = dict(c.refcount)
    assert len(refs) == 3
    persist = c.persist

    def expire(*args, **kwargs):
        result = persist(*args, **kwargs)
        assert dict(c.refcount) == {key: count + 1 for key, count in refs.items()}
        s.extensions["submission-permits"].clock.now = 5
        return result

    monkeypatch.setattr(c, "persist", expire)
    with pytest.raises(PermitRejectedError):
        await protected_persist(c, collection, **OPTIONS)
    assert dict(c.refcount) == refs
    assert await c.compute(persisted.sum()) == 21

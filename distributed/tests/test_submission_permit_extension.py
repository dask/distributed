from __future__ import annotations

import asyncio

import pytest

import distributed.scheduler as scheduler_module
from distributed._submission_permit_extension import SubmissionPermitExtension
from distributed._submission_permits import ClosedPermitError, UnknownPermitError
from distributed.core import Status
from distributed.metrics import time
from distributed.scheduler import DEFAULT_EXTENSIONS
from distributed.utils_test import async_poll_for, gen_cluster, inc


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
            max_pending_per_client=2,
            max_pending=4,
            max_outcomes_per_client=2,
            clock=self.clock,
        )


SCHEDULER_KWARGS = {
    "extensions": {**DEFAULT_EXTENSIONS, "submission-permits": ClockedExtension}
}


async def acquire(c, sequence=1, duration=5):
    return await c.scheduler.submission_permit_acquire(
        client=c.id,
        epoch=c._submission_permit_capabilities["epoch"],
        sequence=sequence,
        duration=duration,
    )


@gen_cluster(client=True, nthreads=[], scheduler_kwargs=SCHEDULER_KWARGS)
async def test_rpc_acknowledges_live_permit_and_retry_does_not_extend(c, s):
    ext = s.extensions["submission-permits"]
    s.periodic_callbacks["idle-timeout"].stop()
    assert c._submission_permit_capabilities == {
        "version": 1,
        "epoch": c._submission_permit_capabilities["epoch"],
        "max_duration": 10,
    }
    first = await acquire(c)
    assert first == {"sequence": 1, "state": "pending", "duration": 5, "remaining": 5}
    assert ext.has_pending()
    s.idle_timeout = 0.01
    s.idle_since = time() - 1
    assert s.check_idle() is None
    assert s.status == Status.running
    ext.clock.now = 2
    retry = await acquire(c)
    assert retry["remaining"] == 3
    assert retry["state"] == "pending"
    epoch = c._submission_permit_capabilities["epoch"]
    result = await c.scheduler.submission_permit_abort(
        client=c.id, epoch=epoch, sequence=1
    )
    assert result["state"] == "aborted"
    assert not ext.has_pending()
    assert (
        await c.scheduler.submission_permit_status(client=c.id, epoch=epoch, sequence=1)
        == result
    )


@gen_cluster(client=True, nthreads=[], scheduler_kwargs=SCHEDULER_KWARGS)
async def test_expiry_allows_idle_shutdown_and_fences_grants_before_close(c, s):
    ext = s.extensions["submission-permits"]
    s.periodic_callbacks["idle-timeout"].stop()
    await acquire(c)
    assert s.check_idle() is None
    ext.clock.now = 5
    assert s.check_idle() is not None
    assert not ext.has_pending()
    s.idle_timeout = 0.01
    s.idle_since = time() - 1
    s.check_idle()
    # close has been queued but cannot have run until this coroutine yields.
    assert s.status == Status.running
    with pytest.raises(ClosedPermitError):
        ext.acquire(c.id, c._submission_permit_capabilities["epoch"], 2, 1)
    await s.finished()
    assert s.status == Status.closed


@gen_cluster(client=True, nthreads=[], scheduler_kwargs=SCHEDULER_KWARGS)
async def test_rpc_rejects_unknown_or_stale_connection(c, s):
    with pytest.raises(UnknownPermitError, match="stale"):
        await c.scheduler.submission_permit_acquire(
            client=c.id, epoch="old-epoch", sequence=1, duration=1
        )
    with pytest.raises(ClosedPermitError, match="not running"):
        await c.scheduler.submission_permit_acquire(
            client="not-connected", epoch="missing", sequence=1, duration=1
        )
    assert not s.extensions["submission-permits"].has_pending()


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)],
    scheduler_kwargs=SCHEDULER_KWARGS,
)
async def test_tagged_graph_transfers_into_active_guard_and_computes(c, s, a):
    ext = s.extensions["submission-permits"]
    s.periodic_callbacks["idle-timeout"].stop()
    epoch = c._submission_permit_capabilities["epoch"]
    await acquire(c)
    accepted = asyncio.Queue()
    c._stream_handlers["submission-permit-admission"] = lambda **msg: (
        accepted.put_nowait(msg)
    )
    transfer = ext.transfer
    observations = []

    def observe_transfer(*args):
        fresh = transfer(*args)
        observations.append(
            (s._active_graph_updates, ext.has_pending(), s.check_idle())
        )
        return fresh

    ext.transfer = observe_transfer
    send = c._send_to_scheduler

    def tagged_send(msg):
        if msg["op"] == "update-graph":
            msg = dict(msg, submission_epoch=epoch, submission_sequence=1)
        send(msg)

    c._send_to_scheduler = tagged_send
    future = c.submit(inc, 1, key="protected")
    assert await accepted.get() == {"epoch": epoch, "sequence": 1, "status": "accepted"}
    assert await future == 2
    assert observations == [(1, False, None)]
    assert ext.registry.status(c.id, epoch, 1).state == "accepted"
    assert s._active_graph_updates == 0
    s.validate_state()


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_legacy_scheduler_has_no_capability_and_computes(c, s, a):
    assert c._submission_permit_capabilities is None
    assert "submission-permits" not in s.extensions
    assert "submission_permit_acquire" not in s.handlers
    assert await c.submit(inc, 1) == 2


@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)],
    scheduler_kwargs=SCHEDULER_KWARGS,
)
async def test_untagged_graph_works_with_optional_extension(c, s, a):
    assert await c.submit(inc, 1) == 2
    assert not s.extensions["submission-permits"].has_pending()


@pytest.mark.parametrize(
    "case,reason",
    [
        ("expired", "ExpiredPermitError"),
        ("aborted", "AbortedPermitError"),
        ("retired", "RetiredPermitError"),
        ("unknown", "UnknownPermitError"),
        ("stale", "UnknownPermitError"),
        ("missing-epoch", "ValueError"),
        ("missing-sequence", "ValueError"),
        ("invalid-sequence", "ValueError"),
        ("null-tags", "ValueError"),
        ("invalid-epoch", "ValueError"),
        ("closed", "AbortedPermitError"),
    ],
)
@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)],
    scheduler_kwargs=SCHEDULER_KWARGS,
)
async def test_rejected_admission_does_not_process_graph_or_poison_key(
    c, s, a, case, reason, monkeypatch
):
    future = c.submit(inc, 1, key="existing")
    assert await future == 2
    state = s.tasks[future.key]
    ext = s.extensions["submission-permits"]
    epoch = c._submission_permit_capabilities["epoch"]
    await acquire(c)
    tag = {"submission_epoch": epoch, "submission_sequence": 1}
    if case == "expired":
        ext.clock.now = 5
    elif case == "aborted":
        ext.abort(c.id, epoch, 1)
    elif case == "retired":
        for sequence in (1, 2, 3):
            ext.acquire(c.id, epoch, sequence, 1)
            ext.abort(c.id, epoch, sequence)
    elif case == "unknown":
        tag["submission_sequence"] = 99
    elif case == "stale":
        tag["submission_epoch"] = "previous-connection"
    elif case == "missing-epoch":
        tag.pop("submission_epoch")
    elif case == "missing-sequence":
        tag.pop("submission_sequence")
    elif case == "invalid-sequence":
        tag["submission_sequence"] = True
    elif case == "null-tags":
        tag = {"submission_epoch": None, "submission_sequence": None}
    elif case == "invalid-epoch":
        tag["submission_epoch"] = [epoch]
    elif case == "closed":
        ext.commit_idle_shutdown()
    results = asyncio.Queue()
    c._stream_handlers["submission-permit-admission"] = lambda **msg: (
        results.put_nowait(msg)
    )
    graph_calls = []
    key_messages = []

    def unexpected_deserialize(*args):
        graph_calls.append(args)
        raise AssertionError("rejected request entered graph deserialization")

    report = s.report

    def observe_report(msg, *args, **kwargs):
        if msg["op"] in ("task-erred", "cancelled-keys"):
            key_messages.append(msg)
        report(msg, *args, **kwargs)

    monkeypatch.setattr(scheduler_module, "deserialize", unexpected_deserialize)
    monkeypatch.setattr(s, "report", observe_report)
    c._send_to_scheduler(
        {
            "op": "update-graph",
            "expr_ser": None,
            "keys": {"existing", "unsubmitted"},
            "span_metadata": {},
            "internal_priority": None,
            "submitting_task": None,
            **tag,
        }
    )
    result = await results.get()
    assert result["status"] == "rejected"
    assert result["reason"] == reason
    assert results.empty()
    assert not graph_calls
    assert not key_messages
    assert "unsubmitted" not in s.tasks
    assert s.tasks[future.key] is state
    assert state.state == "memory"
    assert await future == 2
    assert s._active_graph_updates == 0


@pytest.mark.parametrize("same_graph", [False, True])
@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)],
    scheduler_kwargs=SCHEDULER_KWARGS,
)
async def test_consumed_sequence_rejects_every_replay(c, s, a, same_graph):
    await acquire(c)
    epoch = c._submission_permit_capabilities["epoch"]
    results = asyncio.Queue()
    c._stream_handlers["submission-permit-admission"] = lambda **msg: (
        results.put_nowait(msg)
    )
    send = c._send_to_scheduler
    messages = []

    def tagged_send(msg):
        if msg["op"] == "update-graph":
            msg = dict(msg, submission_epoch=epoch, submission_sequence=1)
            messages.append(msg.copy())
        send(msg)

    c._send_to_scheduler = tagged_send
    future = c.submit(inc, 1, key="original")
    assert (await results.get())["status"] == "accepted"
    assert await future == 2
    replay = messages[0]
    if not same_graph:
        replay = dict(replay, expr_ser=None, keys={"other-graph"})
    send(replay)
    assert await results.get() == {
        "epoch": epoch,
        "sequence": 1,
        "status": "rejected",
        "reason": "sequence-already-consumed",
    }
    assert results.empty()
    assert "other-graph" not in s.tasks
    assert await future == 2
    assert s._active_graph_updates == 0


@gen_cluster(client=True, nthreads=[("127.0.0.1", 1)])
async def test_unsupported_tag_rejects_without_graph_work(c, s, a):
    results = asyncio.Queue()
    c._stream_handlers["submission-permit-admission"] = lambda **msg: (
        results.put_nowait(msg)
    )
    c._send_to_scheduler(
        {
            "op": "update-graph",
            "expr_ser": None,
            "keys": {"unsupported"},
            "span_metadata": {},
            "internal_priority": None,
            "submitting_task": None,
            "submission_epoch": "unknown",
            "submission_sequence": 1,
        }
    )
    result = await results.get()
    assert result["status"] == "rejected"
    assert result["reason"] == "ValueError"
    assert not s.tasks
    assert s._active_graph_updates == 0


@pytest.mark.parametrize("disconnect", [False, True])
@gen_cluster(
    client=True,
    nthreads=[("127.0.0.1", 1)],
    scheduler_kwargs=SCHEDULER_KWARGS,
)
async def test_admission_precedes_materialization_and_checks_connection_afterward(
    c, s, a, disconnect, monkeypatch
):
    ext = s.extensions["submission-permits"]
    epoch = c._submission_permit_capabilities["epoch"]
    await acquire(c)
    results = asyncio.Queue()
    c._stream_handlers["submission-permit-admission"] = lambda **msg: (
        results.put_nowait(msg)
    )
    entered = asyncio.Event()
    release = asyncio.Event()
    offload = scheduler_module.offload

    async def paused_offload(func, *args, **kwargs):
        if func is scheduler_module._materialize_graph:
            entered.set()
            await release.wait()
        return await offload(func, *args, **kwargs)

    monkeypatch.setattr(scheduler_module, "offload", paused_offload)
    send = c._send_to_scheduler

    def tagged_send(msg):
        if msg["op"] == "update-graph":
            msg = dict(msg, submission_epoch=epoch, submission_sequence=1)
        send(msg)

    c._send_to_scheduler = tagged_send
    try:
        future = c.submit(inc, 1, key="paused-graph")
        assert (await results.get())["status"] == "accepted"
        await entered.wait()
        assert s._active_graph_updates == 1
        assert ext.registry.status(c.id, epoch, 1).state == "accepted"
        assert not s.tasks
        assert s.check_idle() is None
        if disconnect:
            # Close the captured scheduler-side transport while the handler is
            # awaiting graph preparation. Reconnect must wait for this handler.
            s.client_comms[c.id].comm.abort()
        release.set()
        if disconnect:
            await async_poll_for(lambda: s._active_graph_updates == 0)
            assert "paused-graph" not in s.tasks
        else:
            assert await future == 2
        assert results.empty()
    finally:
        release.set()

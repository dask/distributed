from __future__ import annotations

import asyncio
from functools import partial
from typing import Any, cast

import pytest

import distributed.scheduler as scheduler_module
from distributed._submission_permit_extension import SubmissionPermitExtension
from distributed.comm.core import Comm, CommClosedError, connect
from distributed.core import Status
from distributed.scheduler import DEFAULT_EXTENSIONS
from distributed.utils import wait_for
from distributed.utils_test import async_poll_for, gen_cluster

pytestmark = pytest.mark.ci1


PERMIT_EXTENSIONS = {
    **DEFAULT_EXTENSIONS,
    "submission-permits": partial(
        SubmissionPermitExtension,
        max_duration=10,
        max_pending_per_client=2,
        max_pending=4,
        max_outcomes_per_client=2,
    ),
}


@gen_cluster(nthreads=[])
async def test_legacy_overlapping_client_cleanup_still_closes_replacement(
    s, monkeypatch
):
    client = "legacy-same-client"
    first = await connect(s.address)
    second = await connect(s.address)
    replacement = None
    finished = {
        first.local_address: asyncio.Event(),
        second.local_address: asyncio.Event(),
    }
    add_client = s.handlers["register-client"]

    async def track_client(comm: Comm, client: str, versions: dict[str, Any]) -> None:
        peer = comm.peer_address
        try:
            await add_client(comm=comm, client=client, versions=versions)
        finally:
            if peer in finished:
                finished[peer].set()

    async def register(comm: Comm) -> None:
        await comm.write(
            {"op": "register-client", "client": client, "reply": False, "versions": {}}
        )
        messages = await wait_for(comm.read(), 1)
        assert messages[0]["op"] == "stream-start"
        assert "submission-permits" not in messages[0]

    monkeypatch.setitem(s.handlers, "register-client", track_client)
    try:
        await register(first)
        await register(second)
        await first.close()
        await wait_for(finished[first.local_address].wait(), 1)
        assert client not in s.client_comms
        with pytest.raises(CommClosedError):
            await wait_for(second.read(), 1)
        # Both finalizers must finish before a third registration can race them.
        await wait_for(
            asyncio.gather(*(event.wait() for event in finished.values())), 1
        )
        assert client not in s.clients
        assert client not in s.client_comms

        replacement = await connect(s.address)
        await register(replacement)
        assert client in s.clients
        assert client in s.client_comms
        assert not s.client_comms[client].closed()
    finally:
        for comm in (replacement, second, first):
            if comm is not None and not comm.closed():
                await comm.close()


async def _register(comm: Comm, client: str) -> dict:
    await comm.write(
        {
            "op": "register-client",
            "client": client,
            "reply": False,
            "versions": {},
        }
    )
    msg = await wait_for(comm.read(), 1)
    assert msg[0]["op"] == "stream-start"
    return msg[0]["submission-permits"]


async def _assert_registration_is_refused(comm: Comm, client: str) -> None:
    await comm.write(
        {
            "op": "register-client",
            "client": client,
            "reply": False,
            "versions": {},
        }
    )
    with pytest.raises(CommClosedError):
        await wait_for(comm.read(), 1)


@pytest.mark.parametrize(
    "permits,close_error", [(False, False), (False, True), (True, False), (True, True)]
)
def test_add_client_finalizer_handles_legacy_and_permit_streams(
    monkeypatch, permits, close_error
):
    class Comm:
        def closed(self):
            return False

    class BatchedSend:
        def __init__(self, interval, loop):
            self.messages = []

        def start(self, comm):
            pass

        def send(self, msg):
            self.messages.append(msg)

        async def close(self):
            if close_error:
                raise TypeError("comm became None")

    class PermitExtension:
        def register_client(self, client):
            return "epoch"

        def capabilities(self, epoch):
            return {}

        def unregister_client(self, client, epoch):
            assert epoch == "epoch"

    class Scheduler:
        def __init__(self):
            self.status = Status.running
            self.extensions = (
                {"submission-permits": PermitExtension()} if permits else {}
            )
            self.handlers: dict[str, Any] = {}
            self.client_comms = {}
            self.clients = {}
            self.workers = {}
            self.plugins = {}
            self.loop = None
            self._client_connections_added_total = 0

        def log_event(self, *args):
            pass

        async def handle_stream(self, **kwargs):
            pass

        def remove_client(self, **kwargs):
            pass

        def _is_finalizing(self):
            return False

    async def run():
        scheduler = Scheduler()
        await scheduler_module.Scheduler.add_client(
            cast(Any, scheduler), Comm(), "client", {}
        )
        return scheduler

    monkeypatch.setattr(scheduler_module, "BatchedSend", BatchedSend)
    scheduler = asyncio.run(run())
    if not close_error:
        assert scheduler.client_comms == {}


@gen_cluster(nthreads=[], scheduler_kwargs={"extensions": PERMIT_EXTENSIONS})
async def test_submission_permits_reject_overlapping_client_registration(s):
    client = "same-client"
    first_comm = await connect(s.address)
    second_comm = None
    third_comm = None
    replacement_comm = None
    allow_close = None
    try:
        first_capabilities = await _register(first_comm, client)
        old_epoch = first_capabilities["epoch"]
        old_bcomm = s.client_comms[client]
        added_total = s._client_connections_added_total
        old_client_state = s.clients[client]

        second_comm = await connect(s.address)
        await _assert_registration_is_refused(second_comm, client)
        assert s.clients[client] is old_client_state
        assert s.client_comms[client] is old_bcomm
        assert s.extensions["submission-permits"].registry.is_current(client, old_epoch)
        assert s._client_connections_added_total == added_total

        close_started = asyncio.Event()
        allow_close = asyncio.Event()
        original_close = old_bcomm.close

        async def delayed_close() -> None:
            close_started.set()
            await allow_close.wait()
            await original_close()

        old_bcomm.close = delayed_close
        await first_comm.close()
        await wait_for(close_started.wait(), 1)

        # The old BatchedSend remains the connection fence until its close completes.
        assert s.client_comms[client] is old_bcomm
        third_comm = await connect(s.address)
        await _assert_registration_is_refused(third_comm, client)
        assert s.client_comms[client] is old_bcomm

        allow_close.set()
        await async_poll_for(lambda: client not in s.client_comms)

        replacement_comm = await connect(s.address)
        new_capabilities = await _register(replacement_comm, client)
        new_epoch = new_capabilities["epoch"]
        assert new_epoch != old_epoch
        extension = s.extensions["submission-permits"]
        assert extension.registry.is_current(client, new_epoch)

        # A delayed old disconnect cleanup must be fenced by the old epoch.
        extension.unregister_client(client, old_epoch)
        assert extension.registry.is_current(client, new_epoch)
        assert extension.acquire(client, new_epoch, 1, 1)["state"] == "pending"
    finally:
        if allow_close is not None:
            allow_close.set()
        for comm in (second_comm, third_comm, replacement_comm, first_comm):
            if comm is not None and not comm.closed():
                await comm.close()


@gen_cluster(
    client=True, nthreads=[], scheduler_kwargs={"extensions": PERMIT_EXTENSIONS}
)
async def test_submission_permits_client_reconnect_waits_for_old_comm_cleanup(c, s):
    client = c.id
    old_bcomm = s.client_comms[client]
    old_epoch = c._submission_permit_capabilities["epoch"]
    added_total = s._client_connections_added_total
    close_started = asyncio.Event()
    allow_close = asyncio.Event()
    original_close = old_bcomm.close

    async def delayed_close() -> None:
        close_started.set()
        await allow_close.wait()
        await original_close()

    old_bcomm.close = delayed_close
    try:
        await c.scheduler_comm.comm.close()
        await wait_for(close_started.wait(), 1)

        # The client's automatic retries use the same ID but cannot replace the
        # still-closing connection or receive a fresh capability epoch yet.
        await async_poll_for(lambda: c.status == "connecting")
        assert c._submission_permit_capabilities is None
        assert s.client_comms[client] is old_bcomm
        assert s._client_connections_added_total == added_total

        allow_close.set()
        await async_poll_for(lambda: c.status == "running")
        new_epoch = c._submission_permit_capabilities["epoch"]
        assert new_epoch != old_epoch
        assert s.extensions["submission-permits"].registry.is_current(client, new_epoch)

        reply = await c.scheduler.submission_permit_acquire(
            client=client, epoch=new_epoch, sequence=1, duration=1
        )
        assert reply["state"] == "pending"
    finally:
        allow_close.set()


@gen_cluster(nthreads=[], scheduler_kwargs={"extensions": PERMIT_EXTENSIONS})
async def test_submission_permits_reject_client_registration_after_idle_commit(s):
    extension = s.extensions["submission-permits"]
    extension.commit_idle_shutdown()

    client = "after-idle-commit"
    comm = await connect(s.address)
    try:
        await _assert_registration_is_refused(comm, client)
        assert client not in s.clients
        assert client not in s.client_comms
        assert not extension.registry.is_current(client, "not-an-epoch")
        assert s._client_connections_added_total == 0
    finally:
        if not comm.closed():
            await comm.close()

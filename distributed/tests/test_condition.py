from __future__ import annotations

import asyncio
import logging
import uuid
from collections import defaultdict, deque

from dask.utils import parse_timedelta

from distributed.utils import SyncMethodMixin, TimeoutError, log_errors, wait_for
from distributed.worker import get_client

logger = logging.getLogger(__name__)


class ConditionExtension:
    """Scheduler extension managing Condition lock and notifications

    State managed:
    - _locks: Which client holds which condition's lock
    - _acquire_queue: Clients waiting to acquire lock (FIFO)
    - _waiters: Clients in wait() (released lock, awaiting notify)
    - _client_conditions: Reverse index for cleanup on disconnect
    """

    def __init__(self, scheduler):
        self.scheduler = scheduler

        # {condition_name: client_id} - who holds each lock
        self._locks = {}

        # {condition_name: deque[(client_id, future)]} - waiting to acquire
        self._acquire_queue = defaultdict(deque)

        # {condition_name: {waiter_id: (client_id, event, reacquire_future)}}
        # - clients in wait(), will need to reacquire after notify
        self._waiters = defaultdict(dict)

        # {client_id: set(condition_names)} - for cleanup on disconnect
        self._client_conditions = defaultdict(set)

        self.scheduler.handlers.update(
            {
                "condition_acquire": self.acquire,
                "condition_release": self.release,
                "condition_wait": self.wait,
                "condition_notify": self.notify,
                "condition_notify_all": self.notify_all,
            }
        )

        # Register cleanup on client disconnect
        self.scheduler.extensions["conditions"] = self

    def _track_client(self, name, client_id):
        """Track that a client is using this condition"""
        self._client_conditions[client_id].add(name)

    def _untrack_client(self, name, client_id):
        """Stop tracking client for this condition"""
        if client_id in self._client_conditions:
            self._client_conditions[client_id].discard(name)
            if not self._client_conditions[client_id]:
                del self._client_conditions[client_id]

    @log_errors
    async def acquire(self, name=None, client_id=None):
        """Acquire lock - blocks until available"""
        self._track_client(name, client_id)

        if name not in self._locks:
            # Lock is free
            self._locks[name] = client_id
            return True

        if self._locks[name] == client_id:
            # Re-entrant acquire (from same client)
            return True

        # Lock is held - queue up and wait
        future = asyncio.Future()

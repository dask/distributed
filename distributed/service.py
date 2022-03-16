from __future__ import annotations

"""
Stateful services that run on all workers, for doing out-of-band operations in a controlled way.
"""
from typing import (
    Any,
    Awaitable,
    Callable,
    Generic,
    Mapping,
    NewType,
    Protocol,
    Sequence,
    TypeVar,
)

ServiceGroup = NewType("ServiceGroup", str)
# ^ TODO need a better term. This refers to a single key in the graph, which creates Service instances
# across many workers to accomplish its task. `ServiceInvocation`? `ServiceTask`? `ServiceGroup`?
# There can be multiple types of Services (`ShuffleService`, `RechunkService`, `XGBoostService`),
# and each one can be invoked multiple times (`'shuffle-1234'`, `'shuffle-5678'` `'rechunk-9876'`).
# Each invocation is what we're calling the `ServiceID`. We need a term for both the invocation in general,
# and for the group of instances (across many workers) handling that invocation.
Address = NewType("Address", str)


class Service:
    def __init__(self, *args, **kwargs) -> None:
        ...

    async def start(
        self: T,
        *,
        group: ServiceGroup,
        peers: Mapping[Address, ServiceHandle[T]],
        # ^ QUESTION: should peers be identified by address? Or should they get some opaque UUID?
        # Addresses aren't guaranteed to be unique. It might be helpful to guarantee that
        # the identifier for a peer always corresponds to a unique Service _instance_
        # (if a worker restarts, the same address could correspond to multiple different instances
        # of the Service).
        input_keys: Sequence[str],
        output_keys: Sequence[str],
        concierge: Concierge,
        produce_key_callback: Callable[[str, Any], Awaitable[None]],
        leader: bool,
        **kwargs,
    ) -> None:
        """
        Called to start the `Service` on a `Worker`.

        Once ``start`` returns, the `Service` instance is considered ready, and the
        `Worker` can call ``add_key``.

        Parameters
        ----------
        group:
            The key name in the graph that created the Service. All instances created
            from that key (across all workers) have the same ServiceGroup.

            Multiple instances of the same type of Service can exist on a worker at
            once, but all are guaranteed to have different ``groups``.
        peers:
            All the worker addresses (including this one) on which this ServiceGroup is
            simultaneously being started, and `ServiceHandles` to communicate with them.

            Note that peer services are not guaranteed to be running yet.
        input_keys:
            The input keys this ServiceGroup will receive, *across all workers*, in
            priority order.
        output_keys:
            The output keys this ServiceGroup is expected to produce, *across all
            workers*, in priority order.
        concierge:
            A `Concierge` instance the `Service` can use to communicate with the `Worker`.
            Used to hand off output keys back to Dask, restart or fail the ServiceGroup,
            etc.
        leader:
            Whether this instance was chosen to be the "leader" of the ServiceGroup. The
            scheduler will pick exactly one instance in ``peers`` to be the leader. The
            order in which `Service` instances start up on workers is of course undefined;
            the leader may not be the first instance to actually start.

            The leader has no special treatment, and which instance is selected as the
            leader is irrelevant to Dask. This flag is passed purely as a convenience
            for Services, to make it easier to coordinate initialization tasks that must
            run exactly once.
        **kwargs:
            Forward-compatibility only—any additional arguments can be ignored.
        """
        ...

    # Not going to implement `all_started`; it's way too brittle.
    # What if some peers die before they start?
    # Then those addresses will _never_ start. Probably better to let Services
    # that need this sort of coordination handle it themselves via a Semaphore.

    # async def all_started(self) -> None:
    #     """
    #     Called once all instances in the original ``peers`` list have started.

    #     Do not make the ``start`` method block until ``all_started`` has been called;
    #     this will cause a deadlock.
    #     """
    #     ...

    async def add_key(self, key: str, data: Any) -> None:
        """
        Called by the `Worker` to "hand off" data to the `Service`.

        Once ``add_key`` returns, this `Service` instance owns ``data``, and the
        `Worker` will release all references to the data once no other tasks or clients
        are also waiting on ``key``.

        ``add_key`` can be called as soon as ``start`` has returned. It will only be
        called with keys in ``input_keys``.
        """
        ...

    # Peers joining and leaving won't be supported in the first iteration.
    # For now, new workers will be ignored, and any worker leaving will trigger
    # a restart of the whole service group? (Don't want to have this be released behavior
    # though, since it'll make adding `peer_joined`/`peer_left` backwards-incompatible.)
    # * Dealing with peers joining is somewhat complex. Do current instances get to
    #   veto whether a new peer can join, or does any worker satisfying the restrictions
    #   of the Service task get to join automatically? I assume `peer_joined` doesn't get
    #   called until after `start` has returned on the new instance. What `peers` list gets
    #   passed to the new peer (all current ones, I assume)? What happens when other workers
    #   come up while the new peer's `start` is running—how do we buffer those `peer_joined`
    #   messages to deliver to the peer that hasn't started up yet?
    # * `peer_left` seems more straightforward.
    # In general though, these may not scale well. In a 10k worker cluster, notifying every other
    # worker when one comes and goes is a lot of noise.
    # And mostly, if services would decide that a peer leaving (or joining) means they should
    # restart, then the scheduler will be bombarded by restart messages from (N-1) workers
    # when one worker leaves.

    # async def peer_joined(self: T, addr: Address, handle: ServiceHandle[T]) -> None:
    #     ...

    # async def peer_left(self, addr: Address) -> None:
    #     ...

    async def stop(self) -> None:
        """
        Called by the `Worker` when this ServiceGroup should stop.

        Cases in which `stop` is called:
        * Success: all ``output_keys`` have been produced.
        * Failure: `Concierge.error` was called, or `Service.add_key` raised an error.
        * Restart: `Concierge.restart` was called
        * Restart: a peer worker left (TODO remove this case!).

        The `Service` is not informed what the cause for stopping is.

        The `Service` must clean up all state (files, connections, data, etc.)
        before `stop` returns.

        After `stop` returns, a new `Service` instance with the same ServiceGroup
        may be created on the `Worker`.
        """
        ...


T = TypeVar("T", bound=Service)
# TODO: use pep-673 Self type https://peps.python.org/pep-0673/


class Concierge:
    "Interface for `Service` instances to update the progress of a ServiceGroup"
    id: ServiceGroup

    async def produce_key(self, key: str, data: Any) -> None:
        """
        Hand off output data from a `Service` back to Dask.

        Once ``await produce_key(key, data)`` has returned, the data is owned by the
        worker and tracked by the scheduler. The service should release all references
        to the data.

        If ``produce_key`` raises an error, the state of the data is undefined. In most
        cases, the `Service` should call `restart` if this happens.
        """
        ...

    async def restart(self) -> None:
        """
        Request that the ServiceGroup be restarted.

        `Service.stop` is guaranteed not to be called until after `restart` has
        returned.

        Once `restart` has returned, `Service.stop` will be called on all instances for
        the ServiceGroup, and workers will release their references those instances.
        Once `stop` has returned on all instances, the ServiceGroup's task will
        transition to ``waiting`` on the scheduler. Then, it will transition back to
        ``processing``, and new `Service` instances (with the same ServiceGroup) will be
        started on all workers.
        """
        ...

    async def error(self, exc: Exception) -> None:
        """
        Fail the ServiceGroup with an error message.

        `Service.stop` is guaranteed not to be called until after `error` has returned.

        Once `restart` has returned, `Service.stop` will be called on all instances for
        the ServiceGroup, and workers will release their references those instances.
        Once `stop` has returned on all instances, the ServiceGroup's task will
        transition to ``erred`` on the scheduler. The result of the task will be
        ``exc``.

        If multiple instances call `error`, only the first ``exc`` to reach the
        scheduler will become the result; all others will be dropped (though all calls
        to `error` are logged on the `Worker`).
        """
        ...


# TODO: how to type annotate RPCs? https://github.com/python/mypy/issues/5523
class ServiceHandle(Generic[T]):
    "RPC to a Service instance running on another worker"

    def __getattr__(self, attr: str) -> RPC:
        ...


# https://stackoverflow.com/a/65564936/17100540
class RPC(Protocol):
    def __call__(self, *args, **kwargs) -> Awaitable:
        ...

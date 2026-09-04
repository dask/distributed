from __future__ import annotations

from dataclasses import asdict

import pytest

from distributed._submission_permits import (
    AbortedPermitError,
    ClosedPermitError,
    ExpiredPermitError,
    PermitCapacityError,
    RetiredPermitError,
    SubmissionPermitRegistry,
    UnknownPermitError,
)


class Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def registry() -> tuple[SubmissionPermitRegistry, Clock, str]:
    clock = Clock()
    permits = SubmissionPermitRegistry(10, 2, 3, 2, clock)
    return permits, clock, permits.register("client")


def test_acquire_retry_and_transfer_are_one_shot(
    registry: tuple[SubmissionPermitRegistry, Clock, str],
) -> None:
    permits, clock, epoch = registry
    first = permits.acquire("client", epoch, 1, 5)
    clock.now = 2
    retry = permits.acquire("client", epoch, 1, 5)

    assert first == first.__class__(1, "pending", 5.0, 5.0)
    assert retry == retry.__class__(1, "pending", 5.0, 3.0)
    assert asdict(retry) == retry.to_dict()
    assert permits.transfer("client", epoch, 1) is True
    assert permits.transfer("client", epoch, 1) is False
    assert permits.status("client", epoch, 1).state == "accepted"


def test_overlap_abort_and_expiry(
    registry: tuple[SubmissionPermitRegistry, Clock, str],
) -> None:
    permits, clock, epoch = registry
    permits.acquire("client", epoch, 1, 2)
    permits.acquire("client", epoch, 2, 4)
    assert permits.has_pending()
    assert permits.abort("client", epoch, 1).state == "aborted"
    assert permits.abort("client", epoch, 1).state == "aborted"
    with pytest.raises(AbortedPermitError):
        permits.transfer("client", epoch, 1)

    clock.now = 4
    assert not permits.has_pending()
    assert permits.status("client", epoch, 2).state == "expired"
    with pytest.raises(ExpiredPermitError):
        permits.transfer("client", epoch, 2)


def test_terminal_outcome_eviction_retires_old_sequences(
    registry: tuple[SubmissionPermitRegistry, Clock, str],
) -> None:
    permits, _, epoch = registry
    for sequence in (1, 2, 3):
        permits.acquire("client", epoch, sequence, 1)
        assert permits.transfer("client", epoch, sequence)

    assert permits.status("client", epoch, 1).state == "retired"
    with pytest.raises(RetiredPermitError):
        permits.acquire("client", epoch, 1, 1)
    with pytest.raises(RetiredPermitError):
        permits.transfer("client", epoch, 1)
    assert permits.status("client", epoch, 4).state == "unknown"


def test_out_of_order_sequence_is_rejected_after_a_higher_admission(
    registry: tuple[SubmissionPermitRegistry, Clock, str],
) -> None:
    permits, _, epoch = registry
    permits.acquire("client", epoch, 2, 1)
    with pytest.raises(RetiredPermitError):
        permits.acquire("client", epoch, 1, 1)


def test_pending_capacities_do_not_consume_admission_sequence() -> None:
    clock = Clock()
    permits = SubmissionPermitRegistry(5, 1, 1, 1, clock)
    first_epoch = permits.register("first")
    second_epoch = permits.register("second")
    permits.acquire("first", first_epoch, 1, 1)
    with pytest.raises(PermitCapacityError):
        permits.acquire("first", first_epoch, 2, 1)
    with pytest.raises(PermitCapacityError):
        permits.acquire("second", second_epoch, 1, 1)
    assert permits.abort("first", first_epoch, 1).state == "aborted"
    assert permits.acquire("second", second_epoch, 1, 1).state == "pending"


def test_reconnect_invalidates_old_pending_and_stale_cleanup(
    registry: tuple[SubmissionPermitRegistry, Clock, str],
) -> None:
    permits, _, old_epoch = registry
    permits.acquire("client", old_epoch, 1, 1)
    new_epoch = permits.register("client")

    assert not permits.is_current("client", old_epoch)
    assert permits.is_current("client", new_epoch)
    assert not permits.unregister("client", old_epoch)
    assert permits.acquire("client", new_epoch, 1, 1).state == "pending"
    with pytest.raises(UnknownPermitError):
        permits.status("client", old_epoch, 1)
    assert permits.unregister("client", new_epoch)
    assert not permits.has_pending()


def test_close_aborts_live_permits_and_rejects_new_registration_and_grants(
    registry: tuple[SubmissionPermitRegistry, Clock, str],
) -> None:
    permits, _, epoch = registry
    permits.acquire("client", epoch, 1, 1)
    permits.close()

    assert permits.status("client", epoch, 1).state == "aborted"
    assert not permits.has_pending()
    with pytest.raises(ClosedPermitError):
        permits.acquire("client", epoch, 2, 1)
    with pytest.raises(ClosedPermitError):
        permits.register("new-client")


@pytest.mark.parametrize(
    "kwargs",
    [
        {"max_duration": 0},
        {"max_duration": float("inf")},
        {"max_duration": True},
        {"max_pending_per_client": 0},
        {"max_pending": False},
        {"max_outcomes_per_client": -1},
    ],
)
def test_constructor_rejects_invalid_bounds(kwargs: dict[str, object]) -> None:
    options: dict[str, object] = {
        "max_duration": 1,
        "max_pending_per_client": 1,
        "max_pending": 1,
        "max_outcomes_per_client": 1,
    }
    options.update(kwargs)
    with pytest.raises(ValueError):
        SubmissionPermitRegistry(**options)  # type: ignore[arg-type]


@pytest.mark.parametrize("sequence", [0, -1, True, 2**63])
def test_rejects_invalid_sequences(
    registry: tuple[SubmissionPermitRegistry, Clock, str], sequence: object
) -> None:
    permits, _, epoch = registry
    with pytest.raises(ValueError):
        permits.acquire("client", epoch, sequence, 1)  # type: ignore[arg-type]


@pytest.mark.parametrize("duration", [0, -1, True, float("nan"), float("inf"), 11])
def test_rejects_invalid_durations(
    registry: tuple[SubmissionPermitRegistry, Clock, str], duration: object
) -> None:
    permits, _, epoch = registry
    with pytest.raises(ValueError):
        permits.acquire("client", epoch, 1, duration)  # type: ignore[arg-type]

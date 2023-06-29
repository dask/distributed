from __future__ import annotations

import pytest

from distributed.client import wait
from distributed.utils_test import (
    fetch_metrics,
    fetch_metrics_sample_names,
    gen_cluster,
    slowinc,
)


@gen_cluster(client=True)
async def test_prometheus(c, s, a, b):
    pytest.importorskip("prometheus_client")
    active_metrics = await fetch_metrics_sample_names(
        s.http_server.port, prefix="dask_stealing_"
    )
    expected_metrics = {
        "dask_stealing_request_count_total",
        "dask_stealing_request_cost_total",
    }

    assert active_metrics == expected_metrics


@gen_cluster(client=True)
async def test_prometheus_collect_count_total_by_cost_multipliers(c, s, a, b):
    pytest.importorskip("prometheus_client")

    async def fetch_metrics_by_cost_multipliers():
        families = await fetch_metrics(s.http_server.port, prefix="dask_stealing_")
        active_metrics = {
            sample.labels["cost_multiplier"]: sample.value
            for sample in families["dask_stealing_request_count"].samples
            if sample.name == "dask_stealing_request_count_total"
        }
        return active_metrics

    active_metrics = await fetch_metrics_by_cost_multipliers()
    stealing = s.extensions["stealing"]
    expected_metrics = {str(multiplier): 0 for multiplier in stealing.cost_multipliers}
    assert active_metrics == expected_metrics

    futures = c.map(
        slowinc, range(10), delay=0.1, workers=a.address, allow_other_workers=True
    )
    await wait(futures)

    active_metrics = await fetch_metrics_by_cost_multipliers()
    assert len(active_metrics) == len(stealing.cost_multipliers)
    count = sum(active_metrics.values())
    assert count > 0
    expected_count = sum(
        len(event[1]) for _, event in s.events["stealing"] if event[0] == "request"
    )
    assert count == expected_count


@gen_cluster(client=True)
async def test_prometheus_collect_cost_total_by_cost_multipliers(c, s, a, b):
    pytest.importorskip("prometheus_client")

    async def fetch_metrics_by_cost_multipliers():
        families = await fetch_metrics(s.http_server.port, prefix="dask_stealing_")
        active_metrics = {
            sample.labels["cost_multiplier"]: sample.value
            for sample in families["dask_stealing_request_cost"].samples
            if sample.name == "dask_stealing_request_cost_total"
        }
        return active_metrics

    active_metrics = await fetch_metrics_by_cost_multipliers()
    stealing = s.extensions["stealing"]
    expected_metrics = {str(multiplier): 0 for multiplier in stealing.cost_multipliers}
    assert active_metrics == expected_metrics

    futures = c.map(
        slowinc, range(10), delay=0.1, workers=a.address, allow_other_workers=True
    )
    await wait(futures)

    active_metrics = await fetch_metrics_by_cost_multipliers()
    assert len(active_metrics) == len(stealing.cost_multipliers)
    count = sum(active_metrics.values())
    assert count > 0
    expected_cost = sum(
        request[3]
        for _, event in s.events["stealing"]
        for request in event[1]
        if event[0] == "request"
    )
    assert count == expected_cost


@gen_cluster(
    client=True,
    clean_kwargs={"threads": False},
    scheduler_kwargs={"extensions": {}},
    worker_kwargs={"extensions": {}},
)
async def test_prometheus_without_stealing_extension(c, s, a, b):
    pytest.importorskip("prometheus_client")

    active_metrics = await fetch_metrics(s.http_server.port, "dask_stealing_")
    assert not active_metrics

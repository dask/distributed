from __future__ import annotations

import pytest

memray = pytest.importorskip("memray")

pytestmark = pytest.mark.extra_packages

from distributed.diagnostics.memray import memray_scheduler, memray_workers


@pytest.mark.parametrize("fetch_reports_parallel", [True, False, 1])
def test_basic_integration_workers(client, tmp_path, fetch_reports_parallel):
    with memray_workers(tmp_path, fetch_reports_parallel=fetch_reports_parallel):
        pass

    assert len(list(tmp_path.glob("*.html"))) == 2


@pytest.mark.parametrize("report_args", [("flamegraph", "--leaks"), False])
def test_basic_integration_workers_report_args(client, tmp_path, report_args):
    with memray_workers(tmp_path, report_args=report_args):
        pass

    if report_args:
        assert len(list(tmp_path.glob("*.html"))) == 2
    else:
        assert len(list(tmp_path.glob("*.html"))) == 0
        assert len(list(tmp_path.glob("*.memray"))) == 2


def test_basic_integration_scheduler(client, tmp_path):
    with memray_scheduler(tmp_path):
        pass

    assert len(list(tmp_path.glob("*.html"))) == 1


@pytest.mark.parametrize("report_args", [("flamegraph", "--leaks"), False])
def test_basic_integration_scheduler_report_args(client, tmp_path, report_args):
    with memray_scheduler(tmp_path, report_args=report_args):
        pass

    if report_args:
        assert len(list(tmp_path.glob("*.html"))) == 1
    else:
        assert len(list(tmp_path.glob("*.html"))) == 0
        assert len(list(tmp_path.glob("*.memray"))) == 1

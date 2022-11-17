from __future__ import annotations

import sys
from unittest import mock

import pytest
from packaging.version import Version
from tornado.httpclient import AsyncHTTPClient

from distributed import Scheduler
from distributed.utils_test import gen_test


@gen_test()
async def test_missing_bokeh():
    with mock.patch.dict(sys.modules, {"bokeh": None}):
        async with Scheduler(dashboard_address=":0") as s:
            http_client = AsyncHTTPClient()
            response = await http_client.fetch(
                f"http://localhost:{s.http_server.port}/status"
            )
            assert response.code == 200
            body = response.body.decode()
            assert "Dask needs bokeh" in body


@gen_test()
async def test_bokeh_version_too_low():
    pytest.importorskip("bokeh")

    import distributed.dashboard.utils

    with mock.patch.object(
        distributed.dashboard.utils, "BOKEH_VERSION", Version("1.4.0")
    ):
        with pytest.warns(UserWarning, match="1.4.0"):
            async with Scheduler(dashboard_address=":0") as s:
                http_client = AsyncHTTPClient()
                response = await http_client.fetch(
                    f"http://localhost:{s.http_server.port}/status"
                )
                assert response.code == 200
                body = response.body.decode()
                assert "Dask needs bokeh" in body


@gen_test()
async def test_bokeh_version_too_high():
    pytest.importorskip("bokeh")

    import distributed.dashboard.utils

    with mock.patch.object(
        distributed.dashboard.utils, "BOKEH_VERSION", Version("3.0.1")
    ):
        with pytest.warns(UserWarning, match="3.0.1"):
            async with Scheduler(dashboard_address=":0") as s:
                http_client = AsyncHTTPClient()
                response = await http_client.fetch(
                    f"http://localhost:{s.http_server.port}/status"
                )
                assert response.code == 200
                body = response.body.decode()
                assert "Dask needs bokeh" in body

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dask.dataframe import DataFrame


def rearrange_by_column_service(
    df: DataFrame,
    column: str,
    npartitions: int | None = None,
):
    ...

from distributed.protocol import serialize, deserialize
import numpy as np
import pandas as pd
import pytest

cuda = pytest.importorskip("numba.cuda")
cudf = pytest.importorskip("cudf")


def test_serialize_cudf():
    df = pd.DataFrame({
        'a': [0, 1, 2, 3],
        'b': [0.1, 0.2, None, 0.3]}
    )
    gdf = cudf.from_pandas(df)
    header, frames = serialize(gdf, serializers=("cuda", "dask", "pickle"))
    assert any([type(f) == cuda.cudadrv.devicearray.DeviceNDArray for f in frames])
    y = deserialize(header, frames, deserializers=("cuda", "dask", "pickle", "error"))
    assert type(y) == cudf.DataFrame

    pd.testing.assert_frame_equal(y.to_pandas(), df)


def test_serialize_cudf_host():
    df = pd.DataFrame({
        'a': [0, 1, 2, 3],
        'b': [0.1, 0.2, None, 0.3]}
    )
    gdf = cudf.from_pandas(df)
    header, frames = serialize(gdf, serializers=("cuda_host", "dask", "pickle"))
    assert any([type(f) == np.ndarray for f in frames])
    y = deserialize(header, frames, deserializers=("cuda_host", "dask", "pickle", "error"))
    assert type(y) == cudf.DataFrame

    pd.testing.assert_frame_equal(y.to_pandas(), df)

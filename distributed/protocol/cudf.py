import pickle
import cudf
import cudf.groupby.groupby
from .cuda import cuda_serialize, cuda_deserialize

# all (de-)serializtion code lives in the cudf codebase
# here we ammend the returned headers with `is_gpu` for
# UCX buffer consumption
@cuda_serialize.register((cudf.DataFrame, cudf.Series, cudf.groupby.groupby._Groupby))
def serialize_cudf_dataframe(x):
    header, frames = x.serialize()
    header["is_cuda"] = 1
    return header, frames


@cuda_deserialize.register((cudf.DataFrame, cudf.Series, cudf.groupby.groupby._Groupby))
def serialize_cudf_dataframe(header, frames):
    cudf_typ = pickle.loads(header["type"])
    cudf_obj = cudf_typ.deserialize(header, frames)
    return cudf_obj

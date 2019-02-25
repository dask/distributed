import cudf
from .serialize import dask_serialize, dask_deserialize
from .numba import serialize_numba_ndarray, deserialize_numba_ndarray


@dask_serialize.register(cudf.DataFrame)
def serialize_cudf_dataframe(x):
    # TODO: does cudf support duplicate columns?
    print('hey!')
    sub_headers = []
    arrays  = []

    for label, col in x.iteritems():
        header, (frame,) = serialize_numba_ndarray(col.to_gpu_array())
        sub_headers.append(header)
        arrays.append(frame)

    header = {
        'lengths': [len(x)] * x.shape[1],
        'is_cuda': True,
        'subheaders': sub_headers,
        'columns': x.columns,  # TODO
    }

    return header, arrays


@dask_deserialize.register(cudf.DataFrame)
def serialize_cudf_dataframe(header, frames):
    assert len(frames) == len(header['columns'])
    arrays = []

    for subheader, frame in zip(header['subheaders'], frames):
        array = deserialize_numba_ndarray(subheader, [frame])
        arrays.append(array)

    objs = list(zip(header['columns'], arrays))
    return cudf.DataFrame(objs)

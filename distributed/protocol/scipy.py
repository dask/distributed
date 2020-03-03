"""
Efficient serialization of SciPy sparse matrices.
"""
import scipy

from .serialize import dask_deserialize, dask_serialize, register_generic

register_generic(scipy.sparse.spmatrix, "dask", dask_serialize, dask_deserialize)

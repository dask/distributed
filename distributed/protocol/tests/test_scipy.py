from __future__ import annotations

import pytest

from dask.array.scipy_compat import SCIPY_GE_111, SCIPY_GE_200
from dask.utils_test import assert_scipy_sparse_matrix_deprecated

from distributed.protocol import deserialize, serialize

numpy = pytest.importorskip("numpy")
pytest.importorskip("scipy")
scipy_sparse = pytest.importorskip("scipy.sparse")


DTYPES = [numpy.dtype("<f4"), numpy.dtype("<f8")]
MATRIX_TYPES = [
    scipy_sparse.bsr_matrix,
    scipy_sparse.coo_matrix,
    scipy_sparse.csc_matrix,
    scipy_sparse.csr_matrix,
    scipy_sparse.dia_matrix,
    scipy_sparse.dok_matrix,
    scipy_sparse.lil_matrix,
]
SPARRAY_FORMATS = ["bsr", "coo", "csc", "csr", "dia", "dok", "lil"]


def sample_sparse_data(dtype):
    a = numpy.array([[0, 1, 0], [2, 0, 3], [0, 4, 0]], dtype=dtype)
    return a, a.nonzero()


@pytest.mark.parametrize("sparse_type", MATRIX_TYPES)
@pytest.mark.parametrize("dtype", DTYPES)
def test_serialize_scipy_sparse(sparse_type, dtype):
    # SciPy 2 deprecates the sparse matrix classes, including their internal
    # conversion to COO during serialization. Only expect that deprecation.
    with assert_scipy_sparse_matrix_deprecated(SCIPY_GE_200):
        a, anz = sample_sparse_data(dtype)
        acoo = scipy_sparse.coo_matrix((a[anz], anz))
        asp = sparse_type(acoo)

        header, frames = serialize(asp, serializers=["dask"])
        asp2 = deserialize(header, frames)

        assert header["serializer"] == "dask"
        assert type(asp2) is sparse_type
        assert (a == asp2.todense()).all()


@pytest.mark.skipif(not SCIPY_GE_111, reason="scipy.sparse.sparray required")
@pytest.mark.parametrize("format", SPARRAY_FORMATS)
@pytest.mark.parametrize("dtype", DTYPES)
def test_serialize_scipy_sparray(format, dtype):
    sparse_type = getattr(scipy_sparse, f"{format}_array")

    a, anz = sample_sparse_data(dtype)
    acoo = scipy_sparse.coo_array((a[anz], anz))
    asp = sparse_type(acoo)

    header, frames = serialize(asp, serializers=["dask"])
    asp2 = deserialize(header, frames)

    assert header["serializer"] == "dask"
    assert type(asp2) is sparse_type
    assert (a == asp2.toarray()).all()

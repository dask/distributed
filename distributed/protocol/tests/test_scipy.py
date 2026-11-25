from __future__ import annotations

import warnings

import pytest

from dask.utils_test import assert_scipy_sparse_matrix_deprecated

from distributed.protocol import deserialize, serialize

numpy = pytest.importorskip("numpy")
pytest.importorskip("scipy")
scipy_sparse = pytest.importorskip("scipy.sparse")

# Probe for the sparse array API instead of importing dask.array.scipy_compat:
# that would import dask.array (and therefore require numpy) at collection time,
# breaking the "mindeps" environment where neither numpy nor scipy is installed.
HAS_SPARRAY = hasattr(scipy_sparse, "sparray")

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
    # conversion to COO during serialization. Only expect that deprecation; the
    # context manager is a no-op on SciPy 1.x. Warnings must not be swallowed.
    with assert_scipy_sparse_matrix_deprecated():
        a, anz = sample_sparse_data(dtype)
        acoo = scipy_sparse.coo_matrix((a[anz], anz))
        asp = sparse_type(acoo)
        assert isinstance(asp, scipy_sparse.spmatrix)

        header, frames = serialize(asp, serializers=["dask"])
        asp2 = deserialize(header, frames)

        assert header["serializer"] == "dask"
        assert type(asp2) is sparse_type
        assert isinstance(asp2, scipy_sparse.spmatrix)
        assert asp2.dtype == dtype
        assert (a == asp2.toarray()).all()


@pytest.mark.skipif(not HAS_SPARRAY, reason="scipy.sparse.sparray required")
@pytest.mark.parametrize("format", SPARRAY_FORMATS)
@pytest.mark.parametrize("dtype", DTYPES)
def test_serialize_scipy_sparray(format, dtype):
    sparse_type = getattr(scipy_sparse, f"{format}_array")

    a, anz = sample_sparse_data(dtype)
    acoo = scipy_sparse.coo_array((a[anz], anz))
    asp = sparse_type(acoo)
    assert isinstance(asp, scipy_sparse.sparray)

    header, frames = serialize(asp, serializers=["dask"])
    asp2 = deserialize(header, frames)

    assert header["serializer"] == "dask"
    assert type(asp2) is sparse_type
    assert isinstance(asp2, scipy_sparse.sparray)
    assert asp2.dtype == dtype
    assert (a == asp2.toarray()).all()


@pytest.mark.skipif(not HAS_SPARRAY, reason="scipy.sparse.sparray required")
@pytest.mark.parametrize("format", SPARRAY_FORMATS)
def test_serialize_scipy_sparray_does_not_warn(format):
    """Dask must not construct a deprecated sparse matrix on its own.

    Serializing a sparse array must not trigger the SciPy 2 sparse matrix
    deprecation warning; only an explicit user choice of a matrix container may.
    """
    sparse_type = getattr(scipy_sparse, f"{format}_array")
    a, anz = sample_sparse_data(numpy.dtype("<f8"))
    asp = sparse_type(scipy_sparse.coo_array((a[anz], anz)))

    with warnings.catch_warnings(record=True) as record:
        warnings.simplefilter("always")
        header, frames = serialize(asp, serializers=["dask"])
        deserialize(header, frames)

    assert not [w for w in record if "is being replaced by" in str(w.message)]

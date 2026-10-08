from __future__ import annotations

import pytest
from packaging.version import Version

from distributed.protocol import deserialize, serialize

numpy = pytest.importorskip("numpy")
scipy = pytest.importorskip("scipy")
scipy_sparse = pytest.importorskip("scipy.sparse")


SCIPY_VERSION = Version(scipy.__version__)
SCIPY_GE_1_15_0 = SCIPY_VERSION.release >= (1, 15, 0)

SPARSE_MATRIX_TYPES = [
    scipy_sparse.bsr_matrix,
    scipy_sparse.coo_matrix,
    scipy_sparse.csc_matrix,
    scipy_sparse.csr_matrix,
    scipy_sparse.dia_matrix,
    scipy_sparse.dok_matrix,
    scipy_sparse.lil_matrix,
]

# The spmatrix classes are deprecated in favour of sparse arrays (scipy >= 1.8)
SPARSE_ARRAY_TYPES = (
    [
        scipy_sparse.bsr_array,
        scipy_sparse.coo_array,
        scipy_sparse.csc_array,
        scipy_sparse.csr_array,
        scipy_sparse.dia_array,
        scipy_sparse.dok_array,
        scipy_sparse.lil_array,
    ]
    if hasattr(scipy_sparse, "sparray")
    else []
)


@pytest.mark.parametrize(
    "sparse_type",
    SPARSE_MATRIX_TYPES + SPARSE_ARRAY_TYPES,
)
@pytest.mark.parametrize(
    "dtype",
    [
        numpy.dtype("<f4"),
        numpy.dtype("<f8"),
    ],
)
# Keep covering the legacy spmatrix classes until SciPy removes them; only the
# "is being replaced by" deprecation is tolerated, everything else stays an error.
@pytest.mark.filterwarnings("ignore:.*is being replaced by.*:DeprecationWarning")
def test_serialize_scipy_sparse(sparse_type, dtype):
    a = numpy.array([[0, 1, 0], [2, 0, 3], [0, 4, 0]], dtype=dtype)

    anz = a.nonzero()
    if sparse_type in SPARSE_MATRIX_TYPES:
        acoo = scipy_sparse.coo_matrix((a[anz], anz))
    else:
        acoo = scipy_sparse.coo_array((a[anz], anz))
    asp = sparse_type(acoo)

    header, frames = serialize(asp, serializers=["dask"])
    asp2 = deserialize(header, frames)

    a2 = asp2.toarray()

    assert (a == a2).all()

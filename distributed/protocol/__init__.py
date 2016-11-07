from __future__ import print_function, division, absolute_import

from .compression import compressions, default_compression
from .core import dumps, loads, maybe_compress, decompress, msgpack
from .serialize import (serialize, deserialize, Serialize, Serialized,
    to_serialize, register_serialization)

from ..utils  import ignoring

with ignoring(ImportError):
    from . import numpy

with ignoring(ImportError):
    from . import h5py

with ignoring(ImportError):
    from . import netcdf4

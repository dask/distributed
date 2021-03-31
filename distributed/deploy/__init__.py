from contextlib import suppress

from .adaptive import Adaptive
from .cluster import Cluster
from .local import LocalCluster
from .spec import ProcessInterface, SpecCluster
from .ssh import SSHCluster

with suppress(ImportError):
    from .ssh import SSHCluster

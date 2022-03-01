from contextlib import suppress

from .adaptive import Adaptive, TaskAdaptive
from .cluster import Cluster
from .local import LocalCluster
from .spec import ProcessInterface, SpecCluster
from .ssh import SSHCluster

with suppress(ImportError):
    from .ssh import SSHCluster

from contextlib import suppress

from distributed.deploy.adaptive import Adaptive
from distributed.deploy.cluster import Cluster
from distributed.deploy.local import LocalCluster
from distributed.deploy.spec import ProcessInterface, SpecCluster
from distributed.deploy.ssh import SSHCluster

with suppress(ImportError):
    from distributed.deploy.ssh import SSHCluster

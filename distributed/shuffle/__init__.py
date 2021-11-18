try:
    import pandas
except ImportError:
    SHUFFLE_AVAILABLE = False
else:
    del pandas
    SHUFFLE_AVAILABLE = True

    from .common import ShuffleId
    from .graph import rearrange_by_column_p2p
    from .shuffle_scheduler import ShuffleSchedulerPlugin
    from .shuffle_worker import ShuffleWorkerExtension

__all__ = [
    "SHUFFLE_AVAILABLE",
    "ShuffleId",
    "rearrange_by_column_p2p",
    "ShuffleWorkerExtension",
    "ShuffleSchedulerPlugin",
]

try:
    import pandas
except ImportError:
    SHUFFLE_AVAILABLE = False
else:
    del pandas
    SHUFFLE_AVAILABLE = True

    from .shuffle import rearrange_by_column_service
    from .shuffle_extension import ShuffleId, ShuffleMetadata, ShuffleWorkerExtension

__all__ = [
    "SHUFFLE_AVAILABLE",
    "rearrange_by_column_service",
    "ShuffleId",
    "ShuffleMetadata",
    "ShuffleWorkerExtension",
]

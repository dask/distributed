"""
Diagnostics for GPU memory managed by RMM (RAPIDS memory manager).
"""


def _get_pool_size(mr):
    # if the memory resource or any of its upstreams
    # is a `PoolMemoryResource`, get its pool size
    import rmm

    if not isinstance(mr, rmm.mr.PoolMemoryResource):
        if hasattr(mr, "upstream_mr"):
            return _get_pool_size_for_mr(mr.upstream_mr)
        else:
            return 0
    else:
        pool_size = mr.pool_size()
        return pool_size


def _get_allocated_bytes(mr):
    import rmm

    if not hasattr(mr, "get_allocated_bytes"):
        if hasattr(mr, "upstream_mr"):
            return _get_allocated_bytes_for_mr(mr.upstream_mr)
        else:
            return 0
    else:
        return mr.get_allocated_bytes()


def real_time():
    import rmm

    mr = rmm.mr.get_current_device_resource()
    rmm_pool_size = _get_pool_size_for_mr(mr)
    rmm_used = _get_allocated_bytes_for_mr(mr)
    rmm_total = max(rmm_pool_size, rmm_used)
    return {"rmm-used": rmm_used, "rmm-total": rmm_total}

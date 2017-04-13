import atexit
import sys

from importlib import import_module


def preload(module_names, parameter=None):
    """ Import modules, handles `dask_setup` and `dask_teardown` functions

    Parameters
    ----------
    module_names: list of strings
        Module names or file paths
    parameter: object
        Parameter passed to `dask_setup` and `dask_teardown`
    """
    for module_name in module_names:
        if module_name not in sys.modules:
            import_module(module_name)
        module = sys.modules[module_name]
        dask_setup = getattr(module, 'dask_setup', None)
        dask_teardown = getattr(module, 'dask_teardown', None)
        if dask_setup is not None:
            dask_setup(parameter)
        if dask_teardown is not None:
            atexit.register(dask_teardown, parameter)

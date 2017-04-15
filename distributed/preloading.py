import atexit
import shutil
import logging
import os
import sys
from importlib import import_module

from .compatibility import reload, invalidate_caches, cache_from_source


logger = logging.getLogger(__name__)


def load_file(path):
    """ Loads modules for a file (.py, .pyc, .zip, .egg) """
    directory, filename = os.path.split(path)
    name, ext = os.path.splitext(filename)
    names_to_import = []
    if ext in ('.py', '.pyc'):
        if directory not in sys.path:
            sys.path.insert(0, directory)
        names_to_import.append(name)
        # Ensures that no pyc file will be reused
        cache_file = cache_from_source(path)
        if os.path.exists(cache_file):
            os.remove(cache_file)
    if ext in ('.egg', '.zip'):
        if path not in sys.path:
            sys.path.insert(0, path)
        if ext == '.egg':
            import pkg_resources
            pkgs = pkg_resources.find_distributions(path)
            for pkg in pkgs:
                names_to_import.append(pkg.project_name)
        elif ext == '.zip':
            names_to_import.append(name)

    loaded = []
    if not names_to_import:
        logger.warning("Found nothing to import from %s", filename)
    else:
        invalidate_caches()
        for name in names_to_import:
            logger.info("Reload module %s from %s file", name, ext)
            loaded.append(reload(import_module(name)))
    return loaded


def preload(names, parameter=None, file_dir=None):
    """ Imports modules, handles `dask_setup` and `dask_teardown` functions

    Parameters
    ----------
    names: list of strings
        Module names or file paths
    parameter: object
        Parameter passed to `dask_setup` and `dask_teardown`
    file_dir: string
        Path of a directory where files should be copied
    """
    for name in names:
        # import
        if name.endswith(".py"):
            # name is a file path
            if file_dir is not None:
                basename = os.path.basename(name)
                if os.path.exists(os.path.join(file_dir, basename)):
                    logging.error("File name collision", basename)
                module = load_file(shutil.copy(name, file_dir))[0]
            else:
                module = load_file(name)[0]

        else:
            # name is a module name
            if name not in sys.modules:
                import_module(name)
            module = sys.modules[name]

        # handle special functions
        dask_setup = getattr(module, 'dask_setup', None)
        dask_teardown = getattr(module, 'dask_teardown', None)
        if dask_setup is not None:
            dask_setup(parameter)
        if dask_teardown is not None:
            atexit.register(dask_teardown, parameter)

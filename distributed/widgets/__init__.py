import os.path

from dask.widgets import FILTERS, TEMPLATE_PATHS

from distributed.utils import key_split

TEMPLATE_PATHS.append(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "templates")
)
FILTERS["key_split"] = key_split

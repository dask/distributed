from __future__ import annotations

from itertools import chain
from numbers import Number

import bokeh
from bokeh.io import curdoc
from bokeh.models import Title
from packaging.version import Version
from tlz.curried import first

from distributed.dashboard.components import without_property_validation

try:
    import numpy as np
except ImportError:
    np = None  # type: ignore

BOKEH_VERSION = Version(bokeh.__version__)

PROFILING = False

_DATATABLE_STYLESHEETS_KWARGS = {
    "stylesheets": [
        """
.bk-data-table {
z-index: 0;
}
"""
    ]
}


def transpose(lod):
    return {k: [d[k] for d in lod] for k in lod[0]}


def get_title(fig):
    """Read a plot title, whether or not Bokeh created a ``Title`` model."""
    title = fig.title
    return title.text if isinstance(title, Title) else title


def set_title(fig, text):
    """Set a plot title, also when Bokeh property validation is disabled.

    With validation disabled, ``figure(title=text)`` stores the raw string
    instead of converting it to a ``Title`` model, so ``fig.title.text`` is
    not available.
    """
    if isinstance(fig.title, Title):
        fig.title.text = text
    else:
        fig.title = Title(text=text)


@without_property_validation
def update(source, data):
    """Update source with data

    This checks a few things first

    1.  If the data is the same, then don't update
    2.  If numpy is available and the data is numeric, then convert to numpy
        arrays
    3.  If profiling then perform the update in another callback
    """
    if (
        not np
        or not any(
            isinstance(v, np.ndarray)
            for v in chain(source.data.values(), data.values())
        )
    ) and source.data == data:
        return
    if np and len(data[first(data)]) > 10:
        d = {}
        for k, v in data.items():
            if type(v) is not np.ndarray and isinstance(v[0], Number):
                d[k] = np.array(v)
                if d[k].dtype == np.int32:  # avoid int32 (Windows default)
                    d[k] = d[k].astype("int64")
            else:
                d[k] = v
    else:
        d = data

    if PROFILING:
        curdoc().add_next_tick_callback(lambda: source.data.update(d))
    else:
        source.data.update(d)

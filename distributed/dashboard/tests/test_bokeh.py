from __future__ import annotations

import threading
from time import sleep

from bokeh.core.property.bases import Property
from bokeh.models import Title
from bokeh.plotting import figure

from distributed.dashboard.components import without_property_validation
from distributed.dashboard.utils import get_title, set_title


def test_old_import():
    try:
        from distributed.bokeh import BokehScheduler  # noqa: F401
    except ImportError as e:
        assert "distributed.dashboard" in str(e)


def test_without_property_validation_is_thread_safe():
    # Bokeh uses a process-global flag to disable validation. Without a lock,
    # two threads updating the dashboard can restore it in the wrong order and
    # leave validation disabled for the whole process.
    entered = threading.Event()

    @without_property_validation
    def first():
        entered.set()
        sleep(0.2)

    @without_property_validation
    def second():
        entered.wait(5)

    try:
        t1 = threading.Thread(target=first)
        t2 = threading.Thread(target=second)
        t1.start()
        t2.start()
        t1.join(10)
        t2.join(10)
        assert not t1.is_alive() and not t2.is_alive()
        assert Property._should_validate is True
    finally:
        Property._should_validate = True


def test_set_title_without_property_validation():
    # With validation disabled, Bokeh stores the string title verbatim rather
    # than converting it to a Title model.
    try:
        Property._should_validate = False
        fig = figure(title="old")
        assert isinstance(fig.title, str)
        set_title(fig, "new")
        assert isinstance(fig.title, Title)
        assert get_title(fig) == "new"
        set_title(fig, "newer")
        assert get_title(fig) == "newer"
    finally:
        Property._should_validate = True

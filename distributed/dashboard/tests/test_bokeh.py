from __future__ import annotations

from types import SimpleNamespace

import pytest


def test_old_import():
    try:
        from distributed.bokeh import BokehScheduler  # noqa: F401
    except ImportError as e:
        assert "distributed.dashboard" in str(e)


def test_without_property_validation_is_thread_safe():
    pytest.importorskip("bokeh")

    import threading
    from time import sleep

    from bokeh.models import Title
    from bokeh.plotting import figure

    from distributed.dashboard.components import without_property_validation

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

    t1 = threading.Thread(target=first)
    t2 = threading.Thread(target=second)
    t1.start()
    t2.start()
    t1.join(10)
    t2.join(10)

    assert not t1.is_alive() and not t2.is_alive()
    # If validation were left off, Bokeh would store a plain string here.
    assert isinstance(figure(title="title").title, Title)


def test_set_title_with_raw_string():
    pytest.importorskip("bokeh")

    from bokeh.models import Title

    from distributed.dashboard.utils import get_title, set_title

    fig = SimpleNamespace(title="old")
    assert get_title(fig) == "old"

    set_title(fig, "new")
    assert isinstance(fig.title, Title)
    assert get_title(fig) == "new"

    set_title(fig, "newer")
    assert get_title(fig) == "newer"

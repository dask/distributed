from bokeh.models import Plot, ColumnDataSource

from distributed.bokeh.plots.status import TaskStream

def test_TaskStream_initialization():
    task_stream = TaskStream()
    assert isinstance(task_stream.root, Plot)
    assert isinstance(task_stream.source, ColumnDataSource)

def test_TaskStream_update():
    task_stream = TaskStream()

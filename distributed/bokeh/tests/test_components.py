from __future__ import print_function, division, absolute_import

from bokeh.layouts import Row
from bokeh.models import Plot, ColumnDataSource

from distributed.bokeh.components import (
    TaskStream, TaskProgress, MemoryUsage, ResourceProfiles
)

def test_TaskStream_initialization():
    task_stream = TaskStream()
    assert isinstance(task_stream.root, Plot)
    assert isinstance(task_stream.source, ColumnDataSource)

def test_TaskStream_update():
    task_stream = TaskStream()

def test_TaskProgress_initialization():
    task_progress = TaskProgress()
    assert isinstance(task_progress.root, Plot)
    assert isinstance(task_progress.source, ColumnDataSource)

def test_TaskProgress_update():
    task_stream = TaskProgress()

def test_MemoryUsage_initialization():
    memory_usage = MemoryUsage()
    assert isinstance(memory_usage.root, Plot)
    assert isinstance(memory_usage.source, ColumnDataSource)

def test_TaskProgress_update():
    memory_usage = MemoryUsage()

def test_ResourceProfiles_initialization():
    resource_profiles = ResourceProfiles()
    assert isinstance(resource_profiles.root, Row)
    assert isinstance(resource_profiles.source, ColumnDataSource)

def test_ResourceProfiles_update():
    resource_profiles = ResourceProfiles()

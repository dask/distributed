from __future__ import print_function, division, absolute_import

import functools
import gc
import os
import shutil
import subprocess
import sys

from distributed.diskutils import WorkSpace
from distributed.utils_test import captured_logger


def assert_directory_contents(dir_path, expected):
    expected = [os.path.join(dir_path, p) for p in expected]
    actual = [os.path.join(dir_path, p)
              for p in os.listdir(dir_path)
              if p != 'global.lock']
    assert sorted(actual) == sorted(expected)


def test_workdir_simple(tmpdir):
    # Test nominal operation of WorkSpace and WorkDirs
    base_dir = str(tmpdir)
    assert_contents = functools.partial(assert_directory_contents, base_dir)

    ws = WorkSpace(base_dir)
    assert_contents([])
    a = ws.new_work_dir(name='aa')
    assert_contents(['aa', 'aa.dirlock'])
    b = ws.new_work_dir(name='bb')
    assert_contents(['aa', 'aa.dirlock', 'bb', 'bb.dirlock'])
    ws._purge_leftovers()
    assert_contents(['aa', 'aa.dirlock', 'bb', 'bb.dirlock'])

    a.release()
    assert_contents(['bb', 'bb.dirlock'])
    del b
    gc.collect()
    assert_contents([])

    # Generated temporary name with a prefix
    a = ws.new_work_dir(prefix='foo-')
    b = ws.new_work_dir(prefix='bar-')
    c = ws.new_work_dir(prefix='bar-')
    assert_contents({a.dir_path, a._lock_path,
                     b.dir_path, b._lock_path,
                     c.dir_path, c._lock_path})
    assert os.path.basename(a.dir_path).startswith('foo-')
    assert os.path.basename(b.dir_path).startswith('bar-')
    assert os.path.basename(c.dir_path).startswith('bar-')
    assert b.dir_path != c.dir_path


def test_two_workspaces_in_same_directory(tmpdir):
    # If handling the same directory with two WorkSpace instances,
    # things should work ok too
    base_dir = str(tmpdir)
    assert_contents = functools.partial(assert_directory_contents, base_dir)

    ws = WorkSpace(base_dir)
    assert_contents([])
    a = ws.new_work_dir(name='aa')
    assert_contents(['aa', 'aa.dirlock'])

    ws2 = WorkSpace(base_dir)
    ws2._purge_leftovers()
    assert_contents(['aa', 'aa.dirlock'])
    b = ws.new_work_dir(name='bb')
    assert_contents(['aa', 'aa.dirlock', 'bb', 'bb.dirlock'])

    del ws
    gc.collect()


def test_process_crash(tmpdir):
    # WorkSpace should be able to clean up stale contents left by
    # crashed process
    base_dir = str(tmpdir)
    assert_contents = functools.partial(assert_directory_contents, base_dir)

    ws = WorkSpace(base_dir)

    code = """if 1:
        import signal
        import sys
        import time

        from distributed.diskutils import WorkSpace

        ws = WorkSpace(%(base_dir)r)
        a = ws.new_work_dir(name='aa')
        b = ws.new_work_dir(prefix='foo-')
        print((a.dir_path, b.dir_path))
        sys.stdout.flush()

        time.sleep(100)
        """ % dict(base_dir=base_dir)

    p = subprocess.Popen([sys.executable, '-c', code],
                         stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                         universal_newlines=True)
    line = p.stdout.readline()
    assert p.poll() is None
    a_path, b_path = eval(line)
    assert_contents([a_path, a_path + '.dirlock', b_path, b_path + '.dirlock'])

    # The child process holds a lock so the work dirs shouldn't be removed
    ws._purge_leftovers()
    assert_contents([a_path, a_path + '.dirlock', b_path, b_path + '.dirlock'])

    # Kill the process so it's unable to clear the work dirs itself
    p.kill()
    assert p.wait()  # process returned with non-zero code
    assert_contents([a_path, a_path + '.dirlock', b_path, b_path + '.dirlock'])

    with captured_logger('distributed.diskutils', 'WARNING', propagate=False) as sio:
        ws._purge_leftovers()
    assert_contents([])
    # One log line per purged directory
    lines = sio.getvalue().splitlines()
    assert len(lines) == 2
    for p in (a_path, b_path):
        assert any(repr(p) in line for line in lines)


def test_rmtree_failure(tmpdir):
    base_dir = str(tmpdir)

    ws = WorkSpace(base_dir)
    a = ws.new_work_dir(name='aa')
    shutil.rmtree(a.dir_path)
    with captured_logger('distributed.diskutils', 'ERROR', propagate=False) as sio:
        a.release()
    lines = sio.getvalue().splitlines()
    assert len(lines) == 1
    assert lines[0].startswith("Failed to remove %r" % (a.dir_path,))

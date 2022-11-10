from __future__ import annotations

import subprocess
import sys

script = """
import threading
import traceback

import_succeeded = False

def import_dask_distributed():
    try:
        import dask.distributed  # noqa
    except Exception:
        # Print the exception for debugging
        traceback.print_exc()
    else:
        global import_succeeded
        import_succeeded = True

thread = threading.Thread(target=import_dask_distributed, daemon=True)
thread.start()
thread.join()

assert import_succeeded

# Now try to use distributed from the main thread, and check that it works
from dask.distributed import Client
with Client(processes=False) as client:
    res = client.submit(lambda x: x + 1, 1).result()
    assert res == 2
"""


def test_can_import_distributed_in_background_thread():
    """See https://github.com/dask/distributed/issues/5587"""
    subprocess.check_call([sys.executable, "-c", script])

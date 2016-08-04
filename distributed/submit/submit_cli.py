import os
import sys
from tornado import gen
from distributed import rpc


@gen.coroutine
def _submit(remote_client_address, filepath):
    rc = rpc(addr=remote_client_address)
    remote_file = os.path.basename(filepath)
    with open(filepath, 'rb') as f:
        bytes_read = f.read()
    yield rc.upload_file(filename=remote_file, file_payload=bytes_read)
    result = yield rc.execute(filename=remote_file)
    if result['stdout']:
        sys.stdout.write(str(result['stdout']))
    if result['stderr']:
        sys.stderr.write(str(result['stderr']))

import os
import pathlib
import pickle
import shutil
import threading

import zict

from dask.sizeof import sizeof
from dask.utils import parse_bytes

from ..system import MEMORY_LIMIT
from ..utils import offload


class MultiFile:
    def __init__(
        self,
        directory,
        dump=pickle.dump,
        load=pickle.load,
        join=None,
        n_files=256,
        memory_limit=MEMORY_LIMIT / 2,
        file_cache=None,
        sizeof=sizeof,
    ):
        assert join
        self.directory = pathlib.Path(directory)
        if not os.path.exists(self.directory):
            os.mkdir(self.directory)
        self.dump = dump
        self.load = load
        self.join = join
        self.lock = threading.Lock()

        self.file_buffer_size = int(parse_bytes(memory_limit) / n_files)

        if file_cache is None:
            file_cache = zict.LRU(n_files, dict(), on_evict=lambda k, v: v.close())
        self.file_cache = file_cache
        self.bytes_written = 0
        self.bytes_read = 0

    def open_file(self, id: str):
        with self.lock:
            try:
                return self.file_cache[id]
            except KeyError:
                file = open(
                    self.directory / str(id),
                    mode="ab+",
                    buffering=self.file_buffer_size,
                )
                self.file_cache[id] = file
                return file

    def read(self, id):
        parts = []
        file = self.open_file(id)
        file.seek(0)
        # TODO: Note that this is unsafe to multiple threads trying to read the same file
        while True:
            try:
                parts.append(self.load(file))
            except EOFError:
                break
        # TODO: We could consider deleting the file at this point
        if parts:
            for part in parts:
                self.bytes_read += sizeof(part)
            return self.join(parts)
        else:
            raise KeyError(id)

    async def write(self, part, id):
        file = await offload(self.open_file, id)
        # TODO: We should consider offloading this to a separate thread
        self.bytes_written += sizeof(part)
        await offload(self.dump, part, file)

    def close(self):
        shutil.rmtree(self.directory)
        self.file_cache.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc, typ, traceback):
        self.close()

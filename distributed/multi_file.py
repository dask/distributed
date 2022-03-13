import pathlib
import pickle
import shutil

from zict import LRU

from dask.utils import parse_bytes

from .system import MEMORY_LIMIT


class MultiFile:
    def __init__(
        self,
        directory: pathlib.Path,
        dump=pickle.dump,
        load=pickle.load,
        join=None,
        n_files=256,
        memory_limit=MEMORY_LIMIT / 2,
    ):
        assert join
        self.directory = directory
        self.dump = dump
        self.load = load
        self.join = join

        self.file_buffer_size = int(parse_bytes(memory_limit) / n_files)

        self.file_cache = LRU(n_files, dict(), on_evict=lambda k, v: v.close())

    def open_file(self, id: str):
        try:
            return self.file_cache[id]
        except KeyError:
            file = open(
                self.directory / id, mode="ab+", buffering=self.file_buffer_size
            )
            self.file_cache[id] = file
            return file

    def read(self, id):
        parts = []
        file = self.open_file(id)
        file.seek(0)
        while True:
            try:
                parts.append(self.load(file))
            except EOFError:
                break
        return self.join(parts)

    def write(self, part, id):
        file = self.open_file(id)
        self.dump(part, file)

    def close(self):
        shutil.rmtree(self.directory)
        self.file_cache.clear()

    def __enter__(self):
        return self

    def __exit__(self, exc, typ, traceback):
        self.close()

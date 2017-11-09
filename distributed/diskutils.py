from __future__ import print_function, division, absolute_import

import glob
import logging
import os
import shutil
import stat
import tempfile

import locket

from .compatibility import finalize


logger = logging.getLogger(__name__)

DIR_LOCK_EXT = '.dirlock'


class WorkDir(object):
    """
    A temporary work directory inside a WorkSpace.
    """

    def __init__(self, workspace, name=None, prefix=None):
        assert name is None or prefix is None

        if name is None:
            self.dir_path = tempfile.mkdtemp(prefix=prefix, dir=workspace.base_dir)
        else:
            self.dir_path = os.path.join(workspace.base_dir, name)
            os.mkdir(self.dir_path)  # it shouldn't already exist

        try:
            self._lock_path = os.path.join(self.dir_path + DIR_LOCK_EXT)
            assert not os.path.exists(self._lock_path)
            logger.debug("Locking %r...", self._lock_path)
            self._lock_file = locket.lock_file(self._lock_path)
            self._lock_file.acquire()
        except Exception:
            shutil.rmtree(self.dir_path, ignore_errors=True)
            raise

        workspace._known_locks.add(self._lock_path)
        self._finalizer = finalize(self, self._finalize,
                                   workspace, self._lock_path,
                                   self._lock_file, self.dir_path)

    def release(self):
        """
        Dispose of this directory.
        """
        self._finalizer()

    @classmethod
    def _finalize(cls, workspace, lock_path, lock_file, dir_path):
        try:
            workspace._purge_directory(dir_path)
        finally:
            lock_file.release()
            workspace._known_locks.remove(lock_path)
            os.unlink(lock_path)


class WorkSpace(object):
    """
    An on-disk workspace that tracks disposable work directories inside it.
    If a process crash or another event left stale directories behind,
    this will be detected and the directories purged.
    """

    # Keep track of all locks known to this process, to avoid several
    # WorkSpaces to step on each other's toes
    _known_locks = set()

    def __init__(self, base_dir):
        self.base_dir = os.path.abspath(base_dir)
        self._global_lock = locket.lock_file(
            os.path.join(self.base_dir, 'global.lock'))

    def _init_workspace(self):
        if not os.path.exists(self.base_dir):
            os.mkdir(self.base_dir)

    def _purge_leftovers(self):
        # Need to hold the global lock to avoid several threads / processes
        # purging at once
        with self._global_lock:
            for path in self._list_unknown_locks():
                self._check_lock_or_purge(path)

    def _list_unknown_locks(self):
        for p in glob.glob(os.path.join(self.base_dir, '*' + DIR_LOCK_EXT)):
            st = os.stat(p)
            # XXX restrict to files owned by current user?
            if stat.S_ISREG(st.st_mode):
                yield p

    def _purge_directory(self, dir_path):
        shutil.rmtree(dir_path, onerror=self._on_remove_error)

    def _check_lock_or_purge(self, path):
        # Try locking the given path, if it fails it's in use,
        # otherwise the corresponding directory can be deleted
        assert path.endswith(DIR_LOCK_EXT)
        if path in self._known_locks:
            # Avoid touching a lock that we know is already taken
            return
        logger.debug("Checking lock file %r...", path)
        try:
            with locket.lock_file(path, timeout=0):
                # Lock file could be taken, therefore purge corresponding
                # directory
                dir_path = path[:-len(DIR_LOCK_EXT)]
                if os.path.exists(dir_path):
                    logger.warning("Found stale lock file and directory %r, purging",
                                   dir_path)
                    self._purge_directory(dir_path)
            # Also clean up lock file (need to release it first)
            os.unlink(path)
        except locket.LockError:
            # Lock file still in use
            pass

    def _on_remove_error(self, func, path, exc_info):
        t, v, tb = exc_info
        logger.error("Failed to remove %r failed in %r: %s",
                     path, func, str(v))

    def new_work_dir(self, **kwargs):
        """
        Create and return a new WorkDir in this WorkSpace.
        Either the *prefix* or *name* parameter should be given
        (*prefix* is preferred as it avoids potential collisions)

        Parameters
        ----------
        prefix: str (optional)
            The prefix of the temporary subdirectory name for the workdir
        name: str (optional)
            The subdirectory name for the workdir
        """
        self._purge_leftovers()
        return WorkDir(self, **kwargs)

"""
Modified ThreadPoolExecutor to support threads leaving the thread pool

This includes a global `secede` method that a submitted function can call to
have its thread leave the ThreadPoolExecutor's thread pool.  This allows the
thread pool to allocate another thread if necessary and so is useful when a
function realises that it is going to be a long-running job that doesn't want
to take up space.  When the function finishes its thread will terminate
gracefully.

This code copies and modifies two functions from the
`concurrent.futures.thread` module, notably `_worker` and
ThreadPoolExecutor._adjust_thread_count` to allow for checking against a global
`threading.local` state.  These functions are subject to the following license,
which is included as a comment at the end of this file:

    https://docs.python.org/3/license.html

... and are under copyright by the Python Software Foundation

   Copyright 2001-2016 Python Software Foundation; All Rights Reserved
"""
import logging
import queue
import threading
import weakref

from . import _concurrent_futures_thread as thread

logger = logging.getLogger(__name__)

thread_state = threading.local()


def _worker_secedable(executor_reference, work_queue, initializer, initargs):
    if initializer is not None:
        try:
            initializer(*initargs)
        except BaseException:
            logger.critical("Exception in initializer:", exc_info=True)
            executor = executor_reference()
            if executor is not None:
                executor._initializer_failed()
            return
    thread_state.proceed = True
    thread_state.executor = executor_reference()
    try:
        while thread_state.proceed:
            executor = executor_reference()
            if not executor:
                return
            with executor._rejoin_lock:
                if executor._rejoin_list:
                    rejoin_thread, rejoin_event = executor._rejoin_list.pop()
                    executor._threads.add(rejoin_thread)
                    executor._threads.discard(threading.current_thread())
                    rejoin_event.set()
                    executor._idle_semaphore.acquire(timeout=0)
                    break

            try:
                work_item = work_queue.get(timeout=1)
            except queue.Empty:
                continue

            if work_item is not None:
                work_item.run()
                # Delete references to object. See issue16284
                del work_item

                executor = executor_reference()
                # attempt to increment idle count
                if executor is not None:
                    executor._idle_semaphore.release()
                del executor
                continue

            executor = executor_reference()
            # Exit if:
            #   - The interpreter is shutting down OR
            #   - The executor that owns the worker has been collected OR
            #   - The executor that owns the worker has been shutdown.
            if thread._shutdown or executor is None or executor._shutdown:
                # Flag the executor as shutting down as early as possible if it
                # is not gc-ed yet.
                if executor is not None:
                    executor._shutdown = True
                # Notice other workers
                work_queue.put(None)
                return
            del executor
    except BaseException:
        logger.critical("Exception in worker", exc_info=True)
    finally:
        del thread_state.proceed
        del thread_state.executor


class ThreadPoolExecutor(thread.ThreadPoolExecutor):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._rejoin_list = []
        self._rejoin_lock = threading.Lock()
        self._thread_name_prefix = kwargs.get(
            "thread_name_prefix", "DaskThreadPoolExecutor"
        )

    def _adjust_thread_count(self):
        # Identical to original but is using _worker_secedable
        # if idle threads are available, don't spin new threads
        if self._idle_semaphore.acquire(timeout=0):
            return

        # When the executor gets lost, the weakref callback will wake up
        # the worker threads.
        def weakref_cb(_, q=self._work_queue):
            q.put(None)

        num_threads = len(self._threads)
        if num_threads < self._max_workers:
            thread_name = "%s_%d" % (self._thread_name_prefix, self._counter())
            t = threading.Thread(
                name=thread_name,
                target=_worker_secedable,
                args=(
                    weakref.ref(self, weakref_cb),
                    self._work_queue,
                    self._initializer,
                    self._initargs,
                ),
            )

            self._threads.add(t)
            t.start()
            thread._threads_queues[t] = self._work_queue


def secede(adjust=True):
    """Have this thread secede from the ThreadPoolExecutor

    See Also
    --------
    rejoin : rejoin the thread pool
    """
    thread_state.proceed = False
    with threads_lock:
        thread_state.executor._threads.remove(threading.current_thread())
        if adjust:
            thread_state.executor._adjust_thread_count()


def rejoin():
    """Have this thread rejoin the ThreadPoolExecutor

    This will block until a new slot opens up in the executor.  The next thread
    to finish a task will leave the pool to allow this one to join.

    See Also
    --------
    secede : leave the thread pool
    """
    thread = threading.current_thread()
    event = threading.Event()
    e = thread_state.executor
    with e._rejoin_lock:
        e._rejoin_list.append((thread, event))
    thread_state.proceed = True
    e.submit(lambda: None)
    event.wait()


threads_lock = threading.Lock()

"""
PSF LICENSE AGREEMENT FOR PYTHON 3.5.2
======================================

1. This LICENSE AGREEMENT is between the Python Software Foundation ("PSF"), and
   the Individual or Organization ("Licensee") accessing and otherwise using Python
   3.5.2 software in source or binary form and its associated documentation.

2. Subject to the terms and conditions of this License Agreement, PSF hereby
   grants Licensee a nonexclusive, royalty-free, world-wide license to reproduce,
   analyze, test, perform and/or display publicly, prepare derivative works,
   distribute, and otherwise use Python 3.5.2 alone or in any derivative
   version, provided, however, that PSF's License Agreement and PSF's notice of
   copyright, i.e., "Copyright c 2001-2016 Python Software Foundation; All Rights
   Reserved" are retained in Python 3.5.2 alone or in any derivative version
   prepared by Licensee.

3. In the event Licensee prepares a derivative work that is based on or
   incorporates Python 3.5.2 or any part thereof, and wants to make the
   derivative work available to others as provided herein, then Licensee hereby
   agrees to include in any such work a brief summary of the changes made to Python
   3.5.2.

4. PSF is making Python 3.5.2 available to Licensee on an "AS IS" basis.
   PSF MAKES NO REPRESENTATIONS OR WARRANTIES, EXPRESS OR IMPLIED.  BY WAY OF
   EXAMPLE, BUT NOT LIMITATION, PSF MAKES NO AND DISCLAIMS ANY REPRESENTATION OR
   WARRANTY OF MERCHANTABILITY OR FITNESS FOR ANY PARTICULAR PURPOSE OR THAT THE
   USE OF PYTHON 3.5.2 WILL NOT INFRINGE ANY THIRD PARTY RIGHTS.

5. PSF SHALL NOT BE LIABLE TO LICENSEE OR ANY OTHER USERS OF PYTHON 3.5.2
   FOR ANY INCIDENTAL, SPECIAL, OR CONSEQUENTIAL DAMAGES OR LOSS AS A RESULT OF
   MODIFYING, DISTRIBUTING, OR OTHERWISE USING PYTHON 3.5.2, OR ANY DERIVATIVE
   THEREOF, EVEN IF ADVISED OF THE POSSIBILITY THEREOF.

6. This License Agreement will automatically terminate upon a material breach of
   its terms and conditions.

7. Nothing in this License Agreement shall be deemed to create any relationship
   of agency, partnership, or joint venture between PSF and Licensee.  This License
   Agreement does not grant permission to use PSF trademarks or trade name in a
   trademark sense to endorse or promote products or services of Licensee, or any
   third party.

8. By copying, installing or otherwise using Python 3.5.2, Licensee agrees
   to be bound by the terms and conditions of this License Agreement.
"""

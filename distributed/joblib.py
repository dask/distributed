from __future__ import print_function, division, absolute_import

from subprocess import check_output
from time import sleep
from tempfile import NamedTemporaryFile

from joblib._parallel_backends import ParallelBackendBase, AutoBatchingMixin
from joblib.parallel import register_parallel_backend
from tornado import gen

from .executor import Executor, _wait



class DistributedBackend(ParallelBackendBase, AutoBatchingMixin):
    MIN_IDEAL_BATCH_DURATION = 0.2
    MAX_IDEAL_BATCH_DURATION = 1.0

    def __init__(self, scheduler_host='127.0.0.1:8786', loop=None,
                 scheduler=None, queue=None,
                 nodes=1, threads_per_node=1,
                 queue_wait=30, start_delay=10
                 ):
        if scheduler == 'slurm':

            slurm_batch_script = """#!/bin/bash
SCHEDULER=`hostname`
echo SCHEDULER: $SCHEDULER
dask-scheduler &

sleep 2

# run one worker process per node
srun dask-worker --nthreads=1 $SCHEDULER:8786
"""

            batch_script = NamedTemporaryFile(dir='.', prefix='dask_batch', suffix='.sh')
            batch_script.write(slurm_batch_script.format(nodes=nodes, threads_per_node=threads_per_node, queue=queue))
            batch_script.flush()
            # setup scheduler
            self._job_id = check_output(['sbatch',
                                         '--job-name=dask-scheduler',
                                         '--nodes=%i' % nodes,
                                         '--ntasks-per-node=%i' % threads_per_node,
                                         '-p %s' % queue if queue else '',
                                         batch_script.name
                                         ]).split()[-1]
            print(self._job_id)
            for i in range(queue_wait):
                self._scheduler_node = check_output(['squeue', '-h', '-j', self._job_id, '--Format=batchhost']).strip()
                print(self._scheduler_node, ' '.join(['squeue', '-h', '-j', self._job_id, '--Format=batchhost']).strip())
                sleep(1)
                if self._scheduler_node and self._scheduler_node != 'n/a':
                    break
            else:
                raise Exception("Timeout on queue")
            scheduler_host = '%s:8786' % self._scheduler_node
            sleep(start_delay)  # wait for workers to join

        self.executor = Executor(scheduler_host, loop=loop)
        self.futures = set()

    def configure(self, n_jobs=1, parallel=None, **backend_args):
        return self.effective_n_jobs(n_jobs)

    def effective_n_jobs(self, n_jobs=1):
        return sum(self.executor.ncores().values())

    def apply_async(self, func, *args, **kwargs):
        callback = kwargs.pop('callback', None)
        kwargs['pure'] = False
        future = self.executor.submit(func, *args, **kwargs)
        self.futures.add(future)

        @gen.coroutine
        def callback_wrapper():
            result = yield _wait([future])
            self.futures.remove(future)
            callback(result)  # gets called in separate thread

        self.executor.loop.add_callback(callback_wrapper)

        future.get = future.result  # monkey patch to achieve AsyncResult API
        return future

    def terminate(self):
        self.abort_everything(ensure_ready=False)

    def abort_everything(self, ensure_ready=True):
        # Tell the executor to cancel any task submitted via this instance
        # as joblib.Parallel will never access those results.
        self.executor.cancel(self.futures)
        self.futures.clear()
        if not ensure_ready:
            # Kill jobs
            check_output(['scancel', self._job_id])


register_parallel_backend('distributed', DistributedBackend)

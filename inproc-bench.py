from time import time
import operator

from dask.distributed import Client, wait, get_task_stream

client = Client(processes=False, n_workers=2, threads_per_worker=1)
a, b = client.cluster.scheduler.workers

start = time()
with get_task_stream(plot='save', filename='latency.html') as ts:
    x = 0
    for i in range(10):
        x = client.submit(operator.add, x, 1, workers=[a])
        # x = client.submit(operator.add, x, 1, workers=[b])
    wait(x)

from distributed.utils import format_time

stop = time()
print(format_time(stop - start))

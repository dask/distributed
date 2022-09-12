
from distributed import Client, LocalCluster
from time import time
import numpy as np


def run_test():
    runtime = []
    def foo():
        return [1.5] * 1_000_000

    # with LocalCluster(n_workers=2, threads_per_worker=1, memory_limit='8GiB') as cluster:
    for i in range(5):
        with Client() as client:
            s = time()
            res = client.submit(foo).result()
            runtime.append(time() - s)
    print(f"Run time (in seconds) for 5 runs is: {runtime}, and mean runtime:  {np.mean(runtime)} seconds")

if __name__ == "__main__":
    run_test()
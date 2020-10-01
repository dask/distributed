# Run from the command line using an mpirun command like:
#    mpirun -np 4 python demo.py
# Number of Workers used is 2 less than the number of processes used.
# Requires mpi4py and dask-mpi.

import dask.array as da
from dask_mpi import initialize
from datetime import datetime as dt
from distributed import Client
import time


protocol = 'mpi://'
#protocol = 'tcp://'


# Set to True to give you time to open dashboard in a browser.
want_sleep = False


initialize(protocol=protocol)
client = Client()

si = client.scheduler_info()
print(si)
print(f'Number of workers: {len(si["workers"])}')
print(f'Dashboard address: {si["services"]}')


if want_sleep:
    time.sleep(5)

print('Starting calculation')
start = dt.now()
x = da.random.normal(size=(10000, 10000), chunks=(1000, 1000))
y = (x + x.T).mean(axis=1)
print(y.compute())
print(f'Calculation complete, elapsed seconds {(dt.now() - start).total_seconds()}')

if want_sleep:
    time.sleep(30)
